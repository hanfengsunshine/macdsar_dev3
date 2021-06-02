import logging
from decimal import Decimal
from strategy_trading.StrategyTrading.tradingHelper import StrategyTradingHelper
# from strategy_trading.StrategyTrading.tradingGatewayConnection import TradingConnection, TradingSession
# from strategy_trading.StrategyTrading.marketDataConnection import MarketDataConnection
import asyncio
from strategy_trading.StrategyTrading.marketData import OrderBookDepth, OrderBookDiff, MarketTrade
from strategy_trading.StrategyTrading.orderBook import BaseOrderBook
from strategy_trading.StrategyTrading.order import Side
from strategy_trading.StrategyTrading.positionManager import SigSybPM, SigSybPMOS
from strategy_trading.StrategyTrading.inventoryManager import InventoryManager


class Strategy:
    def __init__(self, exchange, global_symbol, strategy_helper_instance: StrategyTradingHelper):
        self.exchange = exchange
        self.global_symbol = global_symbol
        self.logger = logging.getLogger(self.__class__.__name__)
        self.strategy_helper = strategy_helper_instance

        # get symbol tick size, min_order_size here
        symbol_info = self.strategy_helper.symbol_helper.get_info(global_symbol, exchange)
        self.symbol = symbol_info['symbol']
        self.order_book = BaseOrderBook(exchange, self.global_symbol)
        # @@ add SPOT order book
        self.min_order_size = symbol_info['min_order_size']
        self.strategy_config = strategy_helper.config['strategy']
        self.enable_trading = False if ('enable_trading' not in self.strategy_config
                                        or not self.strategy_config['enable_trading']) else True

        # Config parameters
        self.horizon = 1                    # Number of levels to cross
        self.ratio_horizon = 4              # Number of levels to watch for ratio
        self.weights = [Decimal('1.0'), Decimal('1.0'), Decimal('0.8'), Decimal('0.8')]
        self.bluff_offset = 1               # Distance of Bluffing order to best level
        self.stop_loss = 3                  # Stop loss level   6
        self.stop_gain = 1                  # Stop gain level   2

        self.lead_buffer = Decimal(3000)    # max lead amount  3000
        self.lead_min = Decimal(600)        # min lead amount  500
        self.hedge_buffer = Decimal(2000)   # max target level order size 1000

        self.bluff_size = Decimal(600)      # size of bluffing order  1000
        self.delta_ignore = Decimal(60)     # size of level-support residual 155
        self.delta_guard = Decimal(1000)    # size of delta_guard

        self.drop_th = Decimal(200)         # min last trade/cancel size 200
        self.trade_th = Decimal(500)        # min trade volume           300
        self.book_th1 = Decimal('1.2')      # >= 1.0
        self.book_th2 = Decimal('1.2')      # >= 1.0

        self.tick_size = Decimal('0.001')
        self.premium = Decimal('0.00055')  # spot, fut avg premium

        # initialization
        self.lead_order = {}
        self.hedge_order = {}
        self.bluffing_order = {}
        self.lead_px = Decimal('-1.0')
        self.spot_px = Decimal('-1.0')
        self.status = 0  # 0: pending, 1: lead long, -1: lead short, 2: long stop loss, -2: short stop loss

        self.bid_size = Decimal(-1)
        self.bid_px = Decimal('-1.0')
        self.ask_size = Decimal(-1)
        self.ask_px = Decimal('-1.0')
        self.bid_drop = Decimal(0)
        self.ask_drop = Decimal(0)
        self.bid_trade_vol = Decimal(0)
        self.ask_trade_vol = Decimal(0)

    async def init(self, loop_instance):
        # connect to trading gateway
        self.trading_connection = await self.strategy_helper.connect_to_trading_gateway(
            loop=loop_instance,
            gateway_name=self.exchange,
            strategy="liqamhfcta1",
            process="Default",
            receive_error=False
        )

        if self.exchange != 'HUOBI_CONTRACT':
            self.inventory_manager = InventoryManager(self.strategy_config['balance'], self.exchange,
                                                      symbol_helper=self.strategy_helper.symbol_helper)
            self.position_manager = SigSybPM(self.exchange, self.global_symbol, self.strategy_helper.symbol_helper,
                                             alias='Default', inventory_manager=self.inventory_manager)
        else:
            self.inventory_manager = None
            self.position_manager = SigSybPMOS(self.exchange, self.global_symbol, self.strategy_helper.symbol_helper,
                                               alias='Default')

        self.trading_session = self.trading_connection.get_session(self.global_symbol,
                                                                   callback_when_exec=self.position_manager.add_trade)

        # connect to market data
        self.market_data_connection = await self.strategy_helper.connect_to_market_data_gateway(
            loop=loop,
            md_name=self.exchange
        )
        await self.market_data_connection.subscribe(self.global_symbol, want_orderbook=True,
                                                    want_trades=True, book_level=20)

    async def wait_and_process_market_data_updates(self):
        updates = await self.market_data_connection.get_all_updates(global_symbol=self.global_symbol)
        for _update in updates:
            if isinstance(_update, MarketTrade):
                self.order_book.apply_market_trade(_update)
                # self.logger.warning(str(_update.side) + " | " + str(_update.price) + " | " + str(self.bid_px) + " | " + str(self.ask_px))
                if _update.price == self.bid_px and _update.side == Side.SELL:
                    self.bid_trade_vol += _update.size
                elif _update.price == self.ask_px and _update.side == Side.BUY:
                    self.ask_trade_vol += _update.size
                # @@ Do sanity check (if unmatched, do something)
            elif isinstance(_update, OrderBookDiff):
                pass
            elif isinstance(_update, OrderBookDepth):
                self.order_book.update_depth(_update)

    def lead(self):
        to_create_orders = []
        to_cancel_orders = []
        agg_book = [Decimal(0), Decimal(0)]
        agg_book_true = [Decimal(0), Decimal(0)]
        for i in range(0, self.horizon):
            agg_book[0] += self.weights[i] * self.order_book.bids[i][1]
            agg_book[1] += self.weights[i] * self.order_book.asks[i][1]
            agg_book_true[0] += self.order_book.bids[i][1]
            agg_book_true[1] += self.order_book.asks[i][1]
        ratio_book1 = (agg_book[0] + Decimal(1.0)) / (agg_book[1] + Decimal(1.0))
        ratio_book2 = (agg_book_true[0] + Decimal(1.0)) / (agg_book_true[1] + Decimal(1.0))

        # unexpected fill (State Transfer to stop loss immediately)
        if abs(self.position_manager.position) > self.delta_ignore and self.status == 0:
            if self.position_manager.position > 0:
                self.lead_px = self.order_book.asks[0][0]
                self.logger.warning("Unexpected Buy. Start Stop loss.  Lead Price: " + str(self.lead_px))
                self.status = 2
            else:
                self.lead_px = self.order_book.bids[0][0]
                self.logger.warning("Unexpected Sell. Start Stop loss.  Lead Price: " + str(self.lead_px))
                self.status = -2

        # @@ Add spot price reversion and momentum
        # @@ Add kline
        indent = "   |   "
        if ratio_book1 < Decimal(1.0) / self.book_th1 and ratio_book2 < Decimal(1.0) / self.book_th2 \
                and self.lead_min <= agg_book_true[0] <= self.lead_buffer \
                and self.order_book.bids[self.horizon][1] < self.hedge_buffer \
                and self.bid_trade_vol >= self.trade_th and self.bid_drop >= self.drop_th:
            self.lead_px = self.order_book.bids[self.horizon - 1][0]
            self.logger.warning("Lead Short @ " + str(self.lead_px))
            self.logger.warning("Status: " + str(self.status) + indent +
                                "BookRatio1: " + str(ratio_book1) + indent +
                                "BookRatio2: " + str(ratio_book2) + indent +
                                "AggBookSizeTrue: " + str(agg_book_true[0]) + indent +
                                "BidTradeVol: " + str(self.bid_trade_vol) + indent +
                                "BidDrop: " + str(self.bid_drop)
                                )
            to_create_orders.append({
                'side': Side.SELL,
                'price': self.lead_px,
                'size': agg_book_true[0] + self.delta_ignore,
                'order_type': 'GTC'
            })  # lead order
            to_create_orders.append({
                'side': Side.BUY,
                'price': self.lead_px - self.tick_size * self.stop_gain,
                'size': agg_book_true[0] + self.delta_ignore,
                'order_type': 'GTC'
            })  # hedge order
            to_create_orders.append({
                'side': Side.SELL,
                'price': self.lead_px + self.tick_size * self.bluff_offset,
                'size': self.bluff_size,
                'order_type': 'GTC'
            })  # bluffing order
            if self.enable_trading:
                self.status = -1
        # Long
        elif ratio_book1 > self.book_th1 and ratio_book2 > self.book_th2 \
                and self.lead_min <= agg_book_true[1] <= self.lead_buffer \
                and self.order_book.asks[self.horizon][1] < self.hedge_buffer \
                and self.ask_trade_vol >= self.trade_th and self.ask_drop >= self.drop_th:
            self.lead_px = self.order_book.asks[self.horizon - 1][0]
            self.logger.warning("Lead Buy @ " + str(self.lead_px))
            self.logger.warning("Status: " + str(self.status) + indent +
                                "BookRatio1: " + str(ratio_book1) + indent +
                                "BookRatio2: " + str(ratio_book2) + indent +
                                "AggBookSizeTrue: " + str(agg_book_true[1]) + indent +
                                "AskTradeVol: " + str(self.ask_trade_vol) + indent +
                                "AskDrop: " + str(self.ask_drop)
                                )
            to_create_orders.append({
                'side': Side.BUY,
                'price': self.lead_px,
                'size': agg_book_true[1] + self.delta_ignore,
                'order_type': 'GTC'
            })  # lead order
            to_create_orders.append({
                'side': Side.SELL,
                'price': self.lead_px + self.tick_size * self.stop_gain,
                'size': agg_book_true[1] + self.delta_ignore,
                'order_type': 'GTC'
            })  # hedge order
            to_create_orders.append({
                'side': Side.BUY,
                'price': self.lead_px - self.tick_size * self.bluff_offset,
                'size': self.bluff_size,
                'order_type': 'GTC'
            })  # bluffing order
            if self.enable_trading:
                self.status = 1
        return to_create_orders, to_cancel_orders
    # active_orders: {orderID, order}

    def hedge_short(self, active_orders: dict):
        to_create_orders = []
        to_cancel_orders = []
        if abs(self.position_manager.position) > self.delta_ignore:
            if self.position_manager.position < 0:
                self.logger.warning("Lead executed. Current Open Pos: " + str(self.position_manager.position))
                self.status = -2
                to_create_orders, to_cancel_orders = self.stop_loss_short(active_orders)
            else:
                self.logger.warning("Lead not executed much. Need to stop loss. Current Open Pos: "
                                    + str(self.position_manager.position))
                self.status = 2
                self.lead_px = self.lead_px - self.tick_size * self.stop_gain
                to_create_orders, to_cancel_orders = self.stop_loss_long(active_orders)
        return to_create_orders, to_cancel_orders

    def stop_loss_short(self, active_orders: dict):
        if abs(self.position_manager.position) <= 0.5:
            self.status = 0
            self.logger.warning("Short Position all hedged. Cancel All Orders.")
            return [], list(active_orders.values())
        elif self.position_manager.position > self.delta_ignore:
            self.status = 2
            self.lead_px = self.lead_px - self.tick_size * self.stop_gain
            return self.stop_loss_long(active_orders)

        stop_loss_px = self.lead_px + (self.stop_loss - 1) * self.tick_size

        to_add = abs(self.position_manager.position)
        to_create_orders = []
        to_cancel_orders = []
        if stop_loss_px > self.order_book.bids[0][0]:
            for order in active_orders.values():
                if order.side == Side.BUY:
                    to_add -= order.size
            if to_add > 0:
                to_create_orders.append({
                    'side': Side.BUY,
                    'price': self.lead_px - self.tick_size * self.stop_gain,
                    'size': to_add,
                    'order_type': 'GTC'
                })  # lead order
            return to_create_orders, to_cancel_orders

        hedge_order_exist = False
        for order in active_orders.values():
            if order.side == Side.SELL:
                to_cancel_orders.append(order)
            elif order.price != self.order_book.bids[0][0]:
                to_cancel_orders.append(order)
            else:
                hedge_order_exist = True
        if not hedge_order_exist:
            to_create_orders.append({
                'side': Side.BUY,
                'price': self.order_book.bids[0][0],
                'size': abs(self.position_manager.position),
                'order_type': 'GTC'
            })  # lead order
            self.logger.warning("Levels go back. Stop Loss Start" +
                                ". Open position: " + str(self.position_manager.position) +
                                ". Lead Px: " + str(self.lead_px) +
                                ". Current Px:" + str(self.order_book.bids[0][0]))
        return to_create_orders, to_cancel_orders

    def hedge_long(self, active_orders: dict):
        to_create_orders = []
        to_cancel_orders = []
        if abs(self.position_manager.position) > self.delta_ignore:
            if self.position_manager.position > 0:
                self.logger.warning("Lead executed. Current Open Pos: " + str(self.position_manager.position))
                self.status = 2
                to_create_orders, to_cancel_orders = self.stop_loss_long(active_orders)
            else:
                self.logger.warning("Lead not executed much. Need to stop loss. Current Open Pos: "
                                    + str(self.position_manager.position))
                self.status = -2
                self.lead_px = self.lead_px + self.tick_size * self.stop_gain
                to_create_orders, to_cancel_orders = self.stop_loss_short(active_orders)
        return to_create_orders, to_cancel_orders

    def stop_loss_long(self, active_orders: dict):
        if abs(self.position_manager.position) <= 0.5:
            self.status = 0
            self.logger.warning("Long Position all hedged. Cancel All Orders.")
            return [], list(active_orders.values())
        elif self.position_manager.position < - self.delta_ignore:
            self.status = -2
            self.lead_px = self.lead_px + self.tick_size * self.stop_gain
            return self.stop_loss_short(active_orders)
        stop_loss_px = self.lead_px - (self.stop_loss - 1) * self.tick_size

        to_add = abs(self.position_manager.position)
        to_create_orders = []
        to_cancel_orders = []
        if stop_loss_px < self.order_book.asks[0][0]:
            for order in active_orders.values():
                if order.side == Side.SELL:
                    to_add -= order.size
            if to_add > 0:
                to_create_orders.append({
                    'side': Side.SELL,
                    'price': self.lead_px + self.tick_size * self.stop_gain,
                    'size': to_add,
                    'order_type': 'GTC'
                })  # lead order
            return to_create_orders, to_cancel_orders

        hedge_order_exist = False
        for order in active_orders.values():
            if order.side == Side.BUY:
                to_cancel_orders.append(order)
            elif order.price != self.order_book.asks[0][0]:
                to_cancel_orders.append(order)
            else:
                hedge_order_exist = True
        if not hedge_order_exist:
            to_create_orders.append({
                'side': Side.SELL,
                'price': self.order_book.asks[0][0],
                'size': abs(self.position_manager.position),
                'order_type': 'GTC'
            })  # lead order
            self.logger.warning("Levels go back. Stop Loss Start" +
                                ". Open position: " + str(self.position_manager.position) +
                                ". Lead Px: " + str(self.lead_px) +
                                ". Current Px:" + str(self.order_book.asks[0][0]))
        return to_create_orders, to_cancel_orders

    # Core strategy
    def get_decisions(self, active_orders: dict):
        to_create_orders = []
        to_cancel_orders = []
        if self.order_book.is_invalid():
            # @@ Best case is to do nothing. Worst case is to cancel all orders. Need to test the robustness
            return [], []
        if self.bid_size > Decimal(0) and self.bid_px == self.order_book.bids[0][0]:
            self.bid_drop = self.bid_size - self.order_book.bids[0][1]
        else:
            if self.bid_px != self.order_book.bids[0][0]:
                self.bid_trade_vol = Decimal(0)
                self.bid_px = self.order_book.bids[0][0]
            self.bid_drop = Decimal(0)
        if self.ask_size > Decimal(0) and self.ask_px == self.order_book.asks[0][0]:
            self.ask_drop = self.ask_size - self.order_book.asks[0][1]
        else:
            if self.ask_px != self.order_book.asks[0][0]:
                self.ask_trade_vol = Decimal(0)
                self.ask_px = self.order_book.asks[0][0]
            self.ask_drop = Decimal(0)
        self.bid_size = self.order_book.bids[0][1]
        self.ask_size = self.order_book.asks[0][1]
        # Lead
        if self.status == 0:
            to_create_orders, to_cancel_orders = self.lead()
        # Hedge
        elif self.status == -1:
            to_create_orders, to_cancel_orders = self.hedge_short(active_orders)
        elif self.status == 1:
            to_create_orders, to_cancel_orders = self.hedge_long(active_orders)
        elif self.status == -2:
            to_create_orders, to_cancel_orders = self.stop_loss_short(active_orders)
        elif self.status == 2:
            to_create_orders, to_cancel_orders = self.stop_loss_long(active_orders)
        return to_create_orders, to_cancel_orders

    async def start(self, loop_instance):
        await self.init(loop_instance)
        while True:
            try:
                await self.wait_and_process_market_data_updates()

                # Core strategy entrance
                to_create_orders, to_cancel_orders = self.get_decisions(self.trading_session.active_orders)

                for order in to_cancel_orders:
                    await self.trading_session.cancel_order(order.strategy_order_id, self.order_book.correlation_id)

                if to_create_orders and self.exchange == 'HUOBI_CONTRACT':
                    # add offset according to local position
                    to_create_orders = self.position_manager.get_orders_from_multiple(to_create_orders)

                for order_info in to_create_orders:
                    if self.enable_trading:
                        await self.trading_session.create_order(
                            side=order_info['side'],
                            price=order_info['price'],
                            size=order_info['size'],
                            order_type=order_info['order_type'],
                            correlation_id=self.order_book.correlation_id,
                            offset=order_info['offset'] if 'offset' in order_info else None
                        )
                    else:
                        self.logger.warning("drop order {}. trading not enabled".format(order_info))

            except Exception as e:
                self.logger.exception(e)


if __name__ == '__main__':
    strategy_helper = StrategyTradingHelper()
    strategy_helper.set_parser()

    loop = asyncio.get_event_loop()
    loop.run_until_complete(strategy_helper.load_symbol_helper())

    strategy = Strategy(exchange=strategy_helper.config['strategy']['exchange'],
                        global_symbol=strategy_helper.config['strategy']['global_symbol'],
                        strategy_helper_instance=strategy_helper)

    loop.run_until_complete(strategy.start(loop))

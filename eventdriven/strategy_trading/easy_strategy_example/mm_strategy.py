import json

from loguru import logger
from decimal import Decimal, ROUND_UP, ROUND_DOWN

from strategy_trading.easy_strategy.env import StrategyEnv
from strategy_trading.easy_strategy.strategy_base import StrategyImplBase
from strategy_trading.easy_strategy.datafeed import FullLevelOrderBook, FixedLevelOrderBook, Trade, Kline, Ticker, IndexTicker
from strategy_trading.easy_strategy.order import Order
from strategy_trading.easy_strategy.data_type import Side, OrderType, PositionEffect


class NaiveMMStrategy(StrategyImplBase):
    def __init__(self):
        super(NaiveMMStrategy, self).__init__()

    async def on_full_level_orderbook(self, orderbook: FullLevelOrderBook):
        bids = orderbook.bids()
        asks = orderbook.asks()
        logger.info('exchange={} symbol={} bid/ask={:.6f}/{:.6f}',
            orderbook.instrument.exchange,
            orderbook.instrument.global_symbol,
            bids[0][0],
            asks[0][0])

    async def on_fixed_level_orderbook(self, orderbook: FixedLevelOrderBook):
        print('receive', orderbook.connectivity_timestamp)
        bids = orderbook.bids()
        asks = orderbook.asks()
        # logger.info('exchange={} symbol={} bid/ask={:.6f}/{:.6f}',
        #     orderbook.instrument.exchange,
        #     orderbook.instrument.global_symbol,
        #     bids[0][0],
        #     asks[0][0])

        best_bid = bids[0][0]
        best_ask = asks[0][0]

        if self.quotable:
            if self.bid_order:
                spread_ratio = abs(best_bid - self.bid_order.price) / best_bid
                if spread_ratio <= self.min_spread_ratio:
                    await self.cancel(self.bid_order)
            else:
                price = best_bid * (1 - self.quote_spread_ratio)
                price = price.quantize(self.instrument.price_tick, ROUND_DOWN)
                quantity = self.new_order_qty.quantize(self.instrument.lot_size, ROUND_DOWN)
                if quantity < self.instrument.min_order_size:
                    return
                self.bid_order = Order(
                    account=self.account,
                    instrument=self.instrument,
                    side=Side.BUY,
                    position_effect=PositionEffect.NONE,
                    order_type=OrderType.POST_ONLY,
                    margin_trade=False,
                    price=price,
                    quantity=quantity
                )
                self.virtual_net += self.bid_order.unfilled_quantity
                await self.place(self.bid_order)

            if self.ask_order:
                spread_ratio = abs(self.ask_order.price - best_ask) / best_ask
                if spread_ratio <= self.min_spread_ratio:
                    await self.cancel(self.ask_order)
            else:
                price = best_ask * (1 + self.quote_spread_ratio)
                price = price.quantize(self.instrument.price_tick, ROUND_UP)
                quantity = self.new_order_qty.quantize(self.instrument.lot_size, ROUND_DOWN)
                if quantity < self.instrument.min_order_size:
                    return
                self.ask_order = Order(
                    account=self.account,
                    instrument=self.instrument,
                    side=Side.SELL,
                    position_effect=PositionEffect.NONE,
                    order_type=OrderType.POST_ONLY,
                    margin_trade=False,
                    price=price,
                    quantity=quantity
                )
                self.virtual_net -= self.ask_order.unfilled_quantity
                await self.place(self.ask_order)
        else:
            if abs(self.virtual_net) < self.instrument.min_order_size and abs(self.realized_net) < self.instrument.min_order_size:
                self.quotable = True
                return

            if self.bid_order or self.ask_order:
                if self.bid_order:
                    await self.cancel(self.bid_order)
                if self.ask_order:
                    await self.cancel(self.ask_order)
                return

            side = Side.BUY if self.realized_net < Decimal('0') else Side.SELL
            price = best_ask if side == Side.BUY else best_bid
            quantity = self.realized_net.quantize(self.instrument.lot_size, ROUND_DOWN)

            if self.hedge_order:
                if self.hedge_order.price != price:
                    await self.cancel(self.hedge_order)
                return

            self.hedge_order = Order(
                portfolio=self.portfolio,
                account=self.account,
                instrument=self.instrument,
                side=side,
                position_effect=PositionEffect.NONE,
                order_type=OrderType.GTC,
                margin_trade=False,
                price=price,
                quantity=quantity
            )
            await self.place(self.hedge_order)

    async def on_trade(self, trade: Trade):
        logger.info('exchange={} symbol={} side={} price={} quantity={}',
            trade.instrument.exchange,
            trade.instrument.global_symbol,
            trade.side.name,
            trade.price,
            trade.quantity)

    async def on_kline(self, kline: Kline):
        logger.info('exchange={} symbol={} open={} high={} low={} close={} volume={} amount={} open_timestamp={} close_timestamp={}',
            kline.instrument.exchange,
            kline.instrument.global_symbol,
            kline.open,
            kline.high,
            kline.low,
            kline.close,
            kline.volume,
            kline.amount,
            kline.open_timestamp,
            kline.close_timestamp)

    async def on_ticker(self, ticker: Ticker):
        logger.info('exchange={} symbol={} last_price={} last_quantity={} bid/ask={}/{}',
            ticker.instrument.exchange,
            ticker.instrument.global_symbol,
            ticker.last_price,
            ticker.last_quantity,
            ticker.best_bid_price,
            ticker.best_ask_price)

    async def on_place_confirm(self, order: Order):
        logger.info('on_place_confirm. coid={} side={} price={}',
            order.client_order_id,
            order.side.name,
            '%.{}f'.format(order.instrument.price_precision) % order.price)

        logger.info('virtual_net={} realized_net={}', self.virtual_net, self.realized_net)

    async def on_place_reject(self, order: Order):
        logger.info('on_place_reject. coid={} reason={}', order.client_order_id, order.place_reject_reason)

        if order.side == Side.BUY:
            self.virtual_net -= order.unfilled_quantity
        else:
            self.virtual_net += order.unfilled_quantity

        if self.bid_order == order:
            self.bid_order = None
        elif self.ask_order == order:
            self.ask_order = None
        else:
            self.hedge_order = None

        logger.info('virtual_net={} realized_net={}', self.virtual_net, self.realized_net)

    async def on_cancel_reject(self, order: Order):
        logger.info('on_cancel_reject. coid={} reason={}', order.client_order_id, order.cancel_reject_reason)

    async def on_order_update(self, order: Order):
        logger.info('on_order_update. coid={} status={} filled_quantity={} unfilled_quantity={} last_trade_quantity={} last_trade_price={}',
            order.client_order_id,
            order.status.name,
            '%.{}f'.format(order.instrument.quantity_precision) % order.filled_quantity,
            '%.{}f'.format(order.instrument.quantity_precision) % order.unfilled_quantity,
            '%.{}f'.format(order.instrument.quantity_precision) % order.last_trade_quantity,
            '%.{}f'.format(order.instrument.price_precision) % order.last_trade_price)

        if order.last_trade_quantity != Decimal('0'):
            if order.side == Side.BUY:
                self.realized_net += order.last_trade_quantity
            else:
                self.realized_net -= order.last_trade_quantity

            if self.realized_net >= self.max_hold_net:
                self.quotable = False

        if order.is_finished():
            if order.side == Side.BUY:
                self.virtual_net -= order.unfilled_quantity
            else:
                self.virtual_net += order.unfilled_quantity

            if self.bid_order == order:
                self.bid_order = None
            elif self.ask_order == order:
                self.ask_order = None
            else:
                self.hedge_order = None

        logger.info('virtual_net={} realized_net={}', self.virtual_net, self.realized_net)

    async def start(self):
        self.exchange = 'HUOBI_SWAP'
        self.account = 'hbtest1am1'
        self.new_order_qty = Decimal('1')
        self.max_hold_net = Decimal('1')
        self.quote_spread_ratio = Decimal('0.001')
        self.min_spread_ratio = Decimal('0.0009')
        self.bid_order = None
        self.ask_order = None
        self.hedge_order = None
        self.quotable = True

        self.virtual_net = Decimal('0')
        self.realized_net = Decimal('0')

        self.instrument = self.get_instrument('HUOBI_SWAP', 'SWAP-BTC/USD')
        await self.subscribe_fixed_orderbook(self.instrument, 20)
        # await self.subscribe_full_orderbook(instrument)
        # await self.subscribe_trade(instrument)
        # await self.subscribe_ticker(instrument)
        # await self.subscribe_kline(instrument, 60)


if __name__ == "__main__":
    config = json.load(open('./easy_strategy_example/config.json'))
    #config = json.load(open('C:\Users\Huobi\Downloads\eventdriven\strategy_trading\easy_strategy_example\config-example.json))

    #C:\Users\Huobi\Downloads\eventdriven\strategy_trading\easy_strategy_example\config-example.json)
    test_strategy = NaiveMMStrategy()

    env = StrategyEnv(config)
    env.init(test_strategy, portfolio='test')
    env.run()

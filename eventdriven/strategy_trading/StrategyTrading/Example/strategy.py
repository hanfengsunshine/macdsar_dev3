import logging
from strategy_trading.StrategyTrading.tradingHelper import StrategyTradingHelper
from strategy_trading.StrategyTrading.tradingGatewayConnection import TradingConnection, TradingSession
from strategy_trading.StrategyTrading.marketDataConnection import MarketDataConnection
import asyncio
from strategy_trading.StrategyTrading.marketData import OrderBookDepth, OrderBookDiff, MarketTrade
from strategy_trading.StrategyTrading.orderBook import BaseOrderBook
from strategy_trading.StrategyTrading.order import Side
from strategy_trading.StrategyTrading.positionManager import SigSybPM, SigSybPMOS
from strategy_trading.StrategyTrading.inventoryManager import InventoryManager


class Strategy():
    def __init__(self, exchange, global_symbol, strategy_helper: StrategyTradingHelper):
        self.exchange = exchange
        self.global_symbol = global_symbol
        self.logger = logging.getLogger(self.__class__.__name__)
        self.strategy_helper = strategy_helper

        # get symbol tick size, min_order_size here
        symbol_info = self.strategy_helper.symbol_helper.get_info(global_symbol, exchange)
        self.symbol = symbol_info['symbol']
        self.order_book = BaseOrderBook(exchange, self.global_symbol)
        self.min_order_size = symbol_info['min_order_size']
        self.strategy_config = strategy_helper.config['strategy']
        self.enable_trading = False if ('enable_trading' not in self.strategy_config or not self.strategy_config['enable_trading']) else True

    async def init(self, loop):
        # connect to trading gateway
        self.trading_connection = await self.strategy_helper.connect_to_trading_gateway(
            loop = loop,
            gateway_name=self.exchange,
            strategy = self.__class__.__name__,
            process = "Default",
            receive_error = False
        )

        if self.exchange != 'HUOBI_CONTRACT':
            self.inventory_manager = InventoryManager(self.strategy_config['balance'], self.exchange, symbol_helper=self.strategy_helper.symbol_helper)
            self.position_manager = SigSybPM(self.exchange, self.global_symbol, self.strategy_helper.symbol_helper,
                                                                    alias='Default',
                                                                    inventory_manager=self.inventory_manager)
        else:
            self.inventory_manager = None
            self.position_manager = SigSybPMOS(self.exchange, self.global_symbol, self.strategy_helper.symbol_helper, alias='Default')

        self.trading_session = self.trading_connection.get_session(self.global_symbol, callback_when_exec = self.position_manager.add_trade)

        # connect to market data
        self.market_data_connection = await self.strategy_helper.connect_to_market_data_gateway(
            loop = loop,
            md_name = self.exchange
        )
        await self.market_data_connection.subscribe(self.global_symbol,
                                              want_orderbook=True,
                                              want_trades=True,
                                              book_level=50)

        await self.strategy_helper.send_heartbeat(loop, "TEST", self.global_symbol)

    async def wait_and_process_market_data_updates(self):
        updates = await self.market_data_connection.get_all_updates(global_symbol=self.global_symbol)
        for _update in updates:
            if isinstance(_update, MarketTrade):
                pass
            elif isinstance(_update, OrderBookDiff):
                pass
            elif isinstance(_update, OrderBookDepth):
                self.order_book.update_depth(_update)

    def get_decisions(self, active_orders: dict):
        to_create_orders = []
        to_cancel_orders = []
        if active_orders:
            to_cancel_orders = list(active_orders.values())
        else:
            if self.order_book.is_invalid():
                # do something
                return [], []
            # place order
            to_create_orders.append({
                'side': Side.BUY,
                'price': self.order_book.bids[-1][0],
                'size': self.min_order_size,
                'order_type': 'GTC'
            })
        return to_create_orders, to_cancel_orders

    async def start(self, loop):
        await self.init(loop)
        while True:
            try:
                # you can also use a coroutine to process the data. Once any data is coming, trigger the main loop after last loop is done
                await self.wait_and_process_market_data_updates()

                to_create_orders, to_cancel_orders = self.get_decisions(self.trading_session.active_orders)

                for order in to_cancel_orders:
                    await self.trading_session.cancel_order(order.strategy_order_id, self.order_book.correlation_id)

                if to_create_orders and self.exchange == 'HUOBI_CONTRACT':
                    # add offset according to local position
                    to_create_orders = self.position_manager.get_orders_from_multiple(to_create_orders)

                for order_info in to_create_orders:
                    if self.enable_trading:
                        await self.trading_session.create_order(
                            side = order_info['side'],
                            price = order_info['price'],
                            size = order_info['size'],
                            order_type= order_info['order_type'],
                            correlation_id= self.order_book.correlation_id,
                            offset = order_info['offset'] if 'offset' in order_info else None
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
                        strategy_helper = strategy_helper)

    loop.run_until_complete(strategy.start(loop))

import asyncio

from typing import List

from strategy_trading.easy_strategy.i_strategy_api import IStrategyAPI
from strategy_trading.easy_strategy.i_strategy_spi import IStrategySPI
from strategy_trading.easy_strategy.env import StrategyEnv
from strategy_trading.easy_strategy.instrument_manager import Instrument, InstrumentManager
from strategy_trading.easy_strategy.datafeed_manager import DatafeedManager
from strategy_trading.easy_strategy.trade_manager import TradeManager
from strategy_trading.easy_strategy.datafeed import FullLevelOrderBook, FixedLevelOrderBook, TopOrderBook, Trade, Kline, Ticker, IndexTicker
from strategy_trading.easy_strategy.order import Order


class StrategyImplBase(IStrategyAPI, IStrategySPI):
    def __init__(self):
        self.env = None
        self.is_ready = asyncio.Event()

    def get_instrument(self, exchange: str, global_symbol: str):
        return self.env.get_instrument(exchange, global_symbol)

    def get_balance(self, exchange: str, account: str, currency: str):
        return self.env.get_balance(exchange, account, currency)

    def get_portfolio_position(self, portfolio: str, instrument: Instrument):
        return self.env.get_portfolio_position(portfolio, instrument)

    def get_account_position(self, account: str, instrument: Instrument):
        return self.env.get_account_position(account, instrument)

    def get_full_orderbook(self, instrument: Instrument) -> FullLevelOrderBook:
        return self.env.get_full_orderbook(instrument)

    def get_fixed_orderbook(self, instrument: Instrument, level: int) -> FixedLevelOrderBook:
        return self.env.get_fixed_orderbook(instrument, level)

    def get_top_orderbook(self, instrument: Instrument) -> TopOrderBook:
        return self.env.get_top_orderbook(instrument)

    def get_live_order(self, client_order_id: str):
        return self.env.get_live_order(client_order_id)

    def get_live_orders(self):
        return self.env.get_live_orders()

    async def send_command_response(self, response: str):
        await self.env.send_command_response(response)

    async def subscribe_full_orderbook(self, instrument: Instrument):
        await self.env.subscribe_full_orderbook(instrument)

    async def subscribe_fixed_orderbook(self, instrument: Instrument, level: int):
        await self.env.subscribe_fixed_orderbook(instrument, level)

    async def subscribe_top_orderbook(self, instrument: Instrument):
        await self.env.subscribe_top_orderbook(instrument)

    async def subscribe_trade(self, instrument: Instrument):
        await self.env.subscribe_trade(instrument)

    async def subscribe_kline(self, instrument: Instrument, interval_in_sec:int = 60):
        await self.env.subscribe_kline(instrument, interval_in_sec)

    async def subscribe_ticker(self, instrument: Instrument):
        await self.env.subscribe_ticker(instrument)

    async def place(self, order: Order, priority: int = 0):
        await self.env.place(order)

    async def bulk_place(self, orders: List[Order], priority: int = 0):
        await self.env.bulk_place(orders)

    async def cancel(self, order: Order, priority: int = 0):
        await self.env.cancel(order)

    async def bulk_cancel(self, orders: List[Order], priority: int = 0):
        await self.env.bulk_cancel(orders)

    async def on_command(self, request: dict):
        pass

    async def on_full_level_orderbook(self, ob: FullLevelOrderBook):
        pass

    async def on_fixed_level_orderbook(self, ob: FixedLevelOrderBook):
        pass

    async def on_top_orderbook(self, ob: TopOrderBook):
        pass

    async def on_trade(self, trades: List[Trade]):
        pass

    async def on_kline(self, kline: Kline):
        pass

    async def on_ticker(self, ticker: Ticker):
        pass

    async def on_index_ticker(self, index_ticker: IndexTicker):
        pass

    async def on_place_timeout(self, order: Order):
        pass

    async def on_place_confirm(self, order: Order):
        pass

    async def on_place_reject(self, order: Order):
        pass

    async def on_cancel_timeout(self, order: Order):
        pass

    async def on_cancel_reject(self, order: Order):
        pass

    async def on_order_update(self, order: Order):
        pass

    async def start(self):
        pass

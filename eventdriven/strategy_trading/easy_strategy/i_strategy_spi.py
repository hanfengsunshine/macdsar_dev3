from abc import ABC, abstractmethod
from typing import List

from strategy_trading.easy_strategy.datafeed import FullLevelOrderBook, FixedLevelOrderBook, Trade, Kline, Ticker, IndexTicker, TopOrderBook
from strategy_trading.easy_strategy.order import Order


class IStrategySPI(ABC):
    @abstractmethod
    async def on_command(self, request: dict):
        raise NotImplementedError

    @abstractmethod
    async def on_full_level_orderbook(self, ob: FullLevelOrderBook):
        raise NotImplementedError

    @abstractmethod
    async def on_fixed_level_orderbook(self, ob: FixedLevelOrderBook):
        raise NotImplementedError

    @abstractmethod
    async def on_top_orderbook(self, ob: TopOrderBook):
        raise NotImplementedError

    @abstractmethod
    async def on_trade(self, trades: List[Trade]):
        raise NotImplementedError

    @abstractmethod
    async def on_kline(self, kline: Kline):
        raise NotImplementedError

    @abstractmethod
    async def on_ticker(self, ticker: Ticker):
        raise NotImplementedError

    @abstractmethod
    async def on_index_ticker(self, index_ticker: IndexTicker):
        raise NotImplementedError

    @abstractmethod
    async def on_place_timeout(self, order: Order):
        raise NotImplementedError

    @abstractmethod
    async def on_place_confirm(self, order: Order):
        raise NotImplementedError

    @abstractmethod
    async def on_place_reject(self, order: Order):
        raise NotImplementedError

    @abstractmethod
    async def on_cancel_timeout(self, order: Order):
        raise NotImplementedError

    @abstractmethod
    async def on_cancel_reject(self, order: Order):
        raise NotImplementedError

    @abstractmethod
    async def on_order_update(self, order: Order):
        raise NotImplementedError

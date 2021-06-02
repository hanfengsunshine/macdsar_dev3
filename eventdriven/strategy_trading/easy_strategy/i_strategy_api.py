from typing import List
from abc import ABC, abstractmethod

from strategy_trading.easy_strategy.instrument_manager import Instrument
from strategy_trading.easy_strategy.order import Order


class IStrategyAPI(ABC):
    @abstractmethod
    def get_instrument(self, global_symbol: str, exchange: str):
        raise NotImplementedError

    @abstractmethod
    def get_balance(self, exchange: str, account: str, currency: str):
        raise NotImplementedError

    @abstractmethod
    def get_portfolio_position(self, portfolio: str, instrument: Instrument):
        raise NotImplementedError

    @abstractmethod
    def get_account_position(self, account: str, instrument: Instrument):
        raise NotImplementedError

    @abstractmethod
    def get_full_orderbook(self, instrument: Instrument):
        raise NotImplementedError

    @abstractmethod
    def get_fixed_orderbook(self, instrument: Instrument, level: int):
        raise NotImplementedError

    @abstractmethod
    def get_top_orderbook(self, instrument: Instrument):
        raise NotImplementedError

    @abstractmethod
    def get_live_order(self, client_order_id: str):
        raise NotImplementedError

    @abstractmethod
    def get_live_orders(self):
        raise NotImplementedError

    @abstractmethod
    async def subscribe_full_orderbook(self, instrument: Instrument):
        raise NotImplementedError

    @abstractmethod
    async def subscribe_fixed_orderbook(self, instrument: Instrument, level: int):
        raise NotImplementedError

    @abstractmethod
    async def subscribe_top_orderbook(self, instrument: Instrument):
        raise NotImplementedError

    @abstractmethod
    async def subscribe_trade(self, instrument: Instrument):
        raise NotImplementedError

    @abstractmethod
    async def subscribe_kline(self, instrument: Instrument, interval_in_sec: int = 60):
        raise NotImplementedError

    @abstractmethod
    async def subscribe_ticker(self, instrument: Instrument):
        raise NotImplementedError

    @abstractmethod
    async def place(self, order: Order, priority: int = 0):
        raise NotImplementedError

    @abstractmethod
    async def bulk_place(self, orders: List[Order], priority: int = 0):
        raise NotImplementedError

    @abstractmethod
    async def cancel(self, order: Order, priority: int = 0):
        raise NotImplementedError

    @abstractmethod
    async def bulk_cancel(self, orders: List[Order], priority: int = 0):
        raise NotImplementedError

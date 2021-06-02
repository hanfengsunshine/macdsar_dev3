import asyncio

from strategy_trading.easy_strategy.utils import get_current_us
from strategy_trading.easy_strategy.instrument_manager import Instrument
from strategy_trading.easy_strategy.datafeed import FullLevelOrderBook, FixedLevelOrderBook, TopOrderBook, Trade, Kline, Ticker, IndexTicker


class MarketBase:
    def __init__(self):
        self._full_obs = dict()
        self._fixed_obs = dict()
        self._top_obs = dict()
        self._klines = dict()
        self._timeout_sec = 30

    def register_callback(self, on_full_ob, on_fixed_ob, on_top_ob, on_kline, on_trade):
        self._on_full_ob = on_full_ob
        self._on_fixed_ob = on_fixed_ob
        self._on_top_ob = on_top_ob
        self._on_kline = on_kline
        self._on_trade = on_trade

    def get_full_orderbook(self, instrument: Instrument) -> FullLevelOrderBook:
        if instrument not in self._full_obs:
            self._full_obs[instrument] = FullLevelOrderBook(instrument)
        return self._full_obs[instrument]

    def get_fixed_orderbook(self, instrument: Instrument, level: int) -> FixedLevelOrderBook:
        if (instrument, level) not in self._fixed_obs:
            self._fixed_obs[(instrument, level)] = FixedLevelOrderBook(instrument, level)
        return self._fixed_obs[(instrument, level)]

    def get_top_orderbook(self, instrument: Instrument) -> TopOrderBook:
        if instrument not in self._top_obs:
            self._top_obs[instrument] = TopOrderBook(instrument)
        return self._top_obs[instrument]

    async def subscribe(self,
        instrument: Instrument,
        full_level_orderbook: bool = False,
        fixed_level_orderbook: int = None,
        top_orderbook: bool = False,
        trade: bool = False,
        kline: int = None,
        ticker: bool = False,
        index_ticker: bool = False):
        pass

    async def unsubscribe(self,
        instrument: Instrument,
        full_level_orderbook: bool = False,
        fixed_level_orderbook: int = None,
        top_orderbook: bool = False,
        trade: bool = False,
        kline: int = None,
        ticker: bool = False,
        index_ticker: bool = False):
        pass

    async def run(self):
        pass

    async def _check_datafeed_timeout(self):
        await asyncio.sleep(self._timeout_sec * 2)

        current_ts = get_current_us()
        timeout_us = self._timeout_sec * 1000000

        for instrument, full_ob in self._full_obs.items():
            if full_ob.exchange_timestamp != 0 and current_ts - full_ob.exchange_timestamp >= timeout_us:
                await self.unsubscribe(instrument, full_level_orderbook=True)
                await asyncio.sleep(1)
                await self.subscribe(instrument, full_level_orderbook=True)

        for (instrument, level), fixed_ob in self._fixed_obs.items():
            if fixed_ob.exchange_timestamp != 0 and current_ts - fixed_ob.exchange_timestamp >= timeout_us:
                await self.unsubscribe(instrument, fixed_level_orderbook=level)
                await asyncio.sleep(1)
                await self.subscribe(instrument, fixed_level_orderbook=level)

        for instrument, top_ob in self._top_obs.items():
            if top_ob.exchange_timestamp != 0 and current_ts - top_ob.exchange_timestamp >= timeout_us:
                await self.unsubscribe(instrument, top_orderbook=True)
                await asyncio.sleep(1)
                await self.subscribe(instrument, top_orderbook=True)

        asyncio.ensure_future(self._check_datafeed_timeout())

import zmq
import orjson
import asyncio
import websockets

from abc import ABC
from enum import Enum
from typing import List
from decimal import Decimal
from collections import defaultdict
from zmq.asyncio import Context
from loguru import logger
from strategy_trading.easy_strategy.data_type import DatafeedType
from strategy_trading.easy_strategy.instrument_manager import Instrument, InstrumentManager
from strategy_trading.easy_strategy.datafeed import FullLevelOrderBook, FixedLevelOrderBook, TopOrderBook, Trade, Kline, Ticker, IndexTicker
from strategy_trading.easy_strategy.market.binance_spot import BinanceSpotMarket
from strategy_trading.easy_strategy.market.binance_swap import BinanceSwapMarket
from strategy_trading.easy_strategy.market.binance_linear_swap import BinanceLinearSwapMarket
from strategy_trading.easy_strategy.market.binance_future import BinanceFutureMarket
from strategy_trading.easy_strategy.market.binance_index import BinanceIndexMarket
from strategy_trading.easy_strategy.market.huobi_spot import HuobiSpotMarket
from strategy_trading.easy_strategy.market.huobi_swap import HuobiSwapMarket
from strategy_trading.easy_strategy.market.huobi_linear_swap import HuobiLinearSwapMarket
from strategy_trading.easy_strategy.market.huobi_future import HuobiFutureMarket
from strategy_trading.easy_strategy.market.huobi_index import HuobiIndexMarket
from strategy_trading.easy_strategy.market.okex_spot import OkexSpotMarket
from strategy_trading.easy_strategy.market.okex_swap import OkexSwapMarket
from strategy_trading.easy_strategy.market.okex_future import OkexFutureMarket
from strategy_trading.easy_strategy.market.ftx_spot import FtxSpotMarket
from strategy_trading.easy_strategy.market.ftx_swap import FtxSwapMarket
from strategy_trading.easy_strategy.market.ftx_future import FtxFutureMarket
from strategy_trading.easy_strategy.market.coinbase_spot import CoinbaseSpotMarket


class DirectDatafeedManager:
    def __init__(self):
        self._full_obs = dict()
        self._fixed_obs = dict()
        self._top_obs = dict()
        self._on_full_level_orderbook_cb = None
        self._on_fixed_level_orderbook_cb = None
        self._on_top_orderbook_cb = None
        self._on_trade_cb = None
        self._on_kline_cb = None
        self._on_ticker_cb = None
        self._on_index_ticker_cb = None

    def init(self, config: dict):
        self._markets = defaultdict(dict)
        if 'BINANCE_SPOT' in config:
            binance_spot_market = BinanceSpotMarket(config['BINANCE_SPOT']['url'])
            binance_spot_market.register_callback(
                on_full_ob=self._on_full_level_orderbook,
                on_fixed_ob=self._on_fixed_level_orderbook,
                on_top_ob=self._on_top_orderbook,
                on_kline=self._on_kline,
                on_trade=self._on_trade
            )
            self._markets['BINANCE_SPOT']['SPOT'] = binance_spot_market
            asyncio.ensure_future(binance_spot_market.run())
        if 'BINANCE_SWAP' in config:
            binance_swap_market = BinanceSwapMarket(config['BINANCE_SWAP']['url'])
            binance_swap_market.register_callback(
                on_full_ob=self._on_full_level_orderbook,
                on_fixed_ob=self._on_fixed_level_orderbook,
                on_top_ob=self._on_top_orderbook,
                on_kline=self._on_kline,
                on_trade=self._on_trade
            )
            self._markets['BINANCE_SWAP']['PERPETUAL SWAP'] = binance_swap_market
            asyncio.ensure_future(binance_swap_market.run())
        if 'BINANCE_SWAP_USDT' in config:
            binance_linear_swap_market = BinanceLinearSwapMarket(config['BINANCE_SWAP_USDT']['url'])
            binance_linear_swap_market.register_callback(
                on_full_ob=self._on_full_level_orderbook,
                on_fixed_ob=self._on_fixed_level_orderbook,
                on_top_ob=self._on_top_orderbook,
                on_kline=self._on_kline,
                on_trade=self._on_trade
            )
            self._markets['BINANCE_SWAP_USDT']['PERPETUAL SWAP'] = binance_linear_swap_market
            asyncio.ensure_future(binance_linear_swap_market.run())
        if 'BINANCE_CONTRACT' in config:
            binance_future_market = BinanceFutureMarket(config['BINANCE_CONTRACT']['url'])
            binance_future_market.register_callback(
                on_full_ob=self._on_full_level_orderbook,
                on_fixed_ob=self._on_fixed_level_orderbook,
                on_top_ob=self._on_top_orderbook,
                on_kline=self._on_kline,
                on_trade=self._on_trade
            )
            self._markets['BINANCE_CONTRACT']['FUTURES'] = binance_future_market
            asyncio.ensure_future(binance_future_market.run())
        if 'BINANCE_INDEX' in config:
            binance_index_market = BinanceIndexMarket(config['BINANCE_INDEX']['url'])
            binance_index_market.register_callback(
                on_full_ob=self._on_full_level_orderbook,
                on_fixed_ob=self._on_fixed_level_orderbook,
                on_top_ob=self._on_top_orderbook,
                on_kline=self._on_kline,
                on_trade=self._on_trade
            )
            self._markets['BINANCE_INDEX']['INDEX'] = binance_index_market
            asyncio.ensure_future(binance_index_market.run())
        if 'HUOBI_SPOT' in config:
            huobi_spot_market = HuobiSpotMarket(config['HUOBI_SPOT']['url'])
            huobi_spot_market.register_callback(
                on_full_ob=self._on_full_level_orderbook,
                on_fixed_ob=self._on_fixed_level_orderbook,
                on_top_ob=self._on_top_orderbook,
                on_kline=self._on_kline,
                on_trade=self._on_trade
            )
            self._markets['HUOBI_SPOT']['SPOT'] = huobi_spot_market
            asyncio.ensure_future(huobi_spot_market.run())
        if 'HUOBI_SWAP' in config:
            huobi_swap_market = HuobiSwapMarket(config['HUOBI_SWAP']['url'])
            huobi_swap_market.register_callback(
                on_full_ob=self._on_full_level_orderbook,
                on_fixed_ob=self._on_fixed_level_orderbook,
                on_top_ob=self._on_top_orderbook,
                on_kline=self._on_kline,
                on_trade=self._on_trade
            )
            self._markets['HUOBI_SWAP']['PERPETUAL SWAP'] = huobi_swap_market
            asyncio.ensure_future(huobi_swap_market.run())
        if 'HUOBI_SWAP_USDT' in config:
            huobi_linear_swap_market = HuobiLinearSwapMarket(config['HUOBI_SWAP_USDT']['url'])
            huobi_linear_swap_market.register_callback(
                on_full_ob=self._on_full_level_orderbook,
                on_fixed_ob=self._on_fixed_level_orderbook,
                on_top_ob=self._on_top_orderbook,
                on_kline=self._on_kline,
                on_trade=self._on_trade
            )
            self._markets['HUOBI_SWAP_USDT']['PERPETUAL SWAP'] = huobi_linear_swap_market
            asyncio.ensure_future(huobi_linear_swap_market.run())
        if 'HUOBI_CONTRACT' in config:
            huobi_future_market = HuobiFutureMarket(config['HUOBI_CONTRACT']['url'])
            huobi_future_market.register_callback(
                on_full_ob=self._on_full_level_orderbook,
                on_fixed_ob=self._on_fixed_level_orderbook,
                on_top_ob=self._on_top_orderbook,
                on_kline=self._on_kline,
                on_trade=self._on_trade
            )
            self._markets['HUOBI_CONTRACT']['FUTURES'] = huobi_future_market
            asyncio.ensure_future(huobi_future_market.run())
        if 'HUOBI_INDEX' in config:
            huobi_index_market = HuobiIndexMarket(config['HUOBI_INDEX']['url'])
            huobi_index_market.register_callback(
                on_full_ob=self._on_full_level_orderbook,
                on_fixed_ob=self._on_fixed_level_orderbook,
                on_top_ob=self._on_top_orderbook,
                on_kline=self._on_kline,
                on_trade=self._on_trade
            )
            self._markets['HUOBI_INDEX']['INDEX'] = huobi_index_market
            asyncio.ensure_future(huobi_index_market.run())
        if 'OKEX_SPOT' in config:
            okex_spot_market = OkexSpotMarket(config['OKEX_SPOT']['url'])
            okex_spot_market.register_callback(
                on_full_ob=self._on_full_level_orderbook,
                on_fixed_ob=self._on_fixed_level_orderbook,
                on_top_ob=self._on_top_orderbook,
                on_kline=self._on_kline,
                on_trade=self._on_trade
            )
            self._markets['OKEX_SPOT']['SPOT'] = okex_spot_market
            asyncio.ensure_future(okex_spot_market.run())
        if 'OKEX_SWAP' in config:
            okex_swap_market = OkexSwapMarket(config['OKEX_SWAP']['url'])
            okex_swap_market.register_callback(
                on_full_ob=self._on_full_level_orderbook,
                on_fixed_ob=self._on_fixed_level_orderbook,
                on_top_ob=self._on_top_orderbook,
                on_kline=self._on_kline,
                on_trade=self._on_trade
            )
            self._markets['OKEX_SWAP']['PERPETUAL SWAP'] = okex_swap_market
            asyncio.ensure_future(okex_swap_market.run())
        if 'OKEX_CONTRACT' in config:
            okex_future_market = OkexFutureMarket(config['OKEX_CONTRACT']['url'])
            okex_future_market.register_callback(
                on_full_ob=self._on_full_level_orderbook,
                on_fixed_ob=self._on_fixed_level_orderbook,
                on_top_ob=self._on_top_orderbook,
                on_kline=self._on_kline,
                on_trade=self._on_trade
            )
            self._markets['OKEX_CONTRACT']['FUTURES'] = okex_future_market
            asyncio.ensure_future(okex_future_market.run())
        if 'FTX_SPOT' in config:
            ftx_spot_market = FtxSpotMarket(config['FTX_SPOT']['url'])
            ftx_spot_market.register_callback(
                on_full_ob=self._on_full_level_orderbook,
                on_fixed_ob=self._on_fixed_level_orderbook,
                on_top_ob=self._on_top_orderbook,
                on_kline=self._on_kline,
                on_trade=self._on_trade
            )
            self._markets['FTX_SPOT']['SPOT'] = ftx_spot_market
            asyncio.ensure_future(ftx_spot_market.run())
        if 'FTX_SWAP_USD' in config:
            ftx_swap_market = FtxSwapMarket(config['FTX_SWAP_USD']['url'])
            ftx_swap_market.register_callback(
                on_full_ob=self._on_full_level_orderbook,
                on_fixed_ob=self._on_fixed_level_orderbook,
                on_top_ob=self._on_top_orderbook,
                on_kline=self._on_kline,
                on_trade=self._on_trade
            )
            self._markets['FTX_SWAP_USD']['PERPETUAL SWAP'] = ftx_swap_market
            asyncio.ensure_future(ftx_swap_market.run())
        if 'FTX_CONTRACT' in config:
            ftx_future_market = FtxFutureMarket(config['FTX_CONTRACT']['url'])
            ftx_future_market.register_callback(
                on_full_ob=self._on_full_level_orderbook,
                on_fixed_ob=self._on_fixed_level_orderbook,
                on_top_ob=self._on_top_orderbook,
                on_kline=self._on_kline,
                on_trade=self._on_trade
            )
            self._markets['FTX_CONTRACT']['FUTURES'] = ftx_future_market
            asyncio.ensure_future(ftx_future_market.run())
        if 'COINBASEPRO_SPOT' in config:
            coinbase_spot_market = CoinbaseSpotMarket(config['COINBASEPRO_SPOT']['url'])
            coinbase_spot_market.register_callback(
                on_full_ob=self._on_full_level_orderbook,
                on_fixed_ob=self._on_fixed_level_orderbook,
                on_top_ob=self._on_top_orderbook,
                on_kline=self._on_kline,
                on_trade=self._on_trade
            )
            self._markets['COINBASEPRO_SPOT']['SPOT'] = coinbase_spot_market
            asyncio.ensure_future(coinbase_spot_market.run())

    def register_callback(self,
        on_ready=None,
        on_full_level_orderbook=None,
        on_fixed_level_orderbook=None,
        on_top_orderbook=None,
        on_trade=None,
        on_kline=None,
        on_ticker=None,
        on_index_ticker=None,
        on_tob=None):
        self._on_ready_cb = on_ready
        self._on_full_level_orderbook_cb = on_full_level_orderbook
        self._on_fixed_level_orderbook_cb = on_fixed_level_orderbook
        self._on_top_orderbook_cb = on_top_orderbook
        self._on_trade_cb = on_trade
        self._on_kline_cb = on_kline
        self._on_ticker_cb = on_ticker
        self._on_index_ticker_cb = on_index_ticker
        self._on_tob_cb = None

    def get_full_orderbook(self, instrument: Instrument):
        market = self._markets[instrument.exchange][instrument.symbol_type]
        return market.get_full_orderbook(instrument)

    def get_fixed_orderbook(self, instrument: Instrument, level: int):
        market = self._markets[instrument.exchange][instrument.symbol_type]
        return market.get_fixed_orderbook(instrument, level)

    def get_top_orderbook(self, instrument: Instrument):
        market = self._markets[instrument.exchange][instrument.symbol_type]
        return market.get_top_orderbook(instrument)

    async def subscribe(self,
            instrument: Instrument,
            full_level_orderbook: bool = False,
            fixed_level_orderbook: int = None,
            top_orderbook: bool = False,
            trade: bool = False,
            kline: int = None,
            ticker: bool = False,
            index_ticker: bool = False):
        if not instrument:
            return
        if instrument.exchange in self._markets:
            market = self._markets[instrument.exchange][instrument.symbol_type]
            await market.subscribe(instrument, full_level_orderbook, fixed_level_orderbook, top_orderbook, trade, kline, ticker, index_ticker)

    async def unsubscribe(self,
            instrument: Instrument,
            full_level_orderbook: bool = False,
            fixed_level_orderbook: int = None,
            top_orderbook: bool = False,
            trade: bool = False,
            kline: int = None,
            ticker: bool = False,
            index_ticker: bool = False):
        if not instrument:
            return
        if instrument.exchange in self._markets:
            market = self._markets[instrument.exchange][instrument.symbol_type]
            await market.unsubscribe(instrument, full_level_orderbook, fixed_level_orderbook, top_orderbook, trade, kline, ticker, index_ticker)

    async def run(self):
        asyncio.ensure_future(self._on_ready_cb())

    async def _on_full_level_orderbook(self, ob: FullLevelOrderBook):
        await self._on_full_level_orderbook_cb(ob)

    async def _on_fixed_level_orderbook(self, ob: FixedLevelOrderBook):
        await self._on_fixed_level_orderbook_cb(ob)

    async def _on_top_orderbook(self, ob: TopOrderBook):
        await self._on_top_orderbook_cb(ob)

    async def _on_kline(self, kline: Kline):
        await self._on_kline_cb(kline)

    async def _on_trade(self, trades: List[Trade]):
        await self._on_trade_cb(trades)

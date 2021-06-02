import zmq
import orjson
import asyncio
import websockets

from abc import ABC
from enum import Enum
from typing import List
from decimal import Decimal
from zmq.asyncio import Context
from loguru import logger
from strategy_trading.easy_strategy.data_type import DatafeedType, Side
from strategy_trading.easy_strategy.instrument_manager import Instrument, InstrumentManager
from strategy_trading.easy_strategy.datafeed import FullLevelOrderBook, FixedLevelOrderBook, TopOrderBook, Trade, Kline, Ticker, IndexTicker


class DatafeedManager:
    def __init__(self):
        self._instrument_mngr = InstrumentManager()
        self._full_obs = dict()
        self._fixed_obs = dict()
        self._top_obs = dict()
        self._on_full_level_orderbook_cb = None
        self._on_fixed_level_orderbook_cb = None
        self._on_trade_cb = None
        self._on_kline_cb = None
        self._on_ticker_cb = None
        self._on_index_ticker_cb = None
        self._on_tob_cb = None

    def init(self, config: dict):
        self._datafeed_server_uri = config['server']
        self._zmq_ctx = Context.instance()
        self._zmq_socket = self._zmq_ctx.socket(zmq.SUB)
        self._zmq_socket.setsockopt(zmq.RCVHWM, 1000000)
        self._zmq_socket.setsockopt(zmq.LINGER, 500)

    def register_callback(self,
        on_ready=None,
        on_full_level_orderbook=None,
        on_fixed_level_orderbook=None,
        on_top_orderbook=None,
        on_trade=None,
        on_kline=None,
        on_ticker=None,
        on_index_ticker=None):
        self._on_ready_cb = on_ready
        self._on_full_level_orderbook_cb = on_full_level_orderbook
        self._on_fixed_level_orderbook_cb = on_fixed_level_orderbook
        self._on_top_orderbook_cb = on_top_orderbook
        self._on_trade_cb = on_trade
        self._on_kline_cb = on_kline
        self._on_ticker_cb = on_ticker
        self._on_index_ticker_cb = on_index_ticker

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
        if not instrument:
            return

        datafeed_types = []
        if full_level_orderbook:
            datafeed_types.append('{}:0'.format(DatafeedType.FULL_LEVEL_ORDERBOOK.value))
        if fixed_level_orderbook:
            datafeed_types.append('{}:{}'.format(DatafeedType.FIXED_LEVEL_ORDERBOOK.value, fixed_level_orderbook))
        if trade:
            datafeed_types.append(DatafeedType.TRADE.value)
        if kline:
            datafeed_types.append('{}:{}'.format(DatafeedType.KLINE.value, kline))
        if ticker:
            datafeed_types.append(DatafeedType.TICKER.value)
        if index_ticker:
            datafeed_types.append(DatafeedType.INDEX_TICKER.value)

        for datafeed_type in datafeed_types:
            topic = '{}#{}#{}#{}'.format(
                instrument.exchange,
                instrument.symbol_type,
                instrument.exchange_symbol,
                datafeed_type)
            logger.info('[MD][SUBSCRIBE] topic={}', topic)
            self._zmq_socket.setsockopt(zmq.SUBSCRIBE, topic.encode())

    async def unsubscribe(self,
            instrument: Instrument,
            full_level_orderbook: bool = False,
            fixed_level_orderbook: int = None,
            top_orderbook: bool = False,
            trade: bool = False,
            kline: int = None,
            ticker: bool = False,
            index_ticker: bool = False,
            tob: bool = False):
        if not instrument:
            return

        datafeed_types = []
        if full_level_orderbook:
            datafeed_types.append('{}:0'.format(DatafeedType.FULL_LEVEL_ORDERBOOK.value))
        if fixed_level_orderbook:
            datafeed_types.append('{}:{}'.format(DatafeedType.FIXED_LEVEL_ORDERBOOK.value, fixed_level_orderbook))
        if trade:
            datafeed_types.append(DatafeedType.TRADE.value)
        if kline:
            datafeed_types.append('{}:{}'.format(DatafeedType.KLINE.value, kline))
        if ticker:
            datafeed_types.append(DatafeedType.TICKER.value)
        if index_ticker:
            datafeed_types.append(DatafeedType.INDEX_TICKER.value)

        for datafeed_type in datafeed_types:
            topic = '{}#{}#{}#{}'.format(
                instrument.exchange,
                instrument.symbol_type,
                instrument.exchange_symbol,
                datafeed_type)
            logger.info('[MD][UNSUBSCRIBE] topic={}', topic)
            self._zmq_socket.setsockopt(zmq.UNSUBSCRIBE, topic.encode())

    async def run(self):
        callbacks = {
            'ob:0': self._on_full_level_orderbook,
            'ob:5': self._on_fixed_level_orderbook,
            'ob:10': self._on_fixed_level_orderbook,
            'ob:20': self._on_fixed_level_orderbook,
            'ob:25': self._on_fixed_level_orderbook,
            'trade': self._on_trade,
            'kline:60': self._on_kline,
            'kline:3600': self._on_kline,
            'ticker': self._on_ticker,
            'index_ticker': self._on_index_ticker
        }

        self._zmq_socket.connect(self._datafeed_server_uri)
        asyncio.ensure_future(self._on_ready_cb())

        while True:
            _, message = await self._zmq_socket.recv_multipart()
            message = orjson.loads(message)
            print(message)
            data_type = message['DataType'] if not isinstance(message, list) else message[0]['DataType']
            if data_type not in callbacks:
                continue
            callback = callbacks[data_type]
            if callback:
                asyncio.ensure_future(callback(message))

    async def _on_full_level_orderbook(self, message):
        instrument = self._instrument_mngr.get_instrument_by_exchange_symbol(message['Exchange'], message['Symbol'])
        if not instrument:
            return
        ob = self.get_full_orderbook(instrument)
        if message['OrderbookType'] == 'snapshot':
            ob.on_full_snapshot(message['Bids'], message['Asks'], int(message['ExchangeTimestamp']), int(message['ConnectivityTimestamp']), message['Seq'])
        else:
            ob.on_delta_snapshot(message['Bids'], message['Asks'], int(message['ExchangeTimestamp']), int(message['ConnectivityTimestamp']), message['PrevSeq'], message['Seq'])
        if ob.has_been_initialized:
            await self._on_full_level_orderbook_cb(ob)

    async def _on_fixed_level_orderbook(self, message):
        instrument = self._instrument_mngr.get_instrument_by_exchange_symbol(message['Exchange'], message['Symbol'])
        if not instrument:
            return
        level = len(message['Bids'])
        ob = self.get_fixed_orderbook(instrument, level)
        ob.on_full_snapshot(message['Bids'], message['Asks'], int(message['ExchangeTimestamp']), int(message['ConnectivityTimestamp']), message['Seq'])
        await self._on_fixed_level_orderbook_cb(ob)

    async def _on_top_orderbook(self, message):
        pass

    async def _on_trade(self, message):
        trades = []
        for data in message:
            instrument = self._instrument_mngr.get_instrument_by_exchange_symbol(data['Exchange'], data['Symbol'])
            if not instrument:
                return
            trades.append(Trade(
                instrument,
                side=Side.BUY if data['Side'] == '1' else Side.SELL,
                price=Decimal(str(data['Price'])),
                quantity=Decimal(str(data['Quantity'])),
                exchange_timestamp=int(data['ExchangeTimestamp']),
                connectivity_timestamp=int(data['ConnectivityTimestamp']),
                exchange_sequence=int(data['Seq'])))
        await self._on_trade_cb(trades)

    async def _on_kline(self, message):
        instrument = self._instrument_mngr.get_instrument_by_exchange_symbol(message['Exchange'], message['Symbol'])
        if not instrument:
            return

        kline = Kline(
            instrument,
            int(message['DataType'].split(':')[1]),
            open_px=Decimal(str(message['Open'])),
            high_px=Decimal(str(message['High'])),
            low_px=Decimal(str(message['Low'])),
            close_px=Decimal(str(message['Close'])),
            volume=Decimal(str(message['BaseCurrencyAmount'])) if not instrument.is_traded_in_notional else Decimal(str(message['Volume'])),
            amount=Decimal(str(message['QuoteCurrencyAmount'])) if not instrument.is_traded_in_notional else Decimal(str(message['Volume'])) * instrument.multiplier,
            open_timestamp=int(message['OpenTimestamp']),
            close_timestamp=int(message['CloseTimestamp']),
            connectivity_timestamp=int(message['ConnectivityTimestamp']))
        await self._on_kline_cb(kline)

    async def _on_ticker(self, message):
        instrument = self._instrument_mngr.get_instrument_by_exchange_symbol(message['Exchange'], message['Symbol'])
        if not instrument:
            return
        ticker = Ticker(
            instrument,
            last_price=Decimal(message['LastPrice']) if message['LastPrice'] else None,
            last_quantity=Decimal(message['LastQty']) if message['LastQty'] else None,
            best_bid_price=Decimal(message['Bids'][0][0]),
            best_bid_quantity=Decimal(message['Bids'][0][1]),
            best_ask_price=Decimal(message['Asks'][0][0]),
            best_ask_quantity=Decimal(message['Asks'][0][1]),
            exchange_timestamp=int(message['ExchangeTimestamp']),
            connectivity_timestamp=int(message['ReceivedTimestamp']),
            exchange_sequence=message['Seq'] if message['Seq'] else None)
        await self._on_ticker_cb(ticker)

    async def _on_index_ticker(self, message):
        instrument = self._instrument_mngr.get_instrument_by_exchange_symbol(message['Exchange'], message['Symbol'])
        if not instrument:
            return
        index_ticker = IndexTicker(
            instrument,
            last_price=Decimal(message['last_price']),
            exchange_timestamp=int(message['ExchangeTimestamp']),
            connectivity_timestamp=int(message['ConnectivityTimestamp']))
        await self._on_index_ticker_cb(index_ticker)

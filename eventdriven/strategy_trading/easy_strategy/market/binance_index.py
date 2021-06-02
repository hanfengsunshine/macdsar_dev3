import zmq
import time
import orjson
import asyncio
import websockets

from decimal import Decimal
from zmq.asyncio import Context
from loguru import logger
from strategy_trading.easy_strategy.data_type import Side
from strategy_trading.easy_strategy.datafeed import Kline, Trade
from strategy_trading.easy_strategy.market.market_base import MarketBase
from strategy_trading.easy_strategy.instrument_manager import Instrument, InstrumentManager


class BinanceIndexMarket(MarketBase):
    def __init__(self, url):
        super(BinanceIndexMarket, self).__init__()
        self._url = url
        self._id = int(time.time() * 1000000)
        self._subbed_topics = set()
        self._ws = None
        self._instrument_mngr = InstrumentManager()
        self._exchange = 'BINANCE_INDEX'

    async def subscribe(self,
        instrument: Instrument,
        full_level_orderbook: bool = False,
        fixed_level_orderbook: int = None,
        top_orderbook: bool = False,
        trade: bool = False,
        kline: int = None,
        ticker: bool = False,
        index_ticker: bool = False):
        if kline:
            mins = int(kline / 60)
            if mins <= 30:
                topic = '{}@indexPriceKline_{}m'.format(instrument.exchange_symbol.lower(), mins)
                await self._subscribe_topic(topic)

    async def unsubscribe(self,
        instrument: Instrument,
        full_level_orderbook: bool = False,
        fixed_level_orderbook: int = None,
        top_orderbook: bool = False,
        trade: bool = False,
        kline: int = None,
        ticker: bool = False,
        index_ticker: bool = False):
        if kline:
            mins = int(kline / 60)
            if mins <= 30:
                topic = '{}@indexPriceKline_{}m'.format(instrument.exchange_symbol.lower(), mins)
                await self._unsubscribe_topic(topic)

    async def run(self):
        asyncio.ensure_future(self._run_ws())
        asyncio.ensure_future(self._check_datafeed_timeout())

    async def _reconnect(self):
        try:
            await self._ws.close()
        except:
            pass
        self._ws = None
        await asyncio.sleep(1)
        asyncio.ensure_future(self._run_ws())

    async def _run_ws(self):
        try:
            self._ws = await websockets.connect(self._url)
        except Exception:
            await self._reconnect()
            return

        logger.info('binance index market connected')

        try:
            for topic in [topic for topic in self._subbed_topics]:
                await asyncio.sleep(1)
                await self._subscribe_topic(topic)
        except websockets.exceptions.ConnectionClosed as e:
            logger.warning('binance index market disconnectd. error={}', e)
            await self._reconnect()
            return

        while True:
            try:
                message = await self._ws.recv()
            except websockets.exceptions.ConnectionClosed as e:
                logger.warning('binance index market disconnectd. error={}', e)
                await self._reconnect()
                break

            await self._on_ws_message(message)

    async def _subscribe_topic(self, topic):
        self._id += 1
        self._subbed_topics.add(topic)
        request = orjson.dumps({
            'method': 'SUBSCRIBE',
            'params': [ topic ],
            'id': self._id
        }).decode()
        if self._ws:
            logger.info('[binance][index][s] {}', request)
            try:
                await self._ws.send(request)
            except:
                await self._reconnect()

    async def _unsubscribe_topic(self, topic):
        self._id += 1
        self._subbed_topics.discard(topic)
        request = orjson.dumps({
            'method': 'UNSUBSCRIBE',
            'params': [ topic ],
            'id': self._id
        })
        if self._ws:
            logger.info('[binance][index][s] {}', request)
            try:
                await self._ws.send(request)
            except:
                await self._reconnect()

    async def _on_ws_message(self, message: str):
        message = orjson.loads(message)
        if 'stream' not in message:
            logger.info('[binance][index][r] {}', message)
            return
        stream = message['stream']
        if 'indexPriceKline' in stream:
            data = message['data']
            symbol = data['ps']
            instrument = self._instrument_mngr.get_instrument_by_exchange_symbol(self._exchange, symbol)
            if instrument:
                info = data['k']
                kline = Kline(
                    instrument,
                    round((info['T'] - info['t']) / 1000, 0),
                    open_px=Decimal(info['o']),
                    high_px=Decimal(info['h']),
                    low_px=Decimal(info['l']),
                    close_px=Decimal(info['c']),
                    volume=Decimal(info['v']),
                    amount=Decimal(info['q']),
                    open_timestamp=int(info['t']) * 1000,
                    close_timestamp=int(info['T']) * 1000,
                    connectivity_timestamp=int(time.time() * 1000000))
                await self._on_kline(kline)


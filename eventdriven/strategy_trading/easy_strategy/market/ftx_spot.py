import zmq
import time
import arrow
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


class FtxSpotMarket(MarketBase):
    def __init__(self, url):
        super(FtxSpotMarket, self).__init__()
        self._url = url
        self._id = int(time.time() * 1000000)
        self._subbed_topics = set()
        self._ws = None
        self._instrument_mngr = InstrumentManager()
        self._exchange = 'FTX_SPOT'
        self._last_request_timestamp = 0

    async def subscribe(self,
        instrument: Instrument,
        full_level_orderbook: bool = False,
        fixed_level_orderbook: int = None,
        top_orderbook: bool = False,
        trade: bool = False,
        kline: int = None,
        ticker: bool = False,
        index_ticker: bool = False):
        if top_orderbook:
            topic = ('ticker', instrument.exchange_symbol)
            await self._subscribe_topic(topic)
        if trade:
            topic = ('trades', instrument.exchange_symbol)
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
        if top_orderbook:
            topic = ('ticker', instrument.exchange_symbol)
            await self._unsubscribe_topic(topic)
        if trade:
            topic = ('trades', instrument.exchange_symbol)
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

        logger.info('ftx spot market connected')

        try:
            for topic in [topic for topic in self._subbed_topics]:
                await asyncio.sleep(1)
                await self._subscribe_topic(topic)
        except websockets.exceptions.ConnectionClosed as e:
            logger.warning('ftx spot market disconnectd. error={}', e)
            await self._reconnect()
            return

        while True:
            try:
                message = await self._ws.recv()
            except websockets.exceptions.ConnectionClosed as e:
                logger.warning('ftx spot market disconnectd. error={}', e)
                await self._reconnect()
                break

            await self._on_ws_message(message)

    async def _subscribe_topic(self, topic):
        self._subbed_topics.add(topic)
        request = orjson.dumps({
            'op': 'subscribe',
            'channel': topic[0],
            'market': topic[1]
        }).decode()
        if self._ws:
            logger.info('[ftx][spot][s] {}', request)
            try:
                await self._ws.send(request)
            except:
                await self._reconnect()

    async def _unsubscribe_topic(self, topic):
        self._subbed_topics.discard(topic)
        request = orjson.dumps({
            'op': 'unsubscribe',
            'channel': topic[0],
            'market': topic[1]
        }).decode()
        if self._ws:
            logger.info('[ftx][spot][s] {}', request)
            try:
                await self._ws.send(request)
            except:
                await self._reconnect()

    async def _on_ws_message(self, message: str):
        message = orjson.loads(message)
        if 'channel' not in message:
            logger.info('[ftx][spot][r] {}', message)
            return
        channel = message['channel']
        if 'ticker' == channel and message['type'] == 'update':
            data = message['data']
            symbol = message['market']
            instrument = self._instrument_mngr.get_instrument_by_exchange_symbol(self._exchange, symbol)
            if instrument:
                top_ob = self.get_top_orderbook(instrument)
                top_ob.on_top_snapshot(
                    best_bid=(Decimal(str(data['bid'])), Decimal(str(data['bidSize']))),
                    best_ask=(Decimal(str(data['ask'])), Decimal(str(data['askSize']))),
                    exchange_timestamp=data['time'] * 1000000,
                    connectivity_timestamp=int(time.time() * 1000000),
                    exchange_sequence=data['time'] * 1000000
                )
                await self._on_top_ob(top_ob)
        elif 'trades' == channel and message['type'] == 'update':
            symbol = message['market']
            instrument = self._instrument_mngr.get_instrument_by_exchange_symbol(self._exchange, symbol)
            if instrument:
                trades = []
                for data in message['data']:
                    dateinfo = arrow.get(data['time'], tzinfo='UTC')
                    timestamp = dateinfo.timestamp * 1000000 + dateinfo.microsecond
                    trades.append(Trade(
                        instrument=instrument,
                        side=Side.BUY if data['side'] == 'buy' else Side.SELL,
                        price=Decimal(str(data['price'])),
                        quantity=Decimal(str(data['size'])),
                        exchange_timestamp=timestamp,
                        connectivity_timestamp=int(time.time() * 1000000)
                    ))
                await self._on_trade(trades)

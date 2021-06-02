import zmq
import time
import zlib
import arrow
import orjson
import asyncio
import websockets

from decimal import Decimal
from zmq.asyncio import Context
from loguru import logger
from strategy_trading.easy_strategy.datafeed import Side, Kline, Trade
from strategy_trading.easy_strategy.market.market_base import MarketBase
from strategy_trading.easy_strategy.instrument_manager import Instrument, InstrumentManager


class OkexSwapMarket(MarketBase):
    def __init__(self, url):
        super(OkexSwapMarket, self).__init__()
        self._url = url
        self._id = int(time.time() * 1000000)
        self._subbed_topics = set()
        self._ws = None
        self._instrument_mngr = InstrumentManager()
        self._exchange = 'OKEX_SWAP'

    async def subscribe(self,
        instrument: Instrument,
        full_level_orderbook: bool = False,
        fixed_level_orderbook: int = None,
        top_orderbook: bool = False,
        trade: bool = False,
        kline: int = None,
        ticker: bool = False,
        index_ticker: bool = False):
        if fixed_level_orderbook and fixed_level_orderbook in [5]:
            topic = (instrument.exchange_symbol, 'books5')
            await self._subscribe_topic(topic)
        if top_orderbook:
            topic = (instrument.exchange_symbol, 'tickers')
            await self._subscribe_topic(topic)
        if trade:
            topic = (instrument.exchange_symbol, 'trades')
            await self._subscribe_topic(topic)
        if kline:
            mins = int(kline / 60)
            if mins < 60:
                topic = (instrument.exchange_symbol, 'candle{}m'.format(mins))
                await self._subscribe_topic(topic)
            elif mins >= 60:
                hours = int(mins / 60)
                topic = (instrument.exchange_symbol, 'candle{}H'.format(hours))
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
        unsub_topics = []
        if fixed_level_orderbook and fixed_level_orderbook in [5]:
            topic = (instrument.exchange_symbol, 'books5')
            unsub_topics.append(topic)
        if top_orderbook:
            topic = (instrument.exchange_symbol, 'tickers')
            unsub_topics.append(topic)
        if trade:
            topic = (instrument.exchange_symbol, 'trades')
            unsub_topics.append(topic)
        if kline:
            mins = int(kline / 60)
            if mins < 60:
                topic = (instrument.exchange_symbol, 'candle{}m'.format(mins))
                unsub_topics.append(topic)
            elif mins >= 60:
                hours = int(mins / 60)
                topic = (instrument.exchange_symbol, 'candle{}H'.format(hours))
                unsub_topics.append(topic)

        for topic in unsub_topics:
            if topic in self._subbed_topics:
                self._subbed_topics.discard(topic)
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

        logger.info('okex swap market connected')

        try:
            for topic in [topic for topic in self._subbed_topics]:
                await asyncio.sleep(1)
                await self._subscribe_topic(topic)
        except websockets.exceptions.ConnectionClosed as e:
            logger.warning('okex swap market disconnectd. error={}', self._url, e)
            await self._reconnect()
            return

        await self._ws.send('ping')

        while True:
            try:
                message = await self._ws.recv()
            except websockets.exceptions.ConnectionClosed as e:
                logger.warning('okex swap market disconnectd. error={}', self._url, e)
                await self._reconnect()
                break

            if message == 'pong':
                asyncio.ensure_future(self._ping())
            else:
                await self._on_ws_message(message)

    async def _ping(self):
        await asyncio.sleep(10)
        try:
            await self._ws.send('ping')
        except:
            await self._ws.close()

    async def _subscribe_topic(self, topic):
        self._id += 1
        self._subbed_topics.add(topic)
        request = orjson.dumps({
            'op': 'subscribe',
            'args': [{
                'instId': topic[0],
                'channel': topic[1]
            }]
        }).decode()
        if self._ws:
            logger.info('[okex][swap][s] {}', request)
            try:
                await self._ws.send(request)
            except:
                await self._reconnect()

    async def _unsubscribe_topic(self, topic):
        self._id += 1
        self._subbed_topics.discard(topic)
        request = orjson.dumps({
            'op': 'unsubscribe',
            'args': [{
                'instId': topic[0],
                'channel': topic[1]
            }]
        })
        if self._ws:
            logger.info('[okex][swap][s] {}', request)
            try:
                await self._ws.send(request)
            except:
                await self._reconnect()

    async def _on_ws_message(self, message: str):
        message = orjson.loads(message.encode())
        if 'event' in message:
            logger.info('[okex][swap][r] {}', message)
            return
        channel = message['arg']['channel']
        if 'books5' in channel:
            for data in message['data']:
                exchange_symbol = data['instId']
                instrument = self._instrument_mngr.get_instrument_by_exchange_symbol(self._exchange, exchange_symbol)
                if instrument:
                    fixed_ob = self.get_fixed_orderbook(instrument, 5)
                    fixed_ob.on_full_snapshot(data['bids'], data['asks'], int(data['ts']) * 1000, int(time.time() * 1000000), int(data['ts']))
                    await self._on_fixed_ob(fixed_ob)
        elif 'trades' in channel:
            instrument = self._instrument_mngr.get_instrument_by_exchange_symbol(self._exchange, message['arg']['instId'])
            if not instrument:
                return
            trades = []
            for data in message['data']:
                trades.append(Trade(
                    instrument=instrument,
                    side=Side.BUY if data['side'] == 'buy' else Side.SELL,
                    price=Decimal(data['px']).quantize(instrument.price_tick),
                    quantity=Decimal(data['sz']).quantize(instrument.lot_size),
                    exchange_timestamp=int(data['ts']) * 1000,
                    connectivity_timestamp=int(time.time() * 1000000),
                    exchange_sequence=int(data['tradeId'])
                ))
            await self._on_trade(trades)
        elif 'tickers' in channel:
            data = message['data'][0]
            exchange_symbol = data['instId']
            instrument = self._instrument_mngr.get_instrument_by_exchange_symbol(self._exchange, exchange_symbol)
            if instrument:
                ob = self.get_top_orderbook(instrument)
                ob.on_top_snapshot(
                    best_bid=(Decimal(data['bidPx']), Decimal(data['bidSz'])),
                    best_ask=(Decimal(data['askPx']), Decimal(data['askSz'])),
                    exchange_timestamp=int(data['ts']) * 1000,
                    connectivity_timestamp=int(time.time() * 1000000),
                    exchange_sequence=int(data['ts'])
                )
                await self._on_top_ob(ob)
        elif 'candle' in channel:
            data = message['data'][0]
            symbol = message['arg']['instId']
            interval_size, interval_scale = int(channel.split('candle')[-1][0:-1]), channel.split('candle')[-1][-1]
            instrument = self._instrument_mngr.get_instrument_by_exchange_symbol(self._exchange, symbol)
            if instrument:
                if interval_scale == 'm':
                    interval = interval_size * 60 * 1000000
                elif interval_scale == 'H':
                    interval = interval_size * 3600 * 1000000
                kline = Kline(
                    instrument,
                    interval,
                    open_px=Decimal(data[1]),
                    high_px=Decimal(data[2]),
                    low_px=Decimal(data[3]),
                    close_px=Decimal(data[4]),
                    volume=Decimal(data[5]),
                    amount=Decimal(data[6]),
                    open_timestamp=int(data[0]) * 1000,
                    close_timestamp=int(data[0]) + interval,
                    connectivity_timestamp=int(time.time() * 1000000))
                await self._on_kline(kline)

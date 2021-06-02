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


class BinanceLinearSwapMarket(MarketBase):
    def __init__(self, url):
        super(BinanceLinearSwapMarket, self).__init__()
        self._url = url
        self._id = int(time.time() * 1000000)
        self._subbed_topics = set()
        self._ws = None
        self._instrument_mngr = InstrumentManager()
        self._exchange = 'BINANCE_SWAP_USDT'
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
        if fixed_level_orderbook and fixed_level_orderbook in [5, 10, 20]:
            topic = '{}@depth{}@100ms'.format(instrument.exchange_symbol.lower(), fixed_level_orderbook)
            await self._subscribe_topic(topic)
        if top_orderbook:
            topic = '{}@bookTicker'.format(instrument.exchange_symbol.lower())
            await self._subscribe_topic(topic)
        if trade:
            topic = '{}@aggTrade'.format(instrument.exchange_symbol.lower())
            await self._subscribe_topic(topic)
        if kline:
            mins = int(kline / 60)
            if mins <= 30:
                topic = '{}@kline_{}m'.format(instrument.exchange_symbol.lower(), mins)
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
        if fixed_level_orderbook and fixed_level_orderbook in [5, 10, 20]:
            topic = '{}@depth{}@100ms'.format(instrument.exchange_symbol.lower(), fixed_level_orderbook)
            await self._unsubscribe_topic(topic)
        if top_orderbook:
            topic = '{}@bookTicker'.format(instrument.exchange_symbol.lower())
            await self._unsubscribe_topic(topic)
        if trade:
            topic = '{}@aggTrade'.format(instrument.exchange_symbol.lower())
            await self._unsubscribe_topic(topic)
        if kline:
            mins = int(kline / 60)
            if mins <= 30:
                topic = '{}@kline_{}m'.format(instrument.exchange_symbol.lower(), mins)
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

        logger.info('binance linear swap market connected')

        try:
            for topic in [topic for topic in self._subbed_topics]:
                await asyncio.sleep(1)
                await self._subscribe_topic(topic)
        except websockets.exceptions.ConnectionClosed as e:
            logger.warning('binance linear swap market disconnectd. error={}', e)
            await self._reconnect()
            return

        while True:
            try:
                message = await self._ws.recv()
            except websockets.exceptions.ConnectionClosed as e:
                logger.warning('binance linear swap market disconnectd. error={}', e)
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
            logger.info('[binance][linear-swap][s] {}', request)
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
            logger.info('[binance][linear-swap][s] {}', request)
            try:
                await self._ws.send(request)
            except:
                await self._reconnect()

    async def _on_ws_message(self, message: str):
        message = orjson.loads(message)
        if 'stream' not in message:
            logger.info('[binance][linear-swap][r] {}', message)
            return
        stream = message['stream']
        if 'bookTicker' in stream:
            data = message['data']
            symbol = data['s']
            instrument = self._instrument_mngr.get_instrument_by_exchange_symbol(self._exchange, symbol)
            if instrument:
                top_ob = self.get_top_orderbook(instrument)
                top_ob.on_top_snapshot(
                    best_bid=(Decimal(data['b']), Decimal(data['B'])),
                    best_ask=(Decimal(data['a']), Decimal(data['A'])),
                    exchange_timestamp=data['E'] * 1000,
                    connectivity_timestamp=int(time.time() * 1000000),
                    exchange_sequence=data['u']
                )
                await self._on_top_ob(top_ob)
        elif 'depth' in stream:
            data = message['data']
            if 'depth@' not in stream:
                level = len(data['b'])
                symbol = stream.split('@')[0].upper()
                instrument = self._instrument_mngr.get_instrument_by_exchange_symbol(self._exchange, symbol)
                if instrument:
                    fixed_ob = self.get_fixed_orderbook(instrument, level)
                    fixed_ob.on_full_snapshot(data['b'], data['a'], data['E'] * 1000, int(time.time() * 1000000), data['u'])
                    await self._on_fixed_ob(fixed_ob)
        elif 'aggTrade' in stream:
            data = message['data']
            symbol = data['s']
            instrument = self._instrument_mngr.get_instrument_by_exchange_symbol(self._exchange, symbol)
            if instrument:
                trades = []
                trades.append(Trade(
                    instrument=instrument,
                    side=Side.BUY if not data['m'] else Side.SELL,
                    price=Decimal(data['p']),
                    quantity=Decimal(data['q']),
                    exchange_timestamp=data['E'] * 1000,
                    connectivity_timestamp=int(time.time() * 1000000)
                ))
                await self._on_trade(trades)
        elif 'kline' in stream:
            data = message['data']
            symbol = data['s']
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


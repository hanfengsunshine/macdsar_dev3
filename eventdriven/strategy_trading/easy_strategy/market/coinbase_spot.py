import zmq
import time
import gzip
import arrow
import orjson
import asyncio
import websockets

from decimal import Decimal
from zmq.asyncio import Context
from loguru import logger
from strategy_trading.easy_strategy.data_type import Side
from strategy_trading.easy_strategy.datafeed import Trade, Kline
from strategy_trading.easy_strategy.market.market_base import MarketBase
from strategy_trading.easy_strategy.instrument_manager import Instrument, InstrumentManager


class CoinbaseSpotMarket(MarketBase):
    def __init__(self, url):
        super(CoinbaseSpotMarket, self).__init__()
        self._url = url
        self._id = int(time.time() * 1000000)
        self._subbed_topics = set()
        self._ws = None
        self._instrument_mngr = InstrumentManager()
        self._exchange = 'COINBASEPRO_SPOT'
        self._heartbeat = 0

    async def subscribe(self,
        instrument: Instrument,
        full_level_orderbook: bool = False,
        fixed_level_orderbook: int = None,
        top_orderbook: bool = False,
        trade: bool = False,
        kline: int = None,
        ticker: bool = False,
        index_ticker: bool = False):
        if full_level_orderbook:
            topic = (instrument, 'level2')
            self._subbed_topics.add(topic)
            await self._subscribe_topic(topic)
        if trade:
            topic = (instrument, 'ticker')
            self._subbed_topics.add(topic)
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
        if full_level_orderbook:
            topic = (instrument, 'level2')
            unsub_topics.append(topic)
        if trade:
            topic = (instrument, 'ticker')
            unsub_topics.append(topic)

        for topic in unsub_topics:
            if topic in self._subbed_topics:
                self._subbed_topics.discard(topic)
                await self._unsubscribe_topic(topic)

    async def run(self):
        asyncio.ensure_future(self._run_ws())
        asyncio.ensure_future(self._check_heartbeat())
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
        self._heartbeat = 0

        try:
            self._ws = await websockets.connect(self._url)
        except Exception:
            asyncio.ensure_future(self.run())
            return

        logger.info('coinbasepro spot market connected')

        try:
            for topic in [topic for topic in self._subbed_topics]:
                await asyncio.sleep(1)
                await self._subscribe_topic(topic)
        except websockets.exceptions.ConnectionClosed as e:
            logger.warning('coinbasepro spot market disconnectd. error={}', e)
            await self._reconnect()
            return

        while True:
            try:
                message = await self._ws.recv()
            except websockets.exceptions.ConnectionClosed as e:
                logger.warning('coinbasepro spot market disconnectd. error={}', e)
                await self._reconnect()
                break

            await self._on_ws_message(message)

    async def _check_heartbeat(self):
        await asyncio.sleep(self._timeout_sec * 2)
        current_sec = int(time.time())
        if self._heartbeat != 0 and current_sec - self._heartbeat > self._timeout_sec:
            self._heartbeat = 0
            logger.warning('[coinbasepro][spot] heartbeat lost')
            await self._ws.close()
        asyncio.ensure_future(self._check_heartbeat())

    async def _subscribe_topic(self, topic):
        self._id += 1
        self._subbed_topics.add(topic)
        request = orjson.dumps({
            'type': 'subscribe',
            'product_ids': [
                topic[0].exchange_symbol
            ],
            'channels': [
                topic[1]
            ]
        }).decode()
        if self._ws:
            logger.info('[coinbasepro][spot][s] {}', request)
            try:
                await self._ws.send(request)
            except:
                await self._reconnect()

    async def _unsubscribe_topic(self, topic):
        self._id += 1
        self._subbed_topics.discard(topic)
        request = orjson.dumps({
            'type': 'unsubscribe',
            'product_ids': [
                topic[0].exchange_symbol
            ],
            'channels': [
                topic[1]
            ]
        }).decode()
        if self._ws:
            logger.info('[coinbasepro][spot][s] {}', request)
            try:
                await self._ws.send(request)
            except:
                await self._reconnect()

    async def _on_ws_message(self, message: str):
        message = orjson.loads(message)
        msg_type = message['type']
        if 'snapshot' in msg_type:
            symbol = message['product_id']
            instrument = self._instrument_mngr.get_instrument_by_exchange_symbol(self._exchange, symbol)
            if not instrument:
                return
            # dateinfo = arrow.get(message['time'], tzinfo='UTC')
            # timestamp = dateinfo.timestamp * 1000000
            ob = self.get_full_orderbook(instrument)
            connectivity_timestamp = int(time.time() * 1000000)
            ob.on_full_snapshot(message['bids'], message['asks'], connectivity_timestamp, connectivity_timestamp)
            await self._on_full_ob(ob)
        elif 'l2update' in msg_type:
            symbol = message['product_id']
            instrument = self._instrument_mngr.get_instrument_by_exchange_symbol(self._exchange, symbol)
            if not instrument:
                return
            dateinfo = arrow.get(message['time'], tzinfo='UTC')
            timestamp = dateinfo.timestamp * 1000000
            ob = self.get_full_orderbook(instrument)
            bids = []
            asks = []
            for change in message['changes']:
                if change[0] == 'sell':
                    asks.append([change[1], change[2]])
                else:
                    bids.append([change[1], change[2]])
            connectivity_timestamp = int(time.time() * 1000000)
            ob.on_delta_snapshot(bids, asks, timestamp, connectivity_timestamp)
            await self._on_full_ob(ob)
        elif 'ticker' in msg_type:
            symbol = message['product_id']
            instrument = self._instrument_mngr.get_instrument_by_exchange_symbol(self._exchange, symbol)
            if not instrument:
                return
            dateinfo = arrow.get(message['time'], tzinfo='UTC')
            timestamp = dateinfo.timestamp * 1000000
            trades = []
            trades.append(Trade(
                instrument=instrument,
                side=Side.BUY if message['side'] == 'buy' else Side.SELL,
                price=Decimal(str(message['price'])).quantize(instrument.price_tick),
                quantity=Decimal(str(message['last_size'])).quantize(instrument.lot_size),
                exchange_timestamp=timestamp,
                connectivity_timestamp=int(time.time() * 1000000),
                exchange_sequence=message['sequence']
            ))
            await self._on_trade(trades)
        elif 'subscriptions' in msg_type:
            logger.info('[coinbasepro][spot][r] {}', message)

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


class HuobiSpotMarket(MarketBase):
    def __init__(self, url):
        super(HuobiSpotMarket, self).__init__()
        self._url = url
        self._id = int(time.time() * 1000000)
        self._subbed_topics = set()
        self._ws = None
        self._instrument_mngr = InstrumentManager()
        self._exchange = 'HUOBI_SPOT'
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
        if fixed_level_orderbook and fixed_level_orderbook in [5, 10, 20]:
            topic = 'market.{}.mbp.refresh.{}'.format(instrument.exchange_symbol.lower(), fixed_level_orderbook)
            self._subbed_topics.add(topic)
            await self._subscribe_topic(topic)
        if top_orderbook:
            topic = 'market.{}.bbo'.format(instrument.exchange_symbol.lower())
            self._subbed_topics.add(topic)
            await self._subscribe_topic(topic)
        if trade:
            topic = 'market.{}.trade.detail'.format(instrument.exchange_symbol)
            self._subbed_topics.add(topic)
            await self._subscribe_topic(topic)
        if kline:
            mins = int(kline / 60)
            if mins <= 60:
                topic = 'market.{}.kline.{}min'.format(instrument.exchange_symbol.lower(), mins)
                self._subbed_topics.add(topic)
                await self._subscribe_topic(topic)
            elif mins > 60:
                hours = int(mins / 60)
                topic = 'market.{}.kline.{}hour'.format(instrument.exchange_symbol.lower(), hours)
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
        if fixed_level_orderbook and fixed_level_orderbook in [5, 10, 20]:
            unsub_topics.append('market.{}.mbp.refresh.{}'.format(instrument.exchange_symbol.lower(), fixed_level_orderbook))
        if top_orderbook:
            unsub_topics.append('market.{}.bbo'.format(instrument.exchange.lower()))
        if trade:
            unsub_topics.append('market.{}.trade.detail'.format(instrument.exchange_symbol))
        if kline:
            mins = int(kline / 60)
            if mins <= 60:
                unsub_topics.append('market.{}.kline.{}min'.format(instrument.exchange_symbol.lower(), mins))
            elif mins > 60:
                hours = int(mins / 60)
                unsub_topics.append('market.{}.kline.{}hour'.format(instrument.exchange_symbol.lower(), hours))

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

        logger.info('huobi spot market connected')

        try:
            for topic in [topic for topic in self._subbed_topics]:
                await asyncio.sleep(1)
                await self._subscribe_topic(topic)
        except websockets.exceptions.ConnectionClosed as e:
            logger.warning('huobi spot market disconnectd. error={}', e)
            await self._reconnect()
            return

        while True:
            try:
                message = await self._ws.recv()
            except websockets.exceptions.ConnectionClosed as e:
                logger.warning('huobi spot market disconnectd. error={}', e)
                await self._reconnect()
                break

            message = gzip.decompress(message).decode()
            await self._on_ws_message(message)

    async def _check_heartbeat(self):
        await asyncio.sleep(self._timeout_sec * 2)
        current_sec = int(time.time())
        if self._heartbeat != 0 and current_sec - self._heartbeat > self._timeout_sec:
            self._heartbeat = 0
            logger.warning('[huobi][spot] heartbeat lost')
            await self._ws.close()
        asyncio.ensure_future(self._check_heartbeat())

    async def _subscribe_topic(self, topic):
        self._id += 1
        self._subbed_topics.add(topic)
        request = orjson.dumps({
            'sub': topic,
            'id': str(self._id)
        }).decode()
        if self._ws:
            logger.info('[huobi][spot][s] {}', request)
            try:
                await self._ws.send(request)
            except:
                await self._reconnect()

    async def _unsubscribe_topic(self, topic):
        self._id += 1
        self._subbed_topics.discard(topic)
        request = orjson.dumps({
            'unsub': topic,
            'id': str(self._id)
        }).decode()
        if self._ws:
            logger.info('[huobi][spot][s] {}', request)
            try:
                await self._ws.send(request)
            except:
                await self._reconnect()

    async def _on_ws_message(self, message: str):
        message = orjson.loads(message)
        if 'ch' not in message:
            if 'ping' in message:
                await self._ws.send(orjson.dumps({
                    'pong': message['ping']
                }).decode())
                self._heartbeat = message['ping']
            else:
                logger.info('[huobi][spot][r] {}', message)
            return
        ch = message['ch']
        if 'bbo' in ch:
            data = message['tick']
            symbol = data['symbol']
            exchange_timestamp = message['ts'] * 1000
            instrument = self._instrument_mngr.get_instrument_by_exchange_symbol(self._exchange, symbol)
            if not instrument:
                return
            ob = self.get_top_orderbook(instrument)
            ob.on_top_snapshot(
                best_bid=(Decimal(str(data['bid'])), Decimal(str(data['bidSize']))),
                best_ask=(Decimal(str(data['ask'])), Decimal(str(data['askSize']))),
                exchange_timestamp=exchange_timestamp,
                connectivity_timestamp=int(time.time() * 1000000),
                exchange_sequence=data['seqId']
            )
            await self._on_top_ob(ob)
        elif 'mbp' in ch:
            symbol = ch.split('market.')[-1].split('.mbp')[0]
            instrument = self._instrument_mngr.get_instrument_by_exchange_symbol(self._exchange, symbol)
            if not instrument:
                return
            exchange_timestamp = message['ts'] * 1000
            data = message['tick']
            if 'refresh' in ch:
                level = len(data['bids'])
                ob = self.get_fixed_orderbook(instrument, level)
                ob.on_full_snapshot(data['bids'], data['asks'], exchange_timestamp, int(time.time() * 1000000), exchange_sequence=data['seqNum'])
                await self._on_fixed_ob(ob)
        elif 'trade' in ch:
            symbol = ch.split('market.')[-1].split('.trade')[0]
            instrument = self._instrument_mngr.get_instrument_by_exchange_symbol(self._exchange, symbol)
            if not instrument:
                return
            data = message['tick']
            trades = []
            for info in data['data'][::-1]:
                trades.append(Trade(
                    instrument=instrument,
                    side=Side.BUY if info['direction'] == 'buy' else Side.SELL,
                    price=Decimal(str(info['price'])).quantize(instrument.price_tick),
                    quantity=Decimal(str(info['amount'])).quantize(instrument.lot_size),
                    exchange_timestamp=info['ts'] * 1000,
                    connectivity_timestamp=int(time.time() * 1000000),
                    exchange_sequence=info['tradeId']
                ))
            await self._on_trade(trades)
        elif 'kline' in ch:
            symbol = ch.split('market.')[-1].split('.kline')[0]
            interval = ch.split('market.')[-1].split('.kline.')[-1]
            instrument = self._instrument_mngr.get_instrument_by_exchange_symbol(self._exchange, symbol)
            if not instrument:
                return
            data = message['tick']
            if 'min' in interval:
                interval = int(interval.split('min')[0]) * 60
            elif 'hour' in interval:
                interval = int(interval.split('hour')[0]) * 3600
            else:
                return
            ts = data['id'] * 1000000
            kline = Kline(
                instrument,
                interval,
                open_px=Decimal(str(data['open'])),
                high_px=Decimal(str(data['high'])),
                low_px=Decimal(str(data['low'])),
                close_px=Decimal(str(data['close'])),
                volume=Decimal(str(data['vol'])),
                amount=Decimal(str(data['amount'])),
                open_timestamp=ts,
                close_timestamp=ts + interval * 1000000,
                connectivity_timestamp=int(time.time() * 1000000))
            await self._on_kline(kline)

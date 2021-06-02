import orjson
import asyncio
import websockets

from loguru import logger
from decimal import Decimal
from collections import defaultdict

from strategy_trading.systemConfig import get_balance_port
from strategy_trading.easy_strategy.data_type import Side, PositionEffect
from strategy_trading.easy_strategy.order import Order
from strategy_trading.easy_strategy.instrument_manager import Instrument


class InventoryRouter:
    def __init__(self, exchange_name, account, exchanges):
        self._exchange_name = exchange_name
        self._account = account
        self._exchanges = exchanges
        self._ws = None
        self._callbacks = dict()

    def register_callback(self, exchange, callback):
        self._callbacks[exchange] = callback

    async def _run_ws(self, endpoint):
        ws = await websockets.connect(endpoint)
        logger.info('inventory router connected. account={}', self._account)
        self._ws = ws
        asyncio.ensure_future(self._process_ws(endpoint, ws))

    async def _process_ws(self, endpoint, ws=None):
        if not ws:
            ws = await websockets.connect(endpoint)
            logger.info('inventory router connected. account={}', self._account)
            self._ws = ws

        await self._ws.send(orjson.dumps({
            'topics': ['{}.{}'.format(exchange, self._account) for exchange in self._exchanges]
        }).decode())

        while True:
            try:
                message = await self._ws.recv()
                await self._on_message(message)
            except websockets.ConnectionClosedError:
                logger.warning('inventory router disconnected. account={}', self._account)
                self._ws = None
                await asyncio.sleep(1)
                asyncio.ensure_future(self._process_ws(endpoint))
                break

    async def _on_message(self, message):
        # logger.info('[IV][RECV] {}', message)

        message = orjson.loads(message)
        exchange = message['topic'].split('.')[0]
        self._callbacks[exchange](exchange, message)

    async def run(self):
        balance_port = get_balance_port(self._exchange_name, self._account)
        balance_endpoint = 'ws://127.0.0.1:{}'.format(balance_port)
        asyncio.ensure_future(self._run_ws(balance_endpoint))

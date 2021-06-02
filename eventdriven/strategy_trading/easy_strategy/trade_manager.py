import orjson
import asyncio
import websockets
import time

from typing import List
from loguru import logger
from decimal import Decimal
from collections import defaultdict

from strategy_trading.easy_strategy.utils import decimal_to_readable
from strategy_trading.systemConfig import get_oms_port
from strategy_trading.easy_strategy.instrument_manager import InstrumentManager
from strategy_trading.easy_strategy.data_type import Side, OrderType, OrderStatus, PositionEffect
from strategy_trading.easy_strategy.order import Order


class TradeManager:
    def __init__(self):
        self._ws = defaultdict(dict)
        self._orders = dict()

    def init(self, config):
        self._config = config

    def register_callback(self, on_ready, on_order_message):
        self._on_ready = on_ready
        self._on_order_message = on_order_message

    async def place(self, portfolio: str, order: Order, priority: int):
        ws = self._ws[(order.instrument.exchange, order.account)]
        await ws.send(orjson.dumps({
            'message_type': 'create_order',
            'data': self._to_place_json(portfolio, order, priority)
        }).decode())

    async def bulk_place(self, portfolio: str, orders: List[Order], priority: int):
        if not orders:
            return

        group_orders = defaultdict(list)
        for order in orders:
            group_orders[(order.instrument.exchange, order.account)].append(order)

        for key, orders in group_orders.items():
            ws = self._ws[key]
            await ws.send(orjson.dumps({
                'message_type': 'create_order',
                'data': [self._to_place_json(portfolio, order, priority) for order in orders]
            }).decode())

    async def cancel(self, order: Order, priority: int):
        ws = self._ws[(order.instrument.exchange, order.account)]
        await ws.send(orjson.dumps({
            'message_type': 'cancel_order',
            'data': self._to_cancel_json(order, priority)
        }).decode())

    async def bulk_cancel(self, orders: List[Order], priority: int):
        if not orders:
            return

        group_orders = defaultdict(list)
        for order in orders:
            group_orders[(order.instrument.exchange, order.account)].append(order)

        for key, orders in group_orders.items():
            ws = self._ws[key]
            await ws.send(orjson.dumps({
                'message_type': 'cancel_order',
                'data': [self._to_cancel_json(order, priority) for order in orders]
            }).decode())

    def _to_place_json(self, portfolio: str, order: Order, priority: int):
        return {
            'strategy': portfolio,
            'symbol': order.instrument.exchange_symbol,
            'strategy_order_id': order.client_order_id,
            'side': order.side.name,
            'offset': order.position_effect.name if order.position_effect != PositionEffect.NONE else None,
            'order_type': order.order_type.name,
            'price': str(order.price) if order.price else None,
            'size': str(order.quantity),
            'margin_trade': order.margin_trade if order.margin_trade != '' else None,
            'leverage_rate': order.leverage_rate,
            'priority': priority,
            'session_id': order.account,
            'process': 'Default',
            'remark': '',
            'correlation_id': ''
        }

    def _to_cancel_json(self, order: Order, priority: int):
        return {
            'symbol': order.instrument.exchange_symbol,
            'strategy_order_id': order.client_order_id,
            'priority': priority,
            'session_id': order.account,
            'correlation_id': ''
        }

    async def _run_ws(self, exchange, account, endpoint):
        try:
            ws = await websockets.connect(endpoint)
        except:
            ws = None

        self._ws[(exchange, account)] = ws
        if ws:
            logger.info('{}-{} oms connected', exchange, account)
            asyncio.ensure_future(self._process_ws(exchange, account, endpoint, ws))

    async def _process_ws(self, exchange, account, endpoint, ws=None):
        if not ws:
            try:
                ws = await websockets.connect(endpoint)
                logger.info('{}-{} oms connected', exchange, account)
            except:
                ws = None
                return
            self._ws[(exchange, account)] = ws

        while True:
            try:
                message = await ws.recv()
                await self._on_message(message)
            except websockets.ConnectionClosedError:
                logger.warning('{}-{} oms disconnected', exchange, account)
                self._ws[(exchange, account)] = None
                await asyncio.sleep(1)
                asyncio.ensure_future(self._process_ws(exchange, account, endpoint))
                break

    async def _on_message(self, message):
        logger.debug('[TD][RECV] {}', message)
        message = orjson.loads(message)
        asyncio.ensure_future(self._on_order_message(message))

    async def run(self):
        for account, exchanges in self._config.items():
            for exchange in exchanges:
                try:
                    oms_port = get_oms_port(exchange, account)
                except:
                    continue
                self._ws[(exchange, account)] = None
                await self._run_ws(exchange, account, 'ws://127.0.0.1:{}'.format(oms_port))
        asyncio.ensure_future(self._on_ready())

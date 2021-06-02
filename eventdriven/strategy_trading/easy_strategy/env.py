import sys
import zmq
import time
import json
import asyncio

from typing import List
from loguru import logger
from decimal import Decimal
from collections import defaultdict
from zmq.asyncio import Context

from strategy_trading.easy_strategy.i_strategy_api import IStrategyAPI
from strategy_trading.easy_strategy.i_strategy_spi import IStrategySPI
from strategy_trading.easy_strategy.instrument_manager import Instrument, InstrumentManager
from strategy_trading.easy_strategy.datafeed_manager import DatafeedManager
from strategy_trading.easy_strategy.direct_datafeed_manager import DirectDatafeedManager
from strategy_trading.easy_strategy.trade_manager import TradeManager
from strategy_trading.easy_strategy.inventory_router import InventoryRouter
from strategy_trading.easy_strategy.account_inventory_manager import AccountBalanceType, AccountPosition, AccountInventoryManager
from strategy_trading.easy_strategy.portfolio_position_manager import PortfolioPosition, PortfolioPositionManager
from strategy_trading.easy_strategy.data_type import Side
from strategy_trading.easy_strategy.datafeed import FullLevelOrderBook, FixedLevelOrderBook, Trade, Kline, Ticker, IndexTicker
from strategy_trading.easy_strategy.order import Order, OrderStatus


class StrategyEnv():
    def __init__(self, config):
        self._config = config
        self._full_obs = dict()
        self._fixed_obs = dict()
        self._orders = dict()

    def init(self, impl, portfolio='', poll_inventory=False, monitor_address=None, exchange_list=None):
        self._impl = impl
        self._impl.env = self
        self._portfolio = portfolio
        self._poll_inventory = poll_inventory

        self._instrument_mngr = InstrumentManager()
        self._instrument_mngr.init(self._config['instrument'])

        direct_datafeed = self._config['datafeed']['direct'] if 'direct' in self._config['datafeed'] else False
        if not direct_datafeed:
            self._datafeed_mngr = DatafeedManager()
        else:
            self._datafeed_mngr = DirectDatafeedManager()
        if not exchange_list:
            self._datafeed_mngr.init(self._config['datafeed'])
        else:
            listen_datafeed = dict()
            for exchange in exchange_list:
                listen_datafeed[exchange] = self._config['datafeed'][exchange]
            self._datafeed_mngr.init(listen_datafeed)
        self._datafeed_mngr.register_callback(
            self._on_datafeed_ready,
            self._on_full_level_orderbook,
            self._on_fixed_level_orderbook,
            self._on_top_level_orderbook,
            self._on_trade,
            self._on_kline,
            self._on_ticker,
            self._on_index_ticker
        )
        self._datafeed_ready = False
        self._trade_mngr = TradeManager()
        self._trade_mngr.init(self._config['trade'])
        self._trade_mngr.register_callback(
            self._on_trade_ready,
            self._on_order_message
        )
        self._trade_ready = False

        self._position_mngr = PortfolioPositionManager(portfolio)
        self._inventory_mngrs = defaultdict(dict)
        self._inventory_router = dict()
        for account, exchanges in self._config['trade'].items():
            exchange_name = exchanges[0].split('_')[0]
            inventory_router = InventoryRouter(exchange_name, account, exchanges) if self._poll_inventory else None
            for exchange in exchanges:
                inventory_mngr = AccountInventoryManager(exchange, account)
                if inventory_router:
                    inventory_router.register_callback(exchange, inventory_mngr.on_inventory_update)
                self._inventory_mngrs[exchange][account] = inventory_mngr
            self._inventory_router[exchange_name] = inventory_router

        self._monitor_address = monitor_address
        self._socket = None

    def run(self):
        asyncio.ensure_future(self._datafeed_mngr.run())
        asyncio.ensure_future(self._trade_mngr.run())
        for _, router in self._inventory_router.items():
            if router:
                asyncio.ensure_future(router.run())
        asyncio.ensure_future(self._start_monitor())
        asyncio.get_event_loop().run_forever()

    def run_async(self):
        asyncio.ensure_future(self._datafeed_mngr.run())
        asyncio.ensure_future(self._trade_mngr.run())
        asyncio.ensure_future(self._start_monitor())

    def get_instrument(self, exchange: str, global_symbol: str):
        return self._instrument_mngr.get_instrument(exchange, global_symbol)

    def get_balance(self, exchange: str, account: str, currency: str):
        return self._inventory_mngrs[exchange][account].get_currency(currency)

    def get_portfolio_position(self, portfolio: str, instrument: Instrument):
        return self._position_mngr.get_position(instrument)

    def get_account_position(self, account: str, instrument: Instrument):
        return self._inventory_mngrs[instrument.exchange][account].get_position(instrument)

    def get_full_orderbook(self, instrument: Instrument):
        return self._datafeed_mngr.get_full_orderbook(instrument)

    def get_fixed_orderbook(self, instrument: Instrument, level: int):
        return self._datafeed_mngr.get_fixed_orderbook(instrument, level)

    def get_top_orderbook(self, instrument: Instrument):
        return self._datafeed_mngr.get_top_orderbook(instrument)

    def get_live_order(self, client_order_id):
        if client_order_id not in self._orders:
            return None
        order = self._orders[client_order_id]
        return order if not order.is_finished() else None

    def get_live_orders(self):
        return [order for _, order in self._orders.items() if not order.is_finished()]

    async def send_command_response(self, response: str):
        if self._socket:
            await self._socket.send(response.encode())

    async def subscribe_full_orderbook(self, instrument: Instrument):
        await self._datafeed_mngr.subscribe(
            instrument=instrument,
            full_level_orderbook=True
        )

    async def subscribe_fixed_orderbook(self, instruemnt: Instrument, level: int):
        await self._datafeed_mngr.subscribe(
            instrument=instruemnt,
            fixed_level_orderbook=level
        )

    async def subscribe_top_orderbook(self, instrument: Instrument):
        await self._datafeed_mngr.subscribe(
            instrument=instrument,
            top_orderbook=True
        )

    async def subscribe_trade(self, instrument: Instrument):
        await self._datafeed_mngr.subscribe(
            instrument=instrument,
            trade=True
        )

    async def subscribe_kline(self, instrument: Instrument, interval_in_sec: int = 60):
        await self._datafeed_mngr.subscribe(
            instrument=instrument,
            kline=interval_in_sec
        )

    async def subscribe_ticker(self, instrument: Instrument):
        await self._datafeed_mngr.subscribe(
            instrument=instrument,
            ticker=True
        )

    async def place(self, order: Order, priority: int = 0):
        self._position_mngr.on_order_place(order)
        self._inventory_mngrs[order.instrument.exchange][order.account].on_order_place(order)
        self._orders[order.client_order_id] = order
        await self._trade_mngr.place(self._portfolio, order, priority)

    async def bulk_place(self, orders: List[Order], priority: int = 0):
        for order in orders:
            self._position_mngr.on_order_place(order)
            self._inventory_mngrs[order.instrument.exchange][order.account].on_order_place(order)
            self._orders[order.client_order_id] = order
        await self._trade_mngr.bulk_place(self._portfolio, orders, priority)

    async def cancel(self, order: Order, priority: int = 0):
        if not order.is_cancel_timeout():
            return
        order.pending_cancel = True
        order.cancel_timestamp = int(time.time() * 1000000)
        if order.status != OrderStatus.PENDING:
            await self._trade_mngr.cancel(order, priority)

    async def bulk_cancel(self, orders: List[Order], priority: int = 0):
        candidates = []
        for order in orders:
            if not order.is_cancel_timeout():
                continue
            order.pending_cancel = True
            order.cancel_timestamp = int(time.time() * 1000000)
            if order.status != OrderStatus.PENDING:
                candidates.append(order)
        await self._trade_mngr.bulk_cancel(candidates, priority)

    async def _on_datafeed_ready(self):
        self._datafeed_ready = True
        if self._trade_ready:
            self._impl.is_ready.set()
            asyncio.ensure_future(self._impl.start())

    async def _on_trade_ready(self):
        self._trade_ready = True
        if self._datafeed_ready:
            self._impl.is_ready.set()
            asyncio.ensure_future(self._impl.start())

    async def _start_monitor(self):
        if not self._monitor_address:
            return

        self._ctx = Context.instance()
        self._socket = self._ctx.socket(zmq.REP)
        self._socket.bind(self._monitor_address)

        while True:
            message = await self._socket.recv()
            try:
                request = json.loads(message)
                await self._impl.on_command(request)
            except:
                await self._socket.send('something wrong'.encode())

    async def _on_full_level_orderbook(self, ob):
        await self._impl.on_full_level_orderbook(ob)

    async def _on_fixed_level_orderbook(self, ob):
        await self._impl.on_fixed_level_orderbook(ob)

    async def _on_top_level_orderbook(self, ob):
        await self._impl.on_top_orderbook(ob)

    async def _on_trade(self, trades):
        await self._impl.on_trade(trades)

    async def _on_kline(self, kline):
        await self._impl.on_kline(kline)

    async def _on_ticker(self, ticker):
        await self._impl.on_ticker(ticker)

    async def _on_index_ticker(self, index_ticker):
        await self._impl.on_index_ticker(index_ticker)

    async def _on_order_message(self, message):
        msg_type = message['message_type']
        if msg_type == 'order_update':
            data = message['data']
            client_order_id = data['strategy_order_id']
            if client_order_id not in self._orders:
                return
            order = self._orders[client_order_id]

            if 'operation' in data:
                operation = data['operation']
                if operation == 'create_order':
                    if data['state'] == 'closed' and order.status == OrderStatus.PENDING:
                        await self._on_place_reject(data)
                elif operation == 'cancel_order':
                    if data['state'] == 'closed' and order.status != OrderStatus.PENDING:
                        await self._on_order_update(data)
            elif 'state' in data:
                state = data['state']
                filled_quantity = Decimal(data['executed_size'])
                if state == 'open':
                    if order.status == OrderStatus.PENDING:
                        await self._on_place_confirm(data)
                    if filled_quantity > order.filled_quantity:
                        await self._on_order_update(data)
                else:
                    await self._on_order_update(data)

    async def _on_place_confirm(self, message):
        client_order_id = message['strategy_order_id']
        if client_order_id not in self._orders:
            return
        order = self._orders[client_order_id]
        order.status = OrderStatus.CONFIRMED
        await self._impl.on_place_confirm(order)
        if order.pending_cancel:
            await self.cancel(order)

    async def _on_place_reject(self, message):
        client_order_id = message['strategy_order_id']
        if client_order_id not in self._orders:
            return
        order = self._orders[client_order_id]
        order.status = OrderStatus.REJECTED
        order.place_reject_reason = message['place_reject_reason'] if 'place_reject_reason' in message else ''
        if 'close_reason' in message:
            order.closed_reason = message['close_reason']

        self._position_mngr.on_order_reject(order)
        self._inventory_mngrs[order.instrument.exchange][order.account].on_order_reject(order)

        await self._impl.on_place_reject(order)
        self._orders.pop(client_order_id, None)

    async def _on_cancel_reject(self, message):
        client_order_id = message['strategy_order_id']
        if client_order_id not in self._orders:
            return
        order = self._orders[client_order_id]
        order.cancel_reject_reason = message['cancel_reject_reason'] if 'cancel_reject_reason' in message else ''
        await self._impl.on_cancel_reject(order)

    async def _on_order_update(self, message):
        client_order_id = message['strategy_order_id']
        if client_order_id not in self._orders:
            return
        order = self._orders[client_order_id]

        has_trade = False
        has_update = False

        if 'executed_size' in message:
            executed_size = Decimal(message['executed_size'])
            avg_price = Decimal(message['avg_price'])

            last_trade_quantity = executed_size - order.filled_quantity
            if last_trade_quantity > Decimal('0'):
                has_trade = True

                order.last_trade_price = (executed_size * avg_price - order.filled_quantity * order.avg_trade_price) / last_trade_quantity
                order.last_trade_quantity = last_trade_quantity
                order.filled_quantity = executed_size
                order.unfilled_quantity = order.quantity - order.filled_quantity
                order.avg_trade_price = avg_price

                if order.filled_quantity < order.quantity:
                    order.status = OrderStatus.PARTIAL_FILLED
                else:
                    order.status = OrderStatus.FILLED

        if message['state'] == 'closed':
            if not order.is_finished():
                has_update = True
                if order.filled_quantity != order.quantity:
                    order.status = OrderStatus.CANCELED
            if 'close_reason' in message:
                order.closed_reason = message['close_reason']

        if has_trade:
            self._inventory_mngrs[order.instrument.exchange][order.account].on_order_trade(order)
            self._position_mngr.on_order_trade(order)

        if order.is_finished():
            self._inventory_mngrs[order.instrument.exchange][order.account].on_order_finish(order)
            self._position_mngr.on_order_finish(order)

        if has_trade or has_update:
            await self._impl.on_order_update(order)
            order.last_trade_price = Decimal('0')
            order.last_trade_quantity = Decimal('0')

        if order.is_finished():
            self._orders.pop(client_order_id, None)

from strategy_trading.STUtils.wssClient import BaseWebsocketClient
from strategy_trading.StrategyTrading.symbolHelper import SymbolHelper
from strategy_trading.StrategyTrading.order import ClientOrder, Side, State, Offset
from strategy_trading.StrategyTrading.correlationID import CorrelationID
from strategy_trading.StrategyTrading.exchStates import ExchangeState
import logging
import asyncio
from decimal import Decimal, ROUND_UP, ROUND_DOWN
from uuid import uuid4
from datetime import datetime, timedelta
from asyncio import Event
from typing import List


INTERVAL_BETWEEN_TWO_CANCEL = 0.1
INTERVAL_BETWEEN_TWO_CANCEL_CLOSING = 1


class TradingSession():
    def __init__(self, exchange, global_symbol, session_id, trading_connection, symbol_helper: SymbolHelper, callback_when_exec = None, receive_error = False, alias = None):
        self.logger = logging.getLogger("{}|{}|{}{}".format("TS", exchange, global_symbol, "|{}".format(alias) if alias is not None else ""))
        self.alias = alias
        self.exchange = exchange
        self.global_symbol = global_symbol
        self.session_id = session_id
        self.trading_connection = trading_connection
        self._order_update_event = Event()
        self.callback_when_exec = callback_when_exec
        if callback_when_exec is None:
            self.logger.warning("Exec is put in queue which might bring in the inconsistency between order manager and exec manager."
                                "It is suggested to use callback_when_exec")
            self.exec_q = asyncio.Queue()
        else:
            assert callable(callback_when_exec)

        self.active_orders = {}
        self.inactive_orders = {}
        self.cloid_to_orders = {}
        self.receive_error = receive_error
        if receive_error:
            self.error_q = asyncio.Queue()
        if symbol_helper.is_inverse_contract(global_symbol, exchange):
            self.size_is_value = True
        else:
            self.size_is_value = False
        symbol_info = symbol_helper.get_info(global_symbol, exchange)
        self.symbol = symbol_info['symbol']

        self.tick_size = symbol_info['tick_size']
        self.min_order_size = symbol_info['min_order_size']
        self.min_order_value = symbol_info['min_order_size_in_value']
        if self.min_order_value is None:
            self.min_order_value = Decimal("0")
        self.size_incremental = symbol_info['order_size_incremental']
        self.price_precision = symbol_info['price_precision']
        self.interval_between_two_cancel = INTERVAL_BETWEEN_TWO_CANCEL

    async def create_order(self, side: Side, price: Decimal, size: Decimal,
                 order_type:str = 'GTC', correlation_id: CorrelationID = None, remark = None, offset: Offset = None, margin_trade = None, priority = 1):
        # TODO: risk checking + normalize price and size

        size = size.quantize(self.size_incremental, rounding=ROUND_DOWN)
        if side == Side.BUY:
            price = price.quantize(self.tick_size, rounding=ROUND_DOWN)
        elif side == Side.SELL:
            price = price.quantize(self.tick_size, rounding=ROUND_UP)

        if size < self.min_order_size or (not self.size_is_value and price * size < self.min_order_value):
            self.logger.warning("too small order size. drop. side: {}, {:.8f}@{:.8f}".format(side.name, price, size))
            self._order_update_event.set()
            return

        if self.exchange in ['HUOBI_CONTRACT', 'HUOBI_SWAP'] and offset is None:
            self.logger.warning("offset should not be None. drop. side: {}, {:.8f}@{:.8f}".format(side.name, price, size))
            self._order_update_event.set()
            return

        if margin_trade is not None and margin_trade not in ['cross-margin', 'margin']:
            self.logger.warning("margin_trade should be None/cross-margin, not {}".format(margin_trade))
            self._order_update_event.set()
            return 

        order = ClientOrder(exchange = self.exchange,
                            global_symbol = self.global_symbol,
                            symbol = self.symbol,
                            strategy_order_id= str(uuid4()),
                            side = side,
                            price = price,
                            size = size,
                            trading_session=self,
                            order_type = order_type,
                            correlation_id = correlation_id,
                            remark = remark,
                            offset = offset,
                            margin_trade = margin_trade,
                            priority = priority
                            )
        self.active_orders[order.strategy_order_id] = order
        await self.trading_connection._create_order(order)
        self.logger.info("new order: side: {}, {:.10f}@{:.8f} sid: {} corrID:{} rem:{}".format(side.name, price, size, order.strategy_order_id, correlation_id, remark))
        return order

    async def bulk_create_orders(self, order_infos: List[dict], correlation_id: CorrelationID = None):
        # TODO: risk checking + normalize price and size
        orders = []
        for order_info in order_infos:
            size = order_info['size']
            side = order_info['side']
            price = order_info['price']
            order_type = order_info.get('orderType', 'GTC')
            remark = order_info.get('remark', None)
            offset = order_info.get('offset', None)
            margin_trade = order_info.get('marginTrade', None)
            priority = order_info.get('priority', 0)
            size = size.quantize(self.size_incremental, rounding=ROUND_DOWN)
            if side == Side.BUY:
                price = price.quantize(self.tick_size, rounding=ROUND_DOWN)
            elif side == Side.SELL:
                price = price.quantize(self.tick_size, rounding=ROUND_UP)

            if size < self.min_order_size or (not self.size_is_value and price * size < self.min_order_value):
                self.logger.warning("too small order size. drop. side: {}, {:.8f}@{:.8f}".format(side.name, price, size))
                self._order_update_event.set()
                continue

            if self.exchange in ['HUOBI_CONTRACT', 'HUOBI_SWAP'] and offset is None:
                self.logger.warning("offset should not be None. drop. side: {}, {:.8f}@{:.8f}".format(side.name, price, size))
                self._order_update_event.set()
                continue

            if margin_trade is not None and margin_trade not in ['cross-margin', "margin"]:
                self.logger.warning("margin_trade should be None/cross-margin, not {}".format(margin_trade))
                self._order_update_event.set()
                continue

            order = ClientOrder(exchange = self.exchange,
                                global_symbol = self.global_symbol,
                                symbol = self.symbol,
                                strategy_order_id= str(uuid4()),
                                side = side,
                                price = price,
                                size = size,
                                trading_session=self,
                                order_type = order_type,
                                correlation_id = correlation_id,
                                remark = remark,
                                offset = offset,
                                margin_trade = margin_trade,
                                priority = priority
                                )
            orders.append(order)

        if orders:
            for order in orders:
                self.active_orders[order.strategy_order_id] = order
                self.logger.info("new order: side: {}, {:.10f}@{:.8f} sid: {} corrID:{} rem:{} bk".format(order.side.name, order.price, order.size, order.strategy_order_id, correlation_id, order.remark))
            await self.trading_connection._bulk_create_orders(orders)
            return orders

    async def cancel_order(self, strategy_order_id, correlation_id: CorrelationID = None, priority = 0):
        if strategy_order_id in self.active_orders:
            order = self.active_orders[strategy_order_id]
            if order.order_type == 'IOC':
                self.logger.warning("cancel order {} is IOC. ignore cancel".format(strategy_order_id))
                return

            current_timestamp = datetime.now().timestamp()
            if order.last_cancel_timestamp is None or (order.state != State.CLOSING and order.last_cancel_timestamp < current_timestamp - self.interval_between_two_cancel) \
                    or (order.state == State.CLOSING and order.last_cancel_timestamp < current_timestamp - INTERVAL_BETWEEN_TWO_CANCEL_CLOSING):
                order.last_cancel_timestamp = current_timestamp
                await self.trading_connection._cancel_order(order, correlation_id, priority = priority)
                self.logger.info("cancel order {}".format(order.strategy_order_id))
            else:
                self.logger.warning("cancel order {}|{} just sent at {}. wait a moment".format(order.strategy_order_id, order.state, order.last_cancel_timestamp))
        else:
            self.logger.warning("order {} is not active. ignore cancel".format(strategy_order_id))

    async def bulk_cancel_orders(self, strategy_order_ids: List[str], correlation_id: CorrelationID = None, priority = 0):
        to_cancel_orders = []
        current_timestamp = datetime.now().timestamp()
        for strategy_order_id in strategy_order_ids:
            if strategy_order_id in self.active_orders:
                order = self.active_orders[strategy_order_id]
                if order.order_type == 'IOC':
                    self.logger.warning("cancel order {} is IOC. ignore cancel".format(strategy_order_id))
                    return

                if order.last_cancel_timestamp is None or (order.state != State.CLOSING and order.last_cancel_timestamp < current_timestamp - self.interval_between_two_cancel) or \
                        (order.state == State.CLOSING and order.last_cancel_timestamp < current_timestamp - INTERVAL_BETWEEN_TWO_CANCEL):
                    order.last_cancel_timestamp = current_timestamp
                    to_cancel_orders.append(order)
                else:
                    self.logger.warning("cancel order {}|{} just sent at {}. wait a moment".format(order.strategy_order_id, order.state, order.last_cancel_timestamp))
            else:
                self.logger.warning("order {} is not active. ignore cancel".format(strategy_order_id))

        if to_cancel_orders:
            for order in to_cancel_orders:
                self.logger.info("cancel order {} bk".format(order.strategy_order_id))
            await self.trading_connection._bulk_cancel_orders(to_cancel_orders, correlation_id, priority = priority)

    def get_liquidation_order(self, data):
        cloid = data['client_order_id']
        order = self.cloid_to_orders.get(cloid, None)
        if order is None:
            side = Side.BUY if data['side'].lower() == 'buy' else Side.SELL
            price = Decimal(data['price'])
            size = Decimal(data['size'])
            order_type = data['order_type']
            correlation_id = data['last_update_timestamp']
            remark = 'liquidation'
            order = ClientOrder(exchange=self.exchange,
                                global_symbol=self.global_symbol,
                                symbol=self.symbol,
                                strategy_order_id=str(uuid4()),
                                side=side,
                                price=price,
                                size=size,
                                trading_session=self,
                                order_type=order_type,
                                correlation_id=correlation_id,
                                remark=remark
                                )
            self.active_orders[order.strategy_order_id] = order
            self.logger.info(
                "new liquidation order: exchange: {}, symbol: {}, side: {}, {:.10f}@{:.8f} sid: {} corrID:{} rem:{}".format(
                    self.exchange, self.symbol, side.name, price, size, order.strategy_order_id, correlation_id,
                    remark))
            self.cloid_to_orders.update({cloid: order})
        return order

    def get_active_orders(self):
        return self.active_orders

    async def wait_for_order_update_event(self):
        await self._order_update_event.wait()
        self._order_update_event.clear()

    def _get_order_exec(self, order: ClientOrder, new_info: dict) -> dict:
        if 'executed_size' in new_info and 'avg_price' in new_info:
            if new_info['executed_size'] is not None:
                if new_info['executed_size'] > order.executed_size:
                    # calc newly_exec_size, estimated_price
                    new_exec_size = new_info['executed_size'] - order.executed_size
                    if order.executed_size == Decimal("0"):
                        avg_price = new_info['avg_price']
                    else:
                        # check if the symbol
                        if self.size_is_value:
                            avg_price = (new_info['executed_size'] - order.executed_size) / (new_info['executed_size'] / new_info['avg_price'] - order.executed_size / order.avg_price)
                        else:
                            avg_price = (new_info['executed_size'] * new_info['avg_price'] - order.executed_size * order.avg_price) / (new_info['executed_size'] - order.executed_size)
                    return {
                        "price": avg_price,
                        "size": new_exec_size,
                        "side": order.side,
                        "order": order,
                        'offset': order.offset
                    }

    def _update_order_info(self, strategy_order_id, info):
        if strategy_order_id in self.active_orders:
            order = self.active_orders[strategy_order_id]
            order_exec_update = self._get_order_exec(order, info)
            order.update_info(info)
            if order.is_closed():
                self.inactive_orders[strategy_order_id] = self.active_orders.pop(strategy_order_id)
                self.logger.info("order {}@{}-{} is closed".format(strategy_order_id, order.symbol, order.side))
        elif strategy_order_id in self.inactive_orders:
            order = self.inactive_orders[strategy_order_id]
            order_exec_update = self._get_order_exec(order, info)
            order.update_info(info)
            if not order.is_closed():
                self.logger.warning("order {} state becomes from inactive to active".format(strategy_order_id))
                self.active_orders[strategy_order_id] = self.inactive_orders.pop(strategy_order_id)
        else:
            self.logger.warning("cannot find order {} locally".format(strategy_order_id))
            return

        # notify strategy via different methods
        self._order_update_event.set()
        if order_exec_update is not None:
            if self.callback_when_exec is None:
                try:
                    self.exec_q.put_nowait(order_exec_update)
                except asyncio.QueueFull:
                    self.logger.critical("exec queue full, drop exec {}".format(order_exec_update))
            else:
                self.callback_when_exec(**order_exec_update)

    async def get_exec(self):
        if self.callback_when_exec is None:
            exec = await self.exec_q.get()
            return exec

    def _get_order(self, strategy_order_id):
        if strategy_order_id in self.active_orders:
            return self.active_orders[strategy_order_id]
        if strategy_order_id in self.inactive_orders:
            return self.inactive_orders[strategy_order_id]

    async def get_error(self) -> dict:
        if self.receive_error:
            return await self.error_q.get()
        else:
            raise Exception("receive_error is not enabled")


class TradingConnection():
    '''
    Usually this class is used to communicate with trading gateways or market data gateways
    One connection sets up one ws channel to one server
    '''
    def __init__(self, exchange, server, port, strategy, process, symbol_helper: SymbolHelper, receive_error = False, callback_when_exch_state = None):
        self.logger = logging.getLogger("{}-{}".format(self.__class__.__name__, exchange))
        self.exchange = exchange
        self.server = server
        self.port = port
        self.ws = BaseWebsocketClient("http://{}:{}".format(server, port), True,
                                      callback_when_data=self.on_message,
                                      heartbeat=30, retry_seconds=5)
        self.strategy = strategy
        self.process = process

        self.session_next_id = 1
        self.sessions = {}
        self.receive_error = receive_error
        self.symbol_helper = symbol_helper
        self.liq_sessions = {}
        self.callback_when_exch_state = callback_when_exch_state
        if callback_when_exch_state is not None:
            assert callable(callback_when_exch_state)

    def on_message(self, message):
        # TODO parse message, if it's order_update, update the info to the corresponding order
        # if it's error message, put into somewhere the strategy is able to use
        if not isinstance(message, dict):
            self.logger.warning("message {} is not dict. ignore".format(message))
            return

        self.logger.info("receive msg {}".format(message))
        if 'message_type' in message:
            mtype = message['message_type']
            if mtype in ['order_update', 'liquidation']:
                data = message['data']
                if mtype == 'liquidation':
                    try:
                        self.handle_liquidation_order(data)
                    except Exception as e:
                        self.logger.warning(e)
                        return
                session_id = data['session_id']
                if 'executed_size' in data and data['executed_size'] is not None:
                    data['executed_size'] = Decimal(data['executed_size'])
                if 'avg_price' in data and data['avg_price'] is not None:
                    data['avg_price'] = Decimal(data['avg_price'])
                if 'state' in data and data['state'] is not None:
                    try:
                        if data['state'] == '':
                            data['state'] = State.OPEN.value
                        data['state'] = State(data['state'])
                    except Exception as e:
                        self.logger.critical("unrecognized order state {}".format(data['state']))
                        data['state'] = State.CLOSED
                if session_id in self.sessions:
                    self.sessions[session_id]._update_order_info(data['strategy_order_id'], data)
                else:
                    self.logger.warning("receive msg without correct sessionid: {}".format(data))
            elif mtype == 'error':
                strategy_order_id = message['data']['strategy_order_id']
                session_id = message['data']['session_id']

                if session_id not in self.sessions:
                    self.logger.warning("receive msg without correct sessionid: {}".format(message))
                    return

                # find the order
                order = self.sessions[session_id]._get_order(strategy_order_id)
                if order is not None:
                    message['data']['order'] = order
                    try:
                        if self.receive_error:
                            self.sessions[session_id].error_q.put_nowait(message['data'])
                    except asyncio.QueueFull:
                        self.logger.warning("error q is full. drop {}".format(message['data']))
                else:
                    self.logger.warning("cannot find order {}. ignore".format(strategy_order_id))
            elif mtype == 'exchange_update':
                state_str = message['data']['trading_state']
                try:
                    state = ExchangeState(state_str)
                except:
                    state = ExchangeState.UNRECOGNIZED
                    self.logger.warning("fail to parse state: {}".format(state_str))
                if self.callback_when_exch_state:
                    self.callback_when_exch_state(**{"state": state})

    async def init(self, loop):
        await self.ws.connect_to_server()
        asyncio.ensure_future(self.ws.receive_data(), loop = loop)
        asyncio.ensure_future(self.clean_orders(), loop=loop)

    def get_session(self, global_symbol, callback_when_exec = None, alias = None) -> TradingSession:
        self.logger.info("created session {} for {} ".format(self.session_next_id, global_symbol))
        session = TradingSession(self.exchange, global_symbol, self.session_next_id,
                                 self,
                                 receive_error=self.receive_error,
                                 symbol_helper = self.symbol_helper,
                                 callback_when_exec = callback_when_exec,
                                 alias = alias)
        return self.update_session(session)

    def update_session(self, session):
        self.sessions[self.session_next_id] = session
        self.session_next_id += 1
        return session

    def get_liquidation_session(self, global_symbol, callback_when_exec = None, alias = None) -> TradingSession:
        self.logger.info("created liquidation session {} for {} ".format(self.session_next_id, global_symbol))
        session = TradingSession(self.exchange, global_symbol, self.session_next_id,
                                 self,
                                 receive_error=self.receive_error,
                                 symbol_helper = self.symbol_helper,
                                 callback_when_exec = callback_when_exec,
                                 alias = alias)
        info = self.symbol_helper.get_info(global_symbol, self.exchange)
        exchange_symbol = info['symbol']
        self.liq_sessions.update({exchange_symbol.lower(): session})
        return self.update_session(session)

    def _get_create_order_msg(self, order: ClientOrder):
        return {
                    "symbol": order.symbol,
                    "side": order.side.name,
                    "price": str(order.price), #"{:.12f}".format(order.price),
                    "size": str(order.size), # "{:.12f}".format(order.size),
                    "order_type": order.order_type,
                    "strategy_order_id": order.strategy_order_id,
                    "correlation_id": "{}|{}".format(order.correlation_id.msg if order.correlation_id is not None else "None", int(datetime.now().timestamp() * 1000000)),
                    "session_id": order.trading_session.session_id,
                    "strategy": self.strategy,
                    "process": self.process,
                    "remark": order.remark,
                    'offset': order.offset.name if order.offset else None,
                    'margin_trade': order.margin_trade,
                    'priority': order.priority
                }

    async def _create_order(self, order: ClientOrder):
        # send to trading gateway
        await self.ws.send_message([
            {
                'message_type': "create_order",
                "data": self._get_create_order_msg(order)
            }
        ])

    async def _bulk_create_orders(self, orders: List[ClientOrder]):
        order_infos = []
        for order in orders:
            order_infos.append(self._get_create_order_msg(order))

        await self.ws.send_message([
            {
                'message_type': "create_order",
                "data": order_infos
            }
        ])

    def _get_cancel_order_msg(self, order: ClientOrder, correlation_id, priority):
        return {
                    "symbol": order.symbol,
                    "strategy_order_id": order.strategy_order_id,
                    "correlation_id": correlation_id.msg if correlation_id is not None else None,
                    "session_id": order.trading_session.session_id,
                    "priority": priority
                }

    async def _cancel_order(self, order: ClientOrder, correlation_id: CorrelationID = None, priority = 0):
        await self.ws.send_message([
            {
                "message_type": "cancel_order",
                "data": self._get_cancel_order_msg(order, correlation_id, priority)
            }
        ])

    async def _bulk_cancel_orders(self, orders: List[ClientOrder], correlation_id: CorrelationID = None, priority = 0):
        order_infos = []
        for order in orders:
            order_infos.append(self._get_cancel_order_msg(order, correlation_id, priority))
        await self.ws.send_message([
            {
                "message_type": "cancel_order",
                "data": order_infos
            }
        ])

    async def clean_orders(self, interval_seconds = 18 * 60):
        assert interval_seconds >= 360
        while True:
            await asyncio.sleep(interval_seconds)

            for session_id in self.sessions:
                dead_order_ids = []
                for strategy_order_id, order in self.sessions[session_id].inactive_orders.items():
                    if order.last_update_time < datetime.now() - timedelta(seconds=interval_seconds):
                        dead_order_ids.append(strategy_order_id)

                for strategy_order_id in dead_order_ids:
                    self.sessions[session_id].inactive_orders.pop(strategy_order_id)
                    self.logger.info("clean order {}".format(strategy_order_id))

    def handle_liquidation_order(self, data):
        symbol = data['symbol']
        session = self.liq_sessions.get(symbol.lower(), None)
        if session:
            order = session.get_liquidation_order(data)
            data['session_id'] = session.session_id
            data['strategy_order_id'] = order.strategy_order_id
        else:
            raise Exception('No session corresponds to exchange symbol {}'.format(symbol))

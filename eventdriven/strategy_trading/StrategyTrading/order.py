from enum import Enum
from decimal import Decimal
import logging
from datetime import datetime
from strategy_trading.StrategyTrading.correlationID import CorrelationID
import asyncio


class Side(Enum):
    BUY = 'BUY'
    SELL = 'SELL'
    #TODO: add open_long/open_short/close_long/close_short in futures market ?


class State(Enum):
    PENDING = 'pending'
    INTERNAL_CLOSED = 'internal_closed'
    OPEN = 'open'
    CLOSED = 'closed'
    CLOSING = 'closing'


class OrderStatus(Enum):
    PENDING = 'pending'
    CONFIRMED = 'confirmed'
    REJECTED = 'rejected'
    PARTIAL_FILLED = 'partial_filled'
    FILLD = 'filled'
    CANCELED = 'canceled'


class Offset(Enum):
    OPEN = 'OPEN'
    CLOSE = 'CLOSE'


logger = logging.getLogger("ClientOrder")

class ClientOrder():
    def __init__(self, strategy_order_id, exchange, global_symbol, symbol, side: Side, price: Decimal, size: Decimal, trading_session,
                 order_type:str = 'GTC', correlation_id: CorrelationID = None, remark = None, offset: Offset = None, margin_trade = None, priority = 0):
        self.strategy_order_id = strategy_order_id
        self.exchange = exchange
        self.global_symbol = global_symbol
        self.symbol = symbol
        self.side = side
        self.price = price
        self.size = size
        self.order_type = order_type
        assert order_type in ['GTC', 'IOC', 'POST_ONLY', 'OPPONENT_IOC']
        self.executed_size = Decimal("0")
        self.remaining_size = size
        self.avg_price = None
        self.state = State.PENDING
        self.order_status = OrderStatus.PENDING
        self.created_time = datetime.now()
        self.last_update_time = self.created_time
        self.correlation_id = correlation_id
        self.last_cancel_timestamp = None
        self.remark = remark
        self.offset = offset
        self.margin_trade = margin_trade
        self.trading_session = trading_session
        self._update_event = asyncio.Event()
        self.priority = priority
        assert priority in [0, 1]

    def update_info(self, info: dict):
        for k, v in info.items():
            if k == 'executed_size':
                if v is not None:
                    self.executed_size = v
                    self.remaining_size = self.size - v
            elif k == 'avg_price':
                if v is not None:
                    self.avg_price = v
            elif k == 'state':
                self.state = v

        if info:
            self.last_update_time = datetime.now()
            self._update_event.set()

    def at_market(self):
        return self.state in [State.OPEN, State.CLOSED]

    def is_closed(self):
        return self.state in [State.CLOSED, State.INTERNAL_CLOSED]

    async def cancel(self, correlation_id: CorrelationID = None):
        await self.trading_session.cancel_order(self.strategy_order_id, correlation_id = correlation_id)

    def on_update(self):
        self._update_event.set()

    async def wait_for_update(self):
        await self._update_event.wait()
        self._update_event.clear()
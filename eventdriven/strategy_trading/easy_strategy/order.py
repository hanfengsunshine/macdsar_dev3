import time
import uuid
from typing import Optional, Union
from decimal import Decimal, ROUND_UP, ROUND_DOWN
from strategy_trading.easy_strategy.data_type import Side, OrderStatus, OrderType, PositionEffect
from strategy_trading.easy_strategy.instrument_manager import Instrument


class Order(object):
    finished_status = {
        OrderStatus.REJECTED,
        OrderStatus.FILLED,
        OrderStatus.CANCELED
    }
    queuing_status = {
        OrderStatus.PENDING,
        OrderStatus.CONFIRMED,
        OrderStatus.PARTIAL_FILLED
    }

    def __init__(self,
        account: str,
        instrument: Instrument,
        side: Side,
        quantity: Decimal,
        price: Optional[Decimal] = None,
        order_type: OrderType = OrderType.GTC,
        position_effect: PositionEffect = PositionEffect.NONE,
        margin_trade: Optional[str] = None,
        place_timeout_in_us: int = 500000,
        cancel_timeout_in_us: int = 500000,
        client_order_id: Union[int, str] = None,
        client_link_order_id: Optional[str] = None,
        leverage_rate: Optional[int] = None):
        self.account = account
        self.instrument = instrument
        self.exchange_order_id = ''
        self.side = side
        self.price = price
        if self.price:
            self.price = self.price.quantize(self.instrument.price_tick, ROUND_DOWN) if side == Side.BUY else self.price.quantize(self.instrument.price_tick, ROUND_UP)
        self.quantity = quantity.quantize(self.instrument.lot_size, ROUND_DOWN)
        self.order_type = order_type
        self.position_effect = position_effect
        self.margin_trade = margin_trade
        self.cancel_timeout_in_us = cancel_timeout_in_us
        self.client_order_id = str(uuid.uuid1()) if not client_order_id else client_order_id
        self.client_link_order_id = client_link_order_id
        self.leverage_rate = leverage_rate
        self.status = OrderStatus.PENDING
        self.filled_quantity = Decimal('0')
        self.unfilled_quantity = self.quantity
        self.avg_trade_price = Decimal('0')
        self.last_trade_price = Decimal('0')
        self.last_trade_quantity = Decimal('0')
        self.create_timestamp = int(time.time() * 1000000)
        self.cancel_timestamp = 0
        self.place_reject_reason = ''
        self.pending_cancel = False
        self.cancel_reject_reason = ''
        self.closed_reason = ''

    def is_finished(self):
        return self.status in Order.finished_status

    def is_queuing(self):
        return self.status in Order.queuing_status

    def is_cancel_timeout(self):
        return True if not self.pending_cancel else int(time.time() * 1000000) - self.cancel_timestamp >= self.cancel_timeout_in_us
    
    def __repr__(self):
        return 'exch={} acc={} symbol={} side={} price={} qty={} type={} pe={} status={} client_order_id={} pending_cancel={} ' \
            'avg_trade_price={} filled_qty={} last_trade_price={} last_trade_qtye={}'.format(
                self.instrument.exchange, self.account, self.instrument.global_symbol,
                self.side, float(self.price), float(self.quantity), self.order_type, self.position_effect, 
                self.status, self.client_order_id, self.pending_cancel,
                float(self.avg_trade_price), float(self.filled_quantity),  
                float(self.last_trade_price), float(self.last_trade_quantity))
    
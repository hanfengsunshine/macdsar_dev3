import orjson
import asyncio
import websockets

from abc import ABC, abstractmethod
from typing import List
from enum import Enum
from loguru import logger
from decimal import Decimal
from collections import defaultdict

from strategy_trading.easy_strategy.data_type import Side, PositionEffect
from strategy_trading.easy_strategy.order import Order
from strategy_trading.easy_strategy.instrument_manager import Instrument


class PortfolioPosition:
    def __init__(self,
        instrument: Instrument,
        net_quantity: Decimal = Decimal('0'),
        net_average_price: Decimal = Decimal('0')):
        self.instrument = instrument
        self.virtual_net = net_quantity
        self.realized_net = net_quantity
        self.realized_average_price = net_average_price

    def reset(self,
        net_quantity: Decimal,
        net_average_price: Decimal):
        self.virtual_net = net_quantity
        self.realized_net = net_quantity
        self.realized_average_price = net_average_price


class PortfolioPositionManager:
    def __init__(self, portfolio):
        self.portfolio = portfolio
        self._positions = dict()

    def get_position(self, instrument: Instrument):
        if instrument not in self._positions:
            self._positions[instrument] = PortfolioPosition(instrument)
        return self._positions[instrument]

    def on_order_place(self, order: Order):
        position = self.get_position(order.instrument)

        if order.side == Side.BUY:
            position.virtual_net += order.quantity
        else:
            position.virtual_net -= order.quantity

    def on_order_reject(self, order: Order):
        position = self.get_position(order.instrument)

        if order.side == Side.BUY:
            position.virtual_net -= order.quantity
        else:
            position.virtual_net += order.quantity

    def on_order_trade(self, order: Order):
        if order.last_trade_quantity <= Decimal('0'):
            return

        position = self.get_position(order.instrument)

        if position.realized_net < Decimal('0'):
            if order.side == Side.BUY:
                realized_net = position.realized_net + order.last_trade_quantity
                if realized_net == Decimal('0'):
                    position.realized_average_price = Decimal('0')
                elif realized_net > Decimal('0'):
                    position.realized_average_price = order.last_trade_price
                position.realized_net = realized_net
            else:
                realized_net = position.realized_net - order.last_trade_quantity
                if order.instrument.is_traded_in_notional:
                    position.realized_average_price = abs(position.realized_net - order.last_trade_quantity) / (order.last_trade_quantity / order.last_trade_price + abs(position.realized_net) / position.realized_average_price)
                else:
                    position.realized_average_price = (abs(position.realized_net) * position.realized_average_price + order.last_trade_quantity * order.last_trade_price) / abs(realized_net)
                position.realized_net = realized_net
        elif position.realized_net > Decimal('0'):
            if order.side == Side.BUY:
                realized_net = position.realized_net + order.last_trade_quantity
                if order.instrument.is_traded_in_notional:
                    position.realized_average_price = realized_net / (order.last_trade_quantity / order.last_trade_price + position.realized_net / position.realized_average_price)
                else:
                    position.realized_average_price = (position.realized_net * position.realized_average_price + order.last_trade_quantity * order.last_trade_price) / realized_net
                position.realized_net = realized_net
            else:
                realized_net = position.realized_net - order.last_trade_quantity
                if realized_net == Decimal('0'):
                    position.realized_average_price = Decimal('0')
                elif realized_net < Decimal('0'):
                    position.realized_average_price = order.last_trade_price
                position.realized_net = realized_net
        else:
            position.realized_net = order.last_trade_quantity if order.side == Side.BUY else -order.last_trade_quantity
            position.realized_average_price = order.last_trade_price

    def on_order_finish(self, order: Order):
        position = self.get_position(order.instrument)

        if order.side == Side.BUY:
            position.virtual_net -= order.unfilled_quantity
        else:
            position.virtual_net += order.unfilled_quantity

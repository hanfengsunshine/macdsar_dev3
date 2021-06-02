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
from strategy_trading.easy_strategy.instrument_manager import Instrument, InstrumentManager


class AccountBalanceType(Enum):
    AVAILABLE = 0
    FROZEN = 1


class AccountPosition:
    def __init__(self, instrument: Instrument, long_quantity: Decimal = Decimal('0'), short_quantity: Decimal = Decimal('0')):
        self.instrument = instrument
        self.long_quantity = long_quantity
        self.short_quantity = short_quantity

    def reset(self, long_quantity: Decimal, short_quantity: Decimal):
        self.long_quantity = long_quantity
        self.short_quantity = short_quantity


class AccountInventoryManager:
    def __init__(self, exchange, account):
        self.exchange = exchange
        self.account = account
        self._balances = defaultdict(dict)
        self._positions = dict()
        self._instrument_mngr = InstrumentManager()

    def get_balance(self, currency: str):
        if currency not in self._balances:
            return {
                AccountBalanceType.AVAILABLE: Decimal('0'),
                AccountBalanceType.FROZEN: Decimal('0')
            }
        return self._balances[currency]

    def get_position(self, instrument: Instrument):
        if instrument not in self._positions:
            self._positions[instrument] = AccountPosition(instrument)
        return self._positions[instrument]

    def on_inventory_update(self, exchange, message):
        if exchange != self.exchange:
            return
        data = message['data']
        if 'balance' in data:
            if 'spot' in data['balance']:
                for currency, balance_info in data['balance']['spot'].items():
                    self._balances[currency][AccountBalanceType.AVAILABLE] = Decimal(str(balance_info['trade']))
            elif 'swap' in data['balance']:
                for currency, balance_info in data['balance']['swap'].items():
                    self._balances[currency][AccountBalanceType.AVAILABLE] = Decimal(str(balance_info['marginAvailable']))
            else:
                for currency, balance_info in data['balance']['contract'].items():
                    self._balances[currency][AccountBalanceType.AVAILABLE] = Decimal(str(balance_info['marginAvailable']))
        if 'position' in data:
            symbol_type = ''
            if 'swap' in data['position']:
                symbol_type = 'SWAP'
                info = data['position']['swap']
            else:
                symbol_type = 'FUTURE'
                info = data['position']['contract']

            for exchange_symbol, position_info in info.items():
                instrument = self._instrument_mngr.get_instrument_by_mds_specific_value(exchange_symbol, exchange, symbol_type)
                if not instrument:
                    continue
                position = self.get_position(instrument)
                if 'long' in position_info:
                    position.long_quantity = Decimal(position_info['long'])
                if 'short' in position_info:
                    position.short_quantity = Decimal(position_info['short'])

    def on_order_place(self, order: Order):
        if not order.instrument.is_twoway_position:
            return
        position = self.get_position(order.instrument)

        if order.position_effect == PositionEffect.NONE:
            if order.side == Side.BUY:
                if position.short_quantity < order.quantity:
                    order.position_effect = PositionEffect.OPEN
                else:
                    order.position_effect = PositionEffect.CLOSE
            else:
                if position.long_quantity < order.quantity:
                    order.position_effect = PositionEffect.OPEN
                else:
                    order.position_effect = PositionEffect.CLOSE

        if order.position_effect == PositionEffect.CLOSE:
            if order.side == Side.BUY:
                position.short_quantity -= order.quantity
            else:
                position.long_quantity -= order.quantity

    def on_order_reject(self, order: Order):
        if not order.instrument.is_twoway_position:
            return

        position = self.get_position(order.instrument)
        if order.position_effect == PositionEffect.CLOSE:
            if order.side == Side.BUY:
                position.short_quantity += order.quantity
            else:
                position.long_quantity += order.quantity

    def on_order_trade(self, order: Order):
        if not order.instrument.is_twoway_position:
            return

        position = self.get_position(order.instrument)
        if order.position_effect == PositionEffect.OPEN:
            if order.side == Side.BUY:
                position.long_quantity += order.last_trade_quantity
            else:
                position.short_quantity += order.last_trade_quantity

    def on_order_finish(self, order: Order):
        if not order.instrument.is_twoway_position:
            return

        position = self.get_position(order.instrument)
        if order.position_effect == PositionEffect.CLOSE:
            if order.side == Side.BUY:
                position.short_quantity += order.unfilled_quantity
            else:
                position.long_quantity += order.unfilled_quantity

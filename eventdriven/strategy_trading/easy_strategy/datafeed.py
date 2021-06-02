from decimal import Decimal
from operator import neg
from typing import Tuple, Optional
from sortedcontainers import SortedDict
from collections import deque

from strategy_trading.easy_strategy.data_type import Side
from strategy_trading.easy_strategy.instrument_manager import Instrument


class FullLevelOrderBook(object):
    def __init__(self, instrument):
        self.instrument = instrument
        self._bids = SortedDict(neg)
        self._asks = SortedDict()
        self.exchange_timestamp = 0
        self.connectivity_timestamp = 0
        self.exchange_sequence = None
        self.cached_updates = deque()
        self.has_been_initialized = False
        self.is_ready = False
        self.last_update_by_diff = False

    def on_full_snapshot(self, bids, asks, exchange_timestamp, connectivity_timestamp, exchange_sequence: Optional[int]=None):
        if self.has_been_initialized and exchange_sequence and exchange_sequence <= self.exchange_sequence:
            return

        self._bids.clear()
        for bid in bids:
            price = Decimal(str(bid[0]))
            qty = Decimal(str(bid[1]))
            self._bids[price] = (price, qty)

        self._asks.clear()
        for ask in asks:
            price = Decimal(str(ask[0]))
            qty = Decimal(str(ask[1]))
            self._asks[price] = (price, qty)

        self.exchange_timestamp = exchange_timestamp
        self.connectivity_timestamp = connectivity_timestamp
        self.exchange_sequence = exchange_sequence

        if len(self.cached_updates) == 0:
            self.has_been_initialized = True
        else:
            while len(self.cached_updates) > 0:
                update = self.cached_updates[0]
                exchange_sequence = update[-1]
                prev_exchange_sequence = update[-2]
                if exchange_sequence < self.exchange_sequence:
                    pass
                elif exchange_sequence == self.exchange_sequence:
                    self.has_been_initialized = True
                elif self.exchange_sequence == prev_exchange_sequence:
                    self.on_delta_snapshot(update[0], update[1], update[2], update[3], update[4], update[5])
                else:
                    self.has_been_initialized = False
                    self.cached_updates.clear()
                    break
                self.cached_updates.popleft()

        self.is_ready = True
        self.last_update_by_diff = False

    def on_delta_snapshot(self, bids, asks, exchange_timestamp, connectivity_timestamp, prev_exchange_sequence: Optional[int]=None, exchange_sequence: Optional[int]=None):
        if self.has_been_initialized:
            if prev_exchange_sequence and prev_exchange_sequence != self.exchange_sequence:
                self.has_been_initialized = False

        if not self.has_been_initialized:
            # print('queue', prev_exchange_sequence, exchange_sequence)
            self.cached_updates.append((bids, asks, exchange_timestamp, connectivity_timestamp, prev_exchange_sequence, exchange_sequence))
            return

        for bid in bids:
            price = Decimal(str(bid[0]))
            qty = Decimal(str(bid[1]))
            if qty == Decimal('0'):
                self._bids.pop(price, None)
            else:
                self._bids[price] = (price, qty)

        for ask in asks:
            price = Decimal(str(ask[0]))
            qty = Decimal(str(ask[1]))
            if qty == Decimal('0'):
                self._asks.pop(price, None)
            else:
                self._asks[price] = (price, qty)

        self.exchange_timestamp = exchange_timestamp
        self.connectivity_timestamp = connectivity_timestamp
        self.exchange_sequence = exchange_sequence
        self.last_update_by_diff = True

    def bids(self, level=None):
        return self._bids.values() if not level else self._bids.values()[0:level]

    def asks(self, level=None):
        return self._asks.values() if not level else self._asks.values()[0:level]


class FixedLevelOrderBook(object):
    def __init__(self, instrument: Instrument, level: int):
        self.instrument = instrument
        self.level = level
        self._bids = []
        self._asks = []
        self.exchange_timestamp = 0
        self.connectivity_timestamp = 0
        self.exchange_sequence = None
        self.is_ready = False

    def on_full_snapshot(self, bids, asks, exchange_timestamp, connectivity_timestamp, exchange_sequence: Optional[int]=None):
        self._bids.clear()
        for bid in bids:
            price = Decimal(str(bid[0]))
            qty = Decimal(str(bid[1]))
            self._bids.append((price, qty))

        self._asks.clear()
        for ask in asks:
            price = Decimal(str(ask[0]))
            qty = Decimal(str(ask[1]))
            self._asks.append((price, qty))

        self.exchange_timestamp = exchange_timestamp
        self.connectivity_timestamp = connectivity_timestamp
        self.exchange_sequence = exchange_sequence
        self.is_ready = True

    def bids(self):
        return self._bids

    def asks(self):
        return self._asks


class TopOrderBook:
    def __init__(self, instrument: Instrument):
        self.instrument = instrument
        self.is_ready = False
        self.exchange_timestamp = 0
        self.connectivity_timestamp = 0
        self.exchange_sequence = None
        self.best_bid = None
        self.best_ask = None
        self.prev_best_bid = None
        self.prev_best_ask = None
        self.prev_bbo_prices = None
        self.is_top_price_change = False

    def on_top_snapshot(self,
        best_bid: Tuple[Decimal, Decimal],
        best_ask: Tuple[Decimal, Decimal],
        exchange_timestamp: int,
        connectivity_timestamp: int,
        exchange_sequence: Optional[int]=None):
        if self.best_bid and self.best_ask:
            self.is_top_price_change = (self.best_bid[0] != best_bid[0]) or (self.best_ask[0] != best_ask[0])
            if self.is_top_price_change:
                self.prev_bbo_prices = (self.best_bid[0], self.best_ask[0])
        else:
            self.is_top_price_change = True

        self.prev_best_bid = self.best_bid
        self.prev_best_ask = self.best_ask
        self.best_bid = best_bid
        self.best_ask = best_ask
        self.exchange_timestamp = exchange_timestamp
        self.connectivity_timestamp = connectivity_timestamp
        self.exchange_sequence = exchange_sequence
        self.is_ready = True


class Trade(object):
    def __init__(self,
        instrument: Instrument,
        side: Side,
        price: Decimal,
        quantity: Decimal,
        exchange_timestamp: int,
        connectivity_timestamp: int,
        exchange_sequence: Optional[int]=None):
        self.instrument = instrument
        self.side = side
        self.price = price
        self.quantity = quantity
        self.exchange_timestamp = exchange_timestamp
        self.connectivity_timestamp = connectivity_timestamp
        self.exchange_sequence = exchange_sequence


class Kline(object):
    def __init__(self,
        instrument: Instrument,
        interval_in_seconds: int,
        open_px: Decimal,
        high_px: Decimal,
        low_px: Decimal,
        close_px: Decimal,
        volume: Decimal,
        amount: Decimal,
        open_timestamp: int,
        close_timestamp: int,
        connectivity_timestamp: int):
        self.instrument = instrument
        self.interval = int(interval_in_seconds)
        self.open = open_px
        self.high = high_px
        self.low = low_px
        self.close = close_px
        self.volume = volume
        self.amount = amount
        self.open_timestamp = open_timestamp
        self.close_timestamp = close_timestamp
        self.connectivity_timestamp = connectivity_timestamp


class Ticker(object):
    def __init__(self,
        instrument: Instrument,
        last_price: Decimal,
        last_quantity: Decimal,
        best_bid_price: Decimal,
        best_bid_quantity: Decimal,
        best_ask_price: Decimal,
        best_ask_quantity: Decimal,
        exchange_timestamp: int,
        connectivity_timestamp: int,
        exchange_sequence: Optional[int]=None):
        self.instrument = instrument
        self.last_price = last_price
        self.last_quantity = last_quantity
        self.best_bid_price = best_bid_price
        self.best_bid_quantity = best_bid_quantity
        self.best_ask_price = best_ask_price
        self.best_ask_quantity = best_ask_quantity
        self.exchange_timestamp = exchange_timestamp
        self.connectivity_timestamp = connectivity_timestamp
        self.exchange_sequence = exchange_sequence


class IndexTicker(object):
    def __init__(self,
        instrument: Instrument,
        last_price: Decimal,
        exchange_timestamp: int,
        connectivity_timestamp: int):
        self.instrument = instrument
        self.last_price = last_price
        self.exchange_timestamp = exchange_timestamp
        self.connectivity_timestamp = connectivity_timestamp

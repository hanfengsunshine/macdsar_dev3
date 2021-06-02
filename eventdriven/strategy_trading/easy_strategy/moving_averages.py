from decimal import Decimal
from collections import deque


class SMA:
    def __init__(self, window, min_updates=1):
        self.window = window
        self.min_updates = min_updates
        assert isinstance(self.window, int) and self.window >= 1
        assert isinstance(self.min_updates, int) and 1 <= self.min_updates <= self.window
        self.values = deque()
        self.amount = Decimal('0')
        self.ma_value = None

    def add(self, value: Decimal):
        self.values.append(value)
        self.amount += value
        if len(self.values) >= self.window:
            out_value = self.values.popleft()
            self.amount -= out_value
        self.ma_value = self.amount / len(self.values)

    def ma(self):
        if len(self.values) >= self.min_updates:
            return self.ma_value
        else:
            return None


class EMA(object):
    def __init__(self, window, min_updates=None):
        self.window = window
        self.min_updates = min_updates if min_updates is not None else window
        assert isinstance(self.window, int) and self.window >= 1
        assert isinstance(self.min_updates, int) and self.min_updates >= 1
        self.decay = Decimal(2. / (1 + self.window))
        self.updates = 0
        self.ma_value = None

    def add(self, value: Decimal):
        if self.updates == 0:
            self.ma_value = value
        else:
            self.ma_value = self.ma_value * (1 - self.decay) + self.decay * value
        if self.updates < self.min_updates:
            self.updates += 1

    def reset(self):
        self.updates = 0
        self.ma_value = None

    def ma(self):
        if self.updates >= self.min_updates:
            return self.ma_value
        else:
            return None

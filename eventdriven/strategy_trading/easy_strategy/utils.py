import time

from decimal import Decimal
from strategy_trading.easy_strategy.data_type import Side


def get_current_us():
    return int(time.time() * 1000000)


def decimal_to_readable(precision, value):
    return ('%.{}f'.format(precision) % value) if precision != Decimal('0') else str(int(value))


def normalize_fraction(d):
    normalized = d.normalize()
    return normalized if normalized.as_tuple()[2] <= 0 else normalized.quantize(1)


def quantize(d, fraction, up):
    if d % fraction == Decimal('0'):
        return d
    else:
        return fraction * int(d / fraction) if not up else fraction * (int(d / fraction) + 1)


SideSign = [1, -1]
def side_sign(side: Side):
    return SideSign[side.value]

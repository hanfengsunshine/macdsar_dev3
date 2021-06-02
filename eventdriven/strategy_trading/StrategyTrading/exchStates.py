from enum import Enum


class ExchangeState(Enum):
    NO_ALERT = "NO_ALERT"
    WITHDRAWAL_ORDER_RATE_WARNING = "WITHDRAWAL_ORDER_RATE_WARNING"
    WITHDRAWAL_ORDER_LIMIT_REACHED = "WITHDRAWAL_ORDER_LIMIT_REACHED"
    API_END_POINT_LIMIT_REACHED = "API_END_POINT_LIMIT_REACHED"
    USED_WEIGHT_WARNING = "USED_WEIGHT_WARNING"
    ORDER_COUNT_WARNING = "ORDER_COUNT_WARNING"
    USED_WEIGHT_BREACHED = "USED_WEIGHT_BREACHED"
    ORDER_COUNT_BREACHED = "ORDER_COUNT_BREACHED"
    UNRECOGNIZED = 'UNRECOGNIZED'


def is_quoting_enabled(state: ExchangeState):
    if state == ExchangeState.NO_ALERT:
        return True
    return False


def is_taking_enabled(state: ExchangeState):
    if state not in [ExchangeState.WITHDRAWAL_ORDER_LIMIT_REACHED,
                     ExchangeState.API_END_POINT_LIMIT_REACHED,
                     ExchangeState.USED_WEIGHT_BREACHED,
                     ExchangeState.ORDER_COUNT_BREACHED,
                     ExchangeState.UNRECOGNIZED]:
        return True
    return False
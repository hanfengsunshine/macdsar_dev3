from enum import Enum


class Side(Enum):
    BUY = 0
    SELL = 1


class OrderType(Enum):
    GTC = 1
    IOC = 2
    FOK = 3
    POST_ONLY = 4
    OPPONENT_IOC = 5
    MARKET = 6
    OPTIMAL_5 = 7
    OPTIMAL_5_IOC = 8
    OPTIMAL_10 = 9
    OPTIMAL_10_IOC = 10


class OrderStatus(Enum):
    PENDING = 1
    CONFIRMED = 2
    REJECTED = 3
    PARTIAL_FILLED = 4
    FILLED = 5
    CANCELED = 6

    def is_completed(self):
        return self in {OrderStatus.REJECTED,
                        OrderStatus.FILLED,
                        OrderStatus.CANCELED}

class PositionEffect(Enum):
    NONE = 1
    OPEN = 2
    CLOSE = 3


class DatafeedType(Enum):
    FULL_LEVEL_ORDERBOOK = 'ob'
    FIXED_LEVEL_ORDERBOOK = 'ob'
    TRADE = 'trade'
    KLINE = 'kline'
    TICKER = 'ticker'
    INDEX_TICKER = 'index_ticker'
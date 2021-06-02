from strategy_trading.StrategyTrading.correlationID import CorrelationID
from strategy_trading.StrategyTrading.order import Side
from decimal import Decimal


class MarketTrade():
    def __init__(self, price: Decimal, size: Decimal, side: Side, more_coming=False, exch_timestamp=None,
                 correlation_id: CorrelationID = None, seq_id=None):
        self.price = Decimal(price)
        self.size = Decimal(size)
        self.side = side
        self.exch_timestamp = exch_timestamp
        self.more_coming = more_coming
        self.correlation_id = correlation_id
        self.seq_id = seq_id

    def __repr__(self):
        return "{}:{} {}@{}".format(str(self.correlation_id), self.side.name, self.price, self.size)


class OrderBookDepth():
    def __init__(self, bids: list, asks: list, exch_timestamp=None, correlation_id: CorrelationID = None, seq_id=None):
        self.bids = [[Decimal(b[0]), Decimal(b[1])] for b in bids]
        self.asks = [[Decimal(a[0]), Decimal(a[1])] for a in asks]
        self.correlation_id = correlation_id
        self.exch_timestamp = exch_timestamp
        self.seq_id = seq_id


class OrderBookDiff():
    def __init__(self, bids: list, asks: list, exch_timestamp=None, correlation_id: CorrelationID = None, seq_id=None):
        self.bids = [[Decimal(b[0]), Decimal(b[1])] for b in bids]
        self.asks = [[Decimal(a[0]), Decimal(a[1])] for a in asks]
        self.correlation_id = correlation_id
        self.exch_timestamp = exch_timestamp
        self.seq_id = seq_id


class Ticker():
    def __init__(self, bid1p, bid1s, ask1p, ask1s, exch_timestamp=None, correlation_id: CorrelationID = None,
                 seq_id=None):
        self.bid1p = Decimal(bid1p)
        self.bid1s = Decimal(bid1s)
        self.ask1p = Decimal(ask1p)
        self.ask1s = Decimal(ask1s)
        self.correlation_id = correlation_id
        self.exch_timestamp = exch_timestamp
        self.seq_id = seq_id

    def __repr__(self):
        return "{}: b: {}@{} a: {}@{}".format(str(self.correlation_id), self.bid1p, self.bid1s, self.ask1p, self.ask1s)


class Kline():
    def __init__(self, freq_seconds, start_timestamp, open_price, high, low, close_price, volume, amount, count,
                 exch_timestamp=None, correlation_id: CorrelationID = None):
        self.freq_seconds = freq_seconds
        self.open_price = Decimal(open_price)
        self.high = Decimal(high)
        self.low = Decimal(low)
        self.close_price = Decimal(close_price)
        self.volume = Decimal(volume)
        self.amount = Decimal(amount)
        self.count = count
        self.start_timestamp = start_timestamp
        self.correlation_id = correlation_id
        self.exch_timestamp = exch_timestamp

    def __repr__(self):
        return "{}|{} {}->{}".format(self.freq_seconds, self.start_timestamp, self.open_price, self.close_price)


class QUOTE():
    def __init__(self, id, expire_timestamp, buy_price, buy_quantity: dict, buy_amount: dict, sell_price,
                 sell_quantity: dict, sell_amount: dict, exch_timestamp=None, correlation_id: CorrelationID = None):
        self.id = id
        self.expire_timestamp = expire_timestamp
        self.buy_price = Decimal(buy_price)
        self.buy_quantity = {
            'quantity': Decimal(buy_quantity['quantity']),
            'currency': buy_quantity['currency']
        }
        self.buy_amount = {
            'amount': Decimal(buy_amount['amount']),
            'currency': buy_amount['currency']
        }
        self.sell_price = Decimal(sell_price)
        self.sell_quantity = {
            'quantity': Decimal(sell_quantity['quantity']),
            'currency': sell_quantity['currency']
        }
        self.sell_amount = {
            'amount': Decimal(sell_amount['amount']),
            'currency': sell_amount['currency']
        }
        self.exch_timestamp = exch_timestamp
        self.correlation_id = correlation_id

    def __repr__(self):
        return "{}|{}: buy: {}@{}{}, sell: {}@{}{}".format(
            self.id, self.expire_timestamp, self.buy_price, self.buy_amount['amount'],
            self.buy_amount['currency'], self.sell_price, self.sell_amount['amount'],
            self.sell_amount['currency'])

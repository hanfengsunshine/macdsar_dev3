from decimal import Decimal
from strategy_trading.StrategyTrading.correlationID import CorrelationID
import logging
from strategy_trading.StrategyTrading.marketData import OrderBookDepth, OrderBookDiff, MarketTrade, Ticker, Kline
from typing import List
from strategy_trading.StrategyTrading.order import ClientOrder, Side
from enum import Enum


class SeqType(Enum):
    DEPTH = 1
    DIFF = 2
    TICKER = 3
    MARKET_TRADE = 4


class BaseOrderBook():
    def __init__(self, exchange, global_symbol, print_when_data: bool = True):
        self.exchange = exchange
        self.symbol = global_symbol
        self.bids = []
        self.asks = []
        self.correlation_id = None

        self.logger = logging.getLogger("{}-{}-{}".format(self.__class__.__name__, exchange, global_symbol))
        self.seq_id = None
        self.seq_type = None
        self.print_when_data = print_when_data

    def update_depth(self, depth: OrderBookDepth):
        self.bids = depth.bids
        self.asks = depth.asks
        self.correlation_id = depth.correlation_id

        self.seq_id = depth.seq_id
        self.seq_type = SeqType.DEPTH

        self.print()

    def print(self):
        if not self.print_when_data:
            return
        if self.bids and self.asks:
            bid1 = self.bids[0]
            ask1 = self.asks[0]
            self.logger.info("bid1p: {:.10f}, ask1p: {:.10f}, bidv: {:.8f}, ask1v: {:.8f}. time: {}".format(bid1[0], ask1[0], bid1[1], ask1[1], self.correlation_id))
        else:
            self.logger.warning("depth not complete. bids: {}, asks: {}".format(len(self.bids), len(self.asks)))

    def update_diff(self, diff: OrderBookDiff):
        if self.seq_id is not None and diff.seq_id is not None:
            if self.seq_id > diff.seq_id:
                return

        for bid in diff.bids:
            b_find = False
            for i in range(len(self.bids)):
                localbid = self.bids[i]
                if bid[0] > localbid[0]:
                    # new bid
                    if bid[1] > Decimal("0"):
                        self.bids.insert(i, bid)
                    b_find = True
                    break
                elif bid[0] == localbid[0]:
                    if bid[1] > Decimal("0"):
                        # update size
                        localbid[1] = bid[1]
                    else:
                        # remove level
                        self.bids.pop(i)
                    b_find = True
                    break
            if not b_find and bid[1] > Decimal("0"):
                # new bid
                self.bids.append(bid)

        for ask in diff.asks:
            b_find = False
            for i in range(len(self.asks)):
                localask = self.asks[i]
                if ask[0] < localask[0]:
                    # new bid
                    if ask[1] > Decimal("0"):
                        self.asks.insert(i, ask)
                    b_find = True
                    break
                elif ask[0] == localask[0]:
                    if ask[1] > Decimal("0"):
                        # update size
                        localask[1] = ask[1]
                    else:
                        # remove level
                        self.asks.pop(i)
                    b_find = True
                    break
            if not b_find and ask[1] > Decimal("0"):
                # new bid
                self.asks.append(ask)

        self.correlation_id = diff.correlation_id
        self.seq_id = diff.seq_id
        self.seq_type = SeqType.DIFF
        self.print()

    def is_invalid(self):
        if not self.bids or not self.asks:
            return True

        if self.bids[0][0] >= self.asks[0][0]:
            return True

        return False

    def get_mid_price(self):
        return (self.bids[0][0] + self.asks[0][0]) / Decimal("2")

    def get_cleaned_book(self, self_orders: List[ClientOrder], levels: int):
        self_orders = self_orders[:]

        bids = []
        bid_len = 0
        for bid in self.bids:
            # find self volume
            self_volume = Decimal("0")
            to_remove_orders = []
            for order in self_orders:
                if order.side == Side.BUY and order.price == bid[0]:
                    self_volume += order.remaining_size
                    to_remove_orders.append(order)

            for order in to_remove_orders:
                self_orders.remove(order)

            if bid[1] > self_volume:
                other_vol = bid[1] - self_volume
                bids.append([bid[0], other_vol])
                bid_len += 1
                if bid_len >= levels:
                    break

        asks = []
        ask_len = 0
        for ask in self.asks:
            # find self volume
            self_volume = Decimal("0")
            to_remove_orders = []
            for order in self_orders:
                if order.side == Side.SELL and order.price == ask[0]:
                    self_volume += order.remaining_size
                    to_remove_orders.append(order)

            for order in to_remove_orders:
                self_orders.remove(order)

            if ask[1] > self_volume:
                other_vol = ask[1] - self_volume
                asks.append([ask[0], other_vol])
                ask_len += 1
                if ask_len >= levels:
                    break

        return bids, asks

    def apply_market_trade(self, market_trade: MarketTrade):
        if self.seq_id is not None and market_trade.seq_id is not None:
            if market_trade.seq_id > self.seq_id or (
                    market_trade.seq_id == self.seq_id and self.seq_type == SeqType.MARKET_TRADE):
                side = market_trade.side
                price = market_trade.price
                size = market_trade.size

                if self.print_when_data:
                    self.logger.debug("adjust resv({}) by trade {:.10f}@{:.8f}".format(side, price, size))
                if side == Side.BUY:
                    to_remove_asks = []
                    for ask_i in range(len(self.asks)):
                        ask = self.asks[ask_i]
                        if ask[0] < price:
                            to_remove_asks.append(ask_i)
                        elif ask[0] == price:
                            drop_vol = min(size, ask[1])
                            ask[1] -= drop_vol
                            if ask[1] <= Decimal("0"):
                                to_remove_asks.append(ask_i)
                            size -= drop_vol
                            if size <= Decimal("0"):
                                break
                        else:
                            break
                    for ask_i in to_remove_asks[::-1]:
                        self.asks.pop(ask_i)

                elif side == Side.SELL:
                    to_remove_bids = []
                    for bid_i in range(len(self.bids)):
                        bid = self.bids[bid_i]
                        if bid[0] > price:
                            to_remove_bids.append(bid_i)
                        elif bid[0] == price:
                            drop_vol = min(size, bid[1])
                            bid[1] -= drop_vol
                            if bid[1] <= Decimal("0"):
                                to_remove_bids.append(bid_i)
                            size -= drop_vol
                            if size <= Decimal("0"):
                                break
                        else:
                            break
                    for bid_i in to_remove_bids[::-1]:
                        self.bids.pop(bid_i)

                self.correlation_id = market_trade.correlation_id
                self.seq_id = market_trade.seq_id
                self.seq_type = SeqType.MARKET_TRADE
                self.print()

    def apply_ticker(self, ticker: Ticker):
        if self.seq_id is not None and ticker.seq_id is not None:
            if ticker.seq_id >= self.seq_id:
                ask1p = ticker.ask1p
                ask1s = ticker.ask1s

                if self.print_when_data:
                    self.logger.info("adjust by ticker {}".format(ticker))
                to_remove_asks = []
                ask_applied = False
                for ask_i in range(len(self.asks)):
                    ask = self.asks[ask_i]
                    ask_price = ask[0]
                    if ask_price < ask1p:
                        to_remove_asks.append(ask_i)
                    elif ask_price == ask1p:
                        ask[1] = ask1s
                        ask_applied = True
                        break
                    else:
                        break

                for ask_i in to_remove_asks[::-1]:
                    self.asks.pop(ask_i)
                if not ask_applied:
                    self.asks.insert(0, [ask1p, ask1s])

                bid1p = ticker.bid1p
                bid1s = ticker.bid1s
                to_remove_bids = []
                bid_applied = False
                for bid_i in range(len(self.bids)):
                    bid = self.bids[bid_i]
                    bid1_price = bid[0]
                    if bid1_price > bid1p:
                        to_remove_bids.append(bid_i)
                    elif bid1_price == bid1p:
                        bid[1] = bid1s
                        bid_applied = True
                        break
                    else:
                        break
                for bid_i in to_remove_bids[::-1]:
                    self.bids.pop(bid_i)
                if not bid_applied:
                    self.bids.insert(0, [bid1p, bid1s])

                self.correlation_id = ticker.correlation_id
                self.seq_id = ticker.seq_id
                self.seq_type = SeqType.TICKER
                self.print()

    def update_and_ret_trades_and_klines(self, updates: list):
        trades = []
        klines = []
        for _update in updates:
            if isinstance(_update, MarketTrade):
                self.apply_market_trade(_update)
                trades.append(_update)
            elif isinstance(_update, Ticker):
                self.apply_ticker(_update)
            elif isinstance(_update, OrderBookDiff):
                self.update_diff(_update)
            elif isinstance(_update, OrderBookDepth):
                self.update_depth(_update)
            elif isinstance(_update, Kline):
                klines.append(_update)
            else:
                self.logger.warning("Unrecognized type {}".format(type(_update)))
        return trades, klines

    def get_taking_price_size(self, side: Side, size):
        if side == Side.BUY:
            book = self.asks
        elif side == Side.SELL:
            book = self.bids
        else:
            raise ValueError('Invalid side {}'.format(side))

        cumu_size = Decimal("0")
        price = None
        for level in book:
            find_size = min(level[1], size - cumu_size)
            cumu_size += find_size
            if cumu_size >= size:
                return level[0], size
            price = level[0]
        return price, cumu_size

    def get_taking_price_size_by_value(self, side: Side, value):
        if side == Side.BUY:
            book = self.asks
        elif side == Side.SELL:
            book = self.bids
        else:
            raise ValueError('Invalid side {}'.format(side))

        cumu_value = Decimal("0")
        cumu_size = Decimal("0")
        price = None
        for level in book:
            find_size = min(level[1] * level[0], value - cumu_value)
            cumu_size += find_size / level[0]
            cumu_value += find_size
            if cumu_value >= value:
                return level[0], cumu_size
            price = level[0]
        return price, cumu_size
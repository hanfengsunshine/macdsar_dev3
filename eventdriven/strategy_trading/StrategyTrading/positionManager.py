from strategy_trading.ExchangeHelper.constants import EXCHANGE_NAME_BINANCE_SWAP
from strategy_trading.StrategyTrading.symbolHelper import SymbolHelper
from decimal import Decimal
from strategy_trading.StrategyTrading.order import Side, Offset
import logging
from strategy_trading.StrategyTrading.inventoryManager import InventoryManager, HuobiContractInventoryManager
from strategy_trading.StrategyTrading.errors import LocalPositionInvalid
from typing import List, Dict
import copy
from datetime import datetime
from strategy_trading.ExchangeHelper.constants import EXCHANGE_NAME_BINANCE_SWAP, EXCHANGE_NAME_OKEX_CONTRACT, EXCHANGE_NAME_BINANCE_CONTRACT


class SigSybPM():
    def __init__(self, exchange, global_symbol, symbol_helper: SymbolHelper, alias = None, inventory_manager: InventoryManager = None):
        self.exchange = exchange
        self.global_symbol = global_symbol
        self.is_inverse = symbol_helper.is_inverse_contract(global_symbol, exchange)
        symbol_info = symbol_helper.get_info(global_symbol, exchange)
        self.pnl_in_quote = symbol_info.get('quote_as_margin', None) or symbol_info['symbol_type'] == 'SPOT'
        self.size_multiplier = symbol_info['size_multiplier']

        self.turnover = Decimal("0")
        self.position = Decimal("0")
        self.entry_price = None
        self.realized_pnl = Decimal("0")
        self.realized_pnl_margin = Decimal("0")

        # used to calc hit rate
        self.rounds = 0
        self.winning_rounds = 0
        self.last_realized_pnl = Decimal("0")
        self.last_realized_side = None
        self.hit_rate = 0

        # used to record last time of opening position
        self.open_position_time = None

        self.inventory_manager = inventory_manager

        if self.inventory_manager is not None and not isinstance(self.inventory_manager, HuobiContractInventoryManager) and self.exchange in [EXCHANGE_NAME_BINANCE_SWAP, EXCHANGE_NAME_BINANCE_CONTRACT]:
            self.inventory_manager.bind_position_manager(self)

        self.logger = logging.getLogger("{}-{}-{}{}".format(self.__class__.__name__, self.exchange, self.global_symbol, "-" + alias if isinstance(alias, str) else ""))

    def _open_new_position(self, price, size, side: Side):
        if self.position != Decimal("0"):
            if side == Side.BUY:
                if not self.is_inverse:
                    self.entry_price = (price * size + self.position * self.entry_price) / (size + self.position)
                else:
                    self.entry_price = (size + self.position) / (size / price + self.position / self.entry_price)
                self.position += size
            elif side == Side.SELL:
                if not self.is_inverse:
                    self.entry_price = (price * size - self.position * self.entry_price) / (size - self.position)
                else:
                    self.entry_price = (size - self.position) / (size / price - self.position / self.entry_price)
                self.position -= size
        else:
            self.entry_price = price
            if side == Side.BUY:
                self.position = size
            else:
                self.position = -size

        self.open_position_time = datetime.now()

    def _close_position(self, price, size, side: Side):
        close_size = min(size, abs(self.position))
        if self.position > Decimal("0"):
            if not self.is_inverse:
                new_realized_pnl = close_size * (price - self.entry_price)
            else:
                new_realized_pnl = close_size / self.entry_price * price - close_size
            self.position -= close_size
        elif self.position < - Decimal("0"):
            if not self.is_inverse:
                new_realized_pnl = close_size * (self.entry_price - price)
            else:
                new_realized_pnl = - (close_size / self.entry_price * price - close_size)
            self.position += close_size

        self._update_hit_rate(side, new_realized_pnl)

        self.realized_pnl += new_realized_pnl
        if self.is_inverse:
            self.realized_pnl_margin += new_realized_pnl / price
        else:
            self.realized_pnl_margin += new_realized_pnl
        return close_size

    def _update_hit_rate(self, side: Side, new_realized_pnl):
        if side == self.last_realized_side:
            # finetune last round
            if self.last_realized_pnl > Decimal("0"):
                self.winning_rounds -= 1
            self.last_realized_pnl += new_realized_pnl

            if self.last_realized_pnl > Decimal("0"):
                self.winning_rounds += 1
        else:
            # new round
            self.rounds += 1
            if new_realized_pnl > Decimal("0"):
                self.winning_rounds += 1
            self.last_realized_pnl = new_realized_pnl
        self.hit_rate = self.winning_rounds / self.rounds

    def add_trade(self, side: Side, price: Decimal, size: Decimal, affect_inventory = True, **kwargs):
        open_size = Decimal("0")
        close_size = Decimal("0")

        if self.position != Decimal("0"):
            if (self.position > Decimal("0") and side == Side.BUY) or (self.position < Decimal("0") and side == Side.SELL):
                # open new position
                self._open_new_position(price, size, side)
                open_size += size
            else:
                # close position first
                close_size = self._close_position(price, size, side)
                remaining_size = size - close_size
                if remaining_size > Decimal("0"):
                    self._open_new_position(price, remaining_size, side)
                    open_size += remaining_size
        else:
            self._open_new_position(price, size, side)
            open_size += size

        if not self.is_inverse:
            self.turnover += price * size
        else:
            self.turnover += size

        entry_price = None if self.entry_price is None else "{:.10f}".format(self.entry_price)
        self.logger.info("EXEC {} {:.10f}@{:.8f}. Position: {:.4f}. RPnL: {:.4f}, TV: {:.4f}, ROT: {:.4f}%, EP: {}, Hit: {:.4f}%".format(side.name, price, size, self.position, self.realized_pnl, self.turnover, self.realized_pnl / self.turnover * 100, entry_price, self.hit_rate * 100))

        if self.inventory_manager is not None and affect_inventory:
            if open_size > Decimal("0"):
                self.inventory_manager.update_exec(self.global_symbol, side, price, open_size, offset = Offset.OPEN)
            if close_size > Decimal("0"):
                self.inventory_manager.update_exec(self.global_symbol, side, price, close_size, offset=Offset.CLOSE)

        self.last_realized_side = side

    def get_unrealized_pnl(self, price):
        unrealized_pnl = Decimal("0")
        if self.position != Decimal("0"):
            if self.is_inverse:
                unrealized_pnl = self.position / self.entry_price * price - self.position
            else:
                unrealized_pnl = self.position * (price - self.entry_price)
        return unrealized_pnl

    def get_position(self):
        return self.position

    def get_position_base(self):
        if self.is_inverse:
            if self.position != Decimal("0"):
                return self.position / self.entry_price
            else:
                return Decimal("0")
        else:
            return self.position

    def get_position_quote(self):
        if not self.is_inverse:
            if self.position != Decimal("0"):
                return - self.position * self.entry_price
            else:
                return Decimal("0")
        else:
            return - self.position

    def get_delta_quote(self):
        if self.pnl_in_quote:
            return self.get_position_quote() + self.realized_pnl
        else:
            return self.get_position_quote()

    def get_delta_base(self):
        if self.pnl_in_quote:
            return self.get_position()
        else:
            if self.is_inverse:
                if self.position != Decimal("0"):
                    return self.position / self.entry_price + self.realized_pnl_margin
                else:
                    return Decimal("0")
            else:
                return self.position


class SigSybPMOS(SigSybPM):
    def __init__(self, exchange, global_symbol, symbol_helper: SymbolHelper, alias = None, inventory_manager: InventoryManager = None, max_account_position = None):
        super(SigSybPMOS, self).__init__(exchange, global_symbol, symbol_helper, alias = alias, inventory_manager = inventory_manager)

        assert exchange in ['HUOBI_CONTRACT', 'HUOBI_SWAP', "OKEX_SWAP", EXCHANGE_NAME_OKEX_CONTRACT]

        self.long_position = Decimal("0")
        self.short_position = Decimal("0")
        self.entry_long_price = None
        self.entry_short_price = None
        self.fut_size_multiplier = symbol_helper.get_info(global_symbol, exchange)['size_multiplier']
        self.max_account_position = max_account_position
        if self.inventory_manager is not None:
            self.inventory_manager.bind_position_manager(self)

        symbol_info = symbol_helper.get_info(global_symbol, exchange)
        self.pnl_in_quote = symbol_info['quote_as_margin'] or symbol_info['symbol_type'] == 'SPOT'

    def _open_new_position(self, price, size, side: Side):
        if side == Side.BUY:
            if self.long_position != Decimal("0"):
                self.entry_long_price = (size + self.long_position) / (size / price + self.long_position / self.entry_long_price)
                self.long_position += size
            else:
                self.entry_long_price = price
                self.long_position = size
        elif side == Side.SELL:
            if self.short_position != Decimal("0"):
                self.entry_short_price = (size + self.short_position) / (size / price + self.short_position / self.entry_short_price)
                self.short_position += size
            else:
                self.entry_short_price = price
                self.short_position = size
        self.position = self.long_position - self.short_position
        if self.long_position > Decimal("0") and self.short_position > Decimal("0"):
            self.logger.warning("Both sides have positive positions. Pls Check. long: {:.0f}, short: {:.0f}".format(self.long_position, self.short_position))

        if self.position > Decimal("0"):
            self.entry_price = self.entry_long_price
        elif self.position < Decimal("0"):
            self.entry_price = self.entry_short_price

        if (self.position > Decimal("0") and side == Side.BUY) or (self.position < Decimal("0") and side == Side.SELL):
            self.open_position_time = datetime.now()

    def _close_position(self, price, size, side: Side):
        new_realized_pnl = None

        if side == Side.BUY:
            if size > self.short_position:
                self.logger.critical("local short position is {:.0f} while new buy_close size is {:.0f}".format(self.short_position, size))
                extra_size = size - self.short_position
                size = self.short_position
            else:
                extra_size = Decimal("0")

            if size > Decimal("0"):
                new_realized_pnl = -(size / self.entry_short_price * price - size)

            self.short_position -= size
            if self.short_position == Decimal("0"):
                self.entry_short_price = None

            if extra_size > Decimal("0"):
                # open position so that local position is relatively right.
                self._open_new_position(price, extra_size, side)
        elif side == Side.SELL:
            if size > self.long_position:
                self.logger.critical("local long position is {:.0f} while new sell_close size is {:.0f}".format(self.short_position, size))
                extra_size = size - self.long_position
                size = self.long_position
            else:
                extra_size = Decimal("0")

            if size > Decimal("0"):
                new_realized_pnl = size / self.entry_long_price * price - size

            self.long_position -= size
            if self.long_position == Decimal("0"):
                self.entry_long_price = None

            if extra_size > Decimal("0"):
                # open position so that local position is relatively right.
                self._open_new_position(price, extra_size, side)

        if new_realized_pnl is not None:
            self.realized_pnl += new_realized_pnl
            self._update_hit_rate(side, new_realized_pnl)

            if self.is_inverse:
                self.realized_pnl_margin += new_realized_pnl / price
            else:
                self.realized_pnl_margin += new_realized_pnl

        self.position = self.long_position - self.short_position
        if self.position > Decimal("0"):
            self.entry_price = self.entry_long_price
        elif self.position < Decimal("0"):
            self.entry_price = self.entry_short_price

    def add_trade(self, side: Side, price: Decimal, size: Decimal, offset: Offset, affect_inventory = True, **kwargs):
        if offset == Offset.OPEN:
            self._open_new_position(price, size, side)
        elif offset == Offset.CLOSE:
            self._close_position(price, size, side)

        self.turnover += size
        long_entry_price = None if self.entry_long_price is None else "{:.5f}".format(self.entry_long_price)
        short_entry_price = None if self.entry_short_price is None else "{:.5f}".format(self.entry_short_price)
        self.logger.info("EXEC {}/{} {:.5f}@{:.0f}. LongPos: {:.0f}. ShortPos: {:.0f}. RPnL: {:.4f}, TV: {:.4f}, ROT: {:.4f}%. LP: {}, SP: {}, Hit: {:.4f}%".format(side.name, offset.name, price, size, self.long_position, self.short_position, self.realized_pnl, self.turnover, self.realized_pnl / self.turnover * 100, long_entry_price, short_entry_price, self.hit_rate * 100))

        if self.inventory_manager is not None and affect_inventory:
            self.inventory_manager.update_exec(self.global_symbol, side, price, size, offset = offset)

        if offset == Offset.OPEN:
            self.last_realized_side = None
        else:
            self.last_realized_side = side

    def get_unrealized_pnl(self, price):
        unrealized_pnl = Decimal("0")
        if self.long_position != Decimal("0"):
            if self.is_inverse:
                unrealized_pnl += self.long_position / self.entry_long_price * price - self.long_position
            else:
                unrealized_pnl += self.long_position * (price - self.entry_long_price)
        if self.short_position != Decimal("0"):
            if self.is_inverse:
                unrealized_pnl += - self.short_position / self.entry_short_price * price + self.short_position
            else:
                unrealized_pnl += - self.short_position * (price - self.entry_short_price)

        return unrealized_pnl

    def get_position(self):
        return self.position

    def get_position_quote(self):
        if not self.is_inverse:
            if self.position != Decimal("0"):
                return - self.position * self.entry_price
            else:
                return Decimal("0")
        else:
            return - self.position

    def get_delta_quote(self):
        if self.pnl_in_quote:
            return self.get_position_quote() + self.pnl_in_quote
        else:
            return self.get_position_quote()

    def get_base_ccy_position(self):
        if self.is_inverse:
            if self.position > Decimal("0"):
                return self.position / self.entry_long_price * self.fut_size_multiplier
            elif self.position < Decimal("0"):
                return self.position / self.entry_short_price * self.fut_size_multiplier
            else:
                return Decimal("0")
        else:
            return self.position * self.fut_size_multiplier

    def get_orders_from_one(self, size: Decimal, side: Side):
        orders = []
        if side == Side.BUY:
            if self.short_position > Decimal("0"):
                orders.append({'size': min(size, self.short_position), 'offset': Offset.CLOSE})
                if self.short_position < size:
                    orders.append({'size': size - self.short_position, 'offset': Offset.OPEN})
            else:
                orders.append({'size': size, 'offset': Offset.OPEN})
        elif side == Side.SELL:
            if self.long_position > Decimal("0"):
                orders.append({'size': min(size, self.long_position), 'offset': Offset.CLOSE})
                if self.long_position < size:
                    orders.append({'size': size - self.long_position, 'offset': Offset.OPEN})
            else:
                orders.append({'size': size, 'offset': Offset.OPEN})
        return orders

    def get_orders_from_one_aggressively(self, size: Decimal, side: Side):
        orders = []
        if side == Side.BUY:
            if self.short_position > Decimal("0"):
                if self.short_position >= size:
                    orders.append({'size': size, 'offset': Offset.CLOSE})
                elif size <= self.max_account_position - self.long_position:
                    orders.append({'size': size, 'offset': Offset.OPEN})
                else:
                    orders.append({'size': min(size, self.short_position), 'offset': Offset.CLOSE})
                    if self.short_position < size:
                        orders.append({'size': size - self.short_position, 'offset': Offset.OPEN})
            else:
                orders.append({'size': size, 'offset': Offset.OPEN})
        elif side == Side.SELL:
            if self.long_position > Decimal("0"):
                if self.long_position >= size:
                    orders.append({'size': size, 'offset': Offset.CLOSE})
                elif size <= self.max_account_position - self.short_position:
                    orders.append({'size': size, 'offset': Offset.OPEN})
                else:
                    orders.append({'size': min(size, self.long_position), 'offset': Offset.CLOSE})
                    if self.long_position < size:
                        orders.append({'size': size - self.long_position, 'offset': Offset.OPEN})
            else:
                orders.append({'size': size, 'offset': Offset.OPEN})
        return orders

    def get_orders_from_one_aggressively_im(self, size: Decimal, side: Side, price: Decimal):
        max_account_position = self.inventory_manager.get_available_size(self.global_symbol, side, price)
        orders = []
        if side == Side.BUY:
            if self.short_position > Decimal("0"):
                if self.short_position >= size:
                    orders.append({'size': size, 'offset': Offset.CLOSE})
                elif size <= max_account_position - self.long_position:
                    orders.append({'size': size, 'offset': Offset.OPEN})
                else:
                    orders.append({'size': min(size, self.short_position), 'offset': Offset.CLOSE})
                    if self.short_position < size:
                        orders.append({'size': size - self.short_position, 'offset': Offset.OPEN})
            else:
                orders.append({'size': size, 'offset': Offset.OPEN})
        elif side == Side.SELL:
            if self.long_position > Decimal("0"):
                if self.long_position >= size:
                    orders.append({'size': size, 'offset': Offset.CLOSE})
                elif size <= max_account_position - self.short_position:
                    orders.append({'size': size, 'offset': Offset.OPEN})
                else:
                    orders.append({'size': min(size, self.long_position), 'offset': Offset.CLOSE})
                    if self.long_position < size:
                        orders.append({'size': size - self.long_position, 'offset': Offset.OPEN})
            else:
                orders.append({'size': size, 'offset': Offset.OPEN})
        return orders

    def get_orders_from_multiple(self, orders: List[Dict]):
        '''
        [{'side': Side.XXX, 'price': Decimal('xxx'), 'size': "xxx"}]
        :param orders:
        :return:
        '''
        new_orders = []
        long_position = self.long_position
        short_position = self.short_position

        buy_orders = []
        sell_orders = []
        for order in orders:
            if order['side'] == Side.BUY:
                buy_orders.append(order)
            elif order['side'] == Side.SELL:
                sell_orders.append(order)

        if buy_orders:
            # sort orders by price in desc
            buy_orders.sort(key=lambda x: x['price'], reverse=True)
            for order in buy_orders:
                size = order['size']
                if short_position > Decimal("0"):
                    close_size = min(size, short_position)
                    order.update({'size': close_size, 'offset': Offset.CLOSE})
                    short_position -= close_size
                    size -= close_size
                    new_orders.append(copy.deepcopy(order))
                    if size > Decimal("0"):
                        order.update({'size': size, 'offset': Offset.OPEN})
                        new_orders.append(order)
                else:
                    order.update({'size': size, 'offset': Offset.OPEN})
                    new_orders.append(order)
        if sell_orders:
            # sort orders by price in asc
            sell_orders.sort(key=lambda x: x['price'])
            for order in sell_orders:
                size = order['size']
                if long_position > Decimal("0"):
                    close_size = min(size, long_position)
                    order.update({'size': close_size, 'offset': Offset.CLOSE})
                    long_position -= close_size
                    size -= close_size
                    new_orders.append(copy.deepcopy(order))
                    if size > Decimal("0"):
                        order.update({'size': size, 'offset': Offset.OPEN})
                        new_orders.append(order)
                else:
                    order.update({'size': size, 'offset': Offset.OPEN})
                    new_orders.append(order)
        return new_orders


if __name__ == '__main__':
    symbol_helper = SymbolHelper()
    import asyncio
    loop = asyncio.get_event_loop()
    loop.run_until_complete(symbol_helper.load_symbol_reference())

    #pm = SigSybPMOS('HUOBI_CONTRACT', 'FUTU-BTC/USD-20191227', symbol_helper = symbol_helper, alias='TEST')
    pm = SigSybPM('HUOBI_SWAP', 'SWAP-BTC/USD', symbol_helper=symbol_helper, alias='TEST')
    pm.add_trade(Side.SELL, Decimal("9512.91"), Decimal("1"), offset=Offset.OPEN)
    print(pm.get_unrealized_pnl(Decimal("9500.91")))
    pm.add_trade(Side.BUY, Decimal("9501.36"), Decimal("1"), offset=Offset.CLOSE)
    pm.add_trade(Side.BUY, Decimal("9501.36"), Decimal("1"), offset=Offset.CLOSE)
    #pm.add_trade(Side.BUY, Decimal("9501.36"), Decimal("1"), offset=Offset.OPEN)
    #pm.add_trade(Side.SELL, Decimal("9499.01"), Decimal("1"), offset=Offset.CLOSE)

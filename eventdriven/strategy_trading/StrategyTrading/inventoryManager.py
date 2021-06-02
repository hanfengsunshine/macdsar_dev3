from decimal import Decimal

from strategy_trading.StrategyTrading.symbolHelper import SymbolHelper
import logging
from strategy_trading.StrategyTrading.order import Side, ClientOrder, Offset
from strategy_trading.ExchangeHelper.constants import EXCHANGE_NAME_BINANCE_SWAP, EXCHANGE_NAME_BINANCE_CONTRACT, EXCHANGE_NAME_OKEX_CONTRACT


class InventoryManager():
    def __init__(self, available_balances: dict, exchange, symbol_helper: SymbolHelper, emergent_balances: dict = None):
        self.initial_balances = {ccy: Decimal(size) for ccy, size in available_balances.items()}
        self.available_balances = {ccy: Decimal(size) for ccy, size in available_balances.items()}
        self.frozen_balances = {ccy: Decimal("0") for ccy in available_balances}
        self.exchange_frozen_inventory = {ccy: Decimal("0") for ccy in available_balances}
        if emergent_balances:
            self.emergent_balances = {ccy: Decimal(size) for ccy, size in emergent_balances.items()}
        else:
            self.emergent_balances = {ccy: Decimal("0") for ccy in available_balances}
        self.exchange = exchange

        self.symbol_details = {}
        self.symbol_helper = symbol_helper
        self.position_manager = None
        self.logger = logging.getLogger("IM-{}".format(exchange))

    def _get_symbol_detail(self, global_symbol):
        if global_symbol not in self.symbol_details:
            info = self.symbol_helper.get_info(global_symbol, self.exchange)
            self.symbol_details[global_symbol] = {"base": info['base'], 'quote': info['quote'], "size_multiplier": info['size_multiplier']}

            for ccy in [info['base'], info['quote']]:
                for d in [self.initial_balances, self.available_balances, self.frozen_balances, self.emergent_balances]:
                    if ccy not in d:
                        d[ccy] = Decimal("0")

        return self.symbol_details[global_symbol]

    def update_emergent_balance(self, ccy, size):
        assert size >= Decimal("0")
        self.emergent_balances[ccy] = size
        self.logger.info("EMERGENT: {}: {:.8f}".format(ccy, size))

    def get_available_size(self, global_symbol, side, price: Decimal = 0, use_emergent = False):
        symbol_info = self._get_symbol_detail(global_symbol)
        if side == Side.BUY:
            quote_ccy = symbol_info['quote']
            if use_emergent:
                available_balance = self.available_balances[quote_ccy] / price / symbol_info['size_multiplier']
            else:
                available_balance = (self.available_balances[quote_ccy] - self.emergent_balances[quote_ccy]) / price / symbol_info['size_multiplier']
            return available_balance
        elif side == Side.SELL:
            base_ccy = symbol_info['base']
            if use_emergent:
                available_balance = self.available_balances[base_ccy] / symbol_info['size_multiplier']
            else:
                available_balance = self.available_balances[base_ccy] - self.emergent_balances[base_ccy] / symbol_info['size_multiplier']

            return available_balance

    def bind_position_manager(self, pm):
        assert self.exchange in ['OKEX_SWAP', EXCHANGE_NAME_BINANCE_SWAP]
        self.position_manager = pm

    def print(self):
        self.logger.info("available: {}".format(", ".join(["{}: {:.8f}".format(ccy, size) for ccy, size in self.available_balances.items()])))
        self.logger.info("frozen: {}".format(", ".join(["{}: {:.8f}".format(ccy, size) for ccy, size in self.frozen_balances.items()])))

    def update_exec(self, global_symbol, side, price, size, **kwargs):
        symbol_info = self._get_symbol_detail(global_symbol)
        base_ccy = symbol_info['base']
        quote_ccy = symbol_info['quote']

        if side == Side.BUY:
            self.available_balances[base_ccy] += size
            self.available_balances[quote_ccy] -= size * price

            release_quote = min(size * price, self.frozen_balances[quote_ccy])
            if release_quote > Decimal("0"):
                self.frozen_balances[quote_ccy] -= release_quote
                self.available_balances[quote_ccy] += release_quote
        elif side == Side.SELL:
            self.available_balances[base_ccy] -= size
            self.available_balances[quote_ccy] += size * price

            release_base = min(size, self.frozen_balances[base_ccy])
            if release_base > Decimal("0"):
                self.frozen_balances[base_ccy] -= release_base
                self.available_balances[base_ccy] += release_base
        self.logger.info("AVAI: {}: {:.8f}, {}: {:.8f}".format(base_ccy, self.available_balances[base_ccy], quote_ccy, self.available_balances[quote_ccy]))

    def append_balance(self, ccy, size: Decimal):
        self.logger.info("{} append {}".format(ccy, size))
        if ccy not in self.available_balances:
            self.available_balances[ccy] = size
        else:
            self.available_balances[ccy] += size
        self.print()

    def inplace_balance(self, ccy, size: Decimal):
        self.logger.info("{} changed to {}".format(ccy, size))
        self.available_balances[ccy] = size
        self.print()

    def get_delta(self, ccy):
        return self.available_balances[ccy] - self.initial_balances[ccy]

    def freeze_before_place(self, order: ClientOrder):
        symbol_info = self._get_symbol_detail(order.global_symbol)

        if order.side == Side.BUY:
            self.frozen_balances[symbol_info['quote']] += order.size * order.price
            self.available_balances[symbol_info['quote']] -= order.size * order.price
        elif order.side == Side.SELL:
            self.frozen_balances[symbol_info['base']] += order.size
            self.available_balances[symbol_info['base']] -= order.size

    def release_after_close(self, order: ClientOrder):
        symbol_info = self._get_symbol_detail(order.global_symbol)

        if order.side == Side.BUY:
            release_quote = min(order.remaining_size * order.price, self.frozen_balances[symbol_info['quote']])
            if release_quote > Decimal("0"):
                self.frozen_balances[symbol_info['quote']] -= release_quote
                self.available_balances[symbol_info['quote']] += release_quote
        elif order.side == Side.SELL:
            release_base = min(order.remaining_size, self.frozen_balances[symbol_info['base']])
            if release_base > Decimal("0"):
                self.frozen_balances[symbol_info['base']] -= release_base
                self.available_balances[symbol_info['base']] += release_base

    def exchange_inventory_update_to(self, ccy, size: Decimal):
        if ccy in self.available_balances:
            if self.available_balances[ccy] > size:
                able_to_freeze = self.available_balances[ccy] - size
                self.exchange_frozen_inventory[ccy] += able_to_freeze
                self.available_balances[ccy] -= able_to_freeze
                self.logger.info("freeze {} {:.8f} down to {:.8f}. Locked: {:.8f}".format(ccy, able_to_freeze, size, self.exchange_frozen_inventory[ccy]))
            elif self.available_balances[ccy] < size:
                able_to_release = min(size - self.available_balances[ccy], self.exchange_frozen_inventory[ccy])
                if able_to_release > Decimal("0"):
                    self.exchange_frozen_inventory[ccy] -= able_to_release
                    self.available_balances[ccy] += able_to_release
                    self.logger.info("release {} {:.8f} up to {:.8f}. Locked: {:.8f}".format(ccy, able_to_release, self.available_balances[ccy], self.exchange_frozen_inventory[ccy]))


class HuobiContractInventoryManager(InventoryManager):
    '''
    The class name is not good any more, but we keep it here. It is used on derivative exchanges supporting inverse contracts only
    '''
    def __init__(self, available_balances: dict, exchange, symbol_helper: SymbolHelper, max_leverage = 5, emergent_balances: dict = None):
        assert exchange in ['HUOBI_CONTRACT', 'HUOBI_SWAP']
        super(HuobiContractInventoryManager, self).__init__(available_balances, exchange, symbol_helper, emergent_balances = emergent_balances)
        self.max_leverage = Decimal("{:.8f}".format(float(max_leverage) * 0.95))
        assert max_leverage > Decimal("0")
        assert max_leverage <= Decimal("20")
        self.position_manager = None

    def _get_symbol_detail(self, global_symbol):
        if global_symbol not in self.symbol_details:
            info = self.symbol_helper.get_info(global_symbol, self.exchange)
            self.symbol_details[global_symbol] = info

            for ccy in [info['base'], info['quote']]:
                for d in [self.initial_balances, self.available_balances, self.frozen_balances, self.emergent_balances]:
                    if ccy not in d:
                        d[ccy] = Decimal("0")

        return self.symbol_details[global_symbol]

    def _get_required_margin(self, symbol_info: dict, size, price):
        used_margin = symbol_info['size_multiplier'] * size / price / self.max_leverage
        return used_margin

    def bind_position_manager(self, pm):
        from strategy_trading.StrategyTrading.positionManager import SigSybPMOS
        assert isinstance(pm, SigSybPMOS)
        self.position_manager = pm

    def get_available_size(self, global_symbol, side, price: Decimal = 0, use_emergent = False):
        symbol_info = self._get_symbol_detail(global_symbol)
        if symbol_info['base'] in self.available_balances:
            base_ccy = symbol_info['base']
            if use_emergent:
                available_balance = self.available_balances[base_ccy]
            else:
                available_balance = self.available_balances[base_ccy] - self.emergent_balances[base_ccy]
            available_size = available_balance * self.max_leverage * price / symbol_info['size_multiplier']
            if self.position_manager is not None:
                if side == Side.BUY:
                    return available_size + self.position_manager.short_position
                elif side == Side.SELL:
                    return available_size + self.position_manager.long_position
            return available_size
        else:
            return Decimal("0")

    def get_required_inventory(self, new_order_size, global_symbol, side, price: Decimal = 0):
        available_size = self.get_available_size(global_symbol, side, price = price)
        if new_order_size <= available_size:
            return Decimal("0")
        else:
            symbol_info = self._get_symbol_detail(global_symbol)
            return (new_order_size - available_size) * symbol_info['size_multiplier'] / price / self.max_leverage

    def update_exec(self, global_symbol, side, price, size, offset = Offset.OPEN, **kwargs):
        symbol_info = self._get_symbol_detail(global_symbol)
        base_ccy = symbol_info['base']

        used_margin = self._get_required_margin(symbol_info, size, price)
        if offset == Offset.OPEN:
            used_margin = max(min(used_margin, self.available_balances[base_ccy]), Decimal("0"))
            self.available_balances[base_ccy] -= used_margin
            self.frozen_balances[base_ccy] += used_margin
        elif offset == Offset.CLOSE:
            used_margin = max(min(used_margin, self.frozen_balances[base_ccy]), Decimal("0"))
            self.available_balances[base_ccy] += used_margin
            self.frozen_balances[base_ccy] -= used_margin

        self.logger.info("AVAI: {}: {:.8f}".format(base_ccy, self.available_balances[base_ccy]))

    def get_delta(self, ccy):
        pass

    def freeze_before_place(self, order: ClientOrder):
        symbol_info = self._get_symbol_detail(order.global_symbol)

        used_margin = self._get_required_margin(symbol_info, order.size, order.price)
        self.available_balances[symbol_info['base']] -= used_margin
        self.frozen_balances[symbol_info['base']] += used_margin

    def release_after_close(self, order: ClientOrder):
        symbol_info = self._get_symbol_detail(order.global_symbol)

        release_margin = self._get_required_margin(symbol_info, order.remaining_size, order.price)
        release_margin = min(release_margin, self.available_balances[symbol_info['base']])

        if release_margin > Decimal("0"):
            self.frozen_balances[symbol_info['base']] -= release_margin
            self.available_balances[symbol_info['base']] += release_margin


class MixedDeriInventoryManager(InventoryManager):
    def __init__(self, available_balances: dict, exchange, symbol_helper: SymbolHelper, max_leverage = 5, emergent_balances: dict = None):
        assert exchange in ['OKEX_SWAP', EXCHANGE_NAME_BINANCE_SWAP, EXCHANGE_NAME_BINANCE_CONTRACT, EXCHANGE_NAME_OKEX_CONTRACT]
        super(MixedDeriInventoryManager, self).__init__(available_balances, exchange, symbol_helper, emergent_balances = emergent_balances)
        self.max_leverage = Decimal("{:.8f}".format(float(max_leverage) * 0.95))
        assert max_leverage > Decimal("0")
        assert max_leverage <= Decimal("20")
        self.position_manager = {}

    def _get_symbol_detail(self, global_symbol):
        if global_symbol not in self.symbol_details:
            info = self.symbol_helper.get_info(global_symbol, self.exchange)
            self.symbol_details[global_symbol] = info

            for ccy in [info['base'], info['quote']]:
                for d in [self.initial_balances, self.available_balances, self.frozen_balances, self.emergent_balances]:
                    if ccy not in d:
                        d[ccy] = Decimal("0")

        return self.symbol_details[global_symbol]

    def _get_required_margin(self, symbol_info: dict, size, price):
        if symbol_info['quote_as_margin']:
            used_margin = symbol_info['size_multiplier'] * size * price / self.max_leverage
        else:
            used_margin = symbol_info['size_multiplier'] * size / price / self.max_leverage
        return used_margin

    def bind_position_manager(self, pm):
        from strategy_trading.StrategyTrading.positionManager import SigSybPM
        assert isinstance(pm, SigSybPM)
        self.position_manager[pm.global_symbol] = pm

    def get_available_size(self, global_symbol, side, price: Decimal = 0, use_emergent = False):
        symbol_info = self._get_symbol_detail(global_symbol)
        if not symbol_info['quote_as_margin']:
            base_ccy = symbol_info['base']
            if base_ccy in self.available_balances:
                if use_emergent:
                    available_balance = self.available_balances[base_ccy]
                else:
                    available_balance = self.available_balances[base_ccy] - self.emergent_balances[base_ccy]
                available_size = available_balance * self.max_leverage * price / symbol_info['size_multiplier']
                if self.position_manager.get(global_symbol, None) is not None:
                    if side == Side.BUY:
                        return available_size + max(- self.position_manager.get(global_symbol).position, Decimal("0"))
                    elif side == Side.SELL:
                        return available_size + max(self.position_manager.get(global_symbol).position, Decimal("0"))
                return available_size
            else:
                return Decimal("0")
        else:
            quote_ccy = symbol_info['quote']
            if quote_ccy in self.available_balances:
                if use_emergent:
                    available_balance = self.available_balances[quote_ccy]
                else:
                    available_balance = self.available_balances[quote_ccy] - self.emergent_balances[quote_ccy]
                available_size = available_balance * self.max_leverage / price / symbol_info['size_multiplier']
                if self.position_manager.get(global_symbol, None) is not None:
                    if side == Side.BUY:
                        return available_size + max(- self.position_manager.get(global_symbol).position, Decimal("0"))
                    elif side == Side.SELL:
                        return available_size + max(self.position_manager.get(global_symbol).position, Decimal("0"))
                return available_size
            else:
                return Decimal("0")

    def get_required_inventory(self, new_order_size, global_symbol, side, price: Decimal = 0):
        available_size = self.get_available_size(global_symbol, side, price = price)
        if new_order_size <= available_size:
            return Decimal("0")
        else:
            symbol_info = self._get_symbol_detail(global_symbol)
            if not symbol_info['quote_as_margin']:
                return (new_order_size - available_size) * symbol_info['size_multiplier'] / price / self.max_leverage
            else:
                return (new_order_size - available_size) * symbol_info['size_multiplier'] * price / self.max_leverage

    def update_exec(self, global_symbol, side, price, size, offset = Offset.OPEN, **kwargs):
        symbol_info = self._get_symbol_detail(global_symbol)
        margin_ccy = symbol_info['pnl_ccy']

        used_margin = self._get_required_margin(symbol_info, size, price)
        if offset == Offset.OPEN:
            used_margin = max(min(used_margin, self.available_balances[margin_ccy]), Decimal("0"))
            self.available_balances[margin_ccy] -= used_margin
            self.frozen_balances[margin_ccy] += used_margin
        elif offset == Offset.CLOSE:
            used_margin = max(min(used_margin, self.frozen_balances[margin_ccy]), Decimal("0"))
            self.available_balances[margin_ccy] += used_margin
            self.frozen_balances[margin_ccy] -= used_margin

        self.logger.info("AVAI: {}: {:.8f}".format(margin_ccy, self.available_balances[margin_ccy]))

    def get_delta(self, ccy):
        pass

    def freeze_before_place(self, order: ClientOrder):
        symbol_info = self._get_symbol_detail(order.global_symbol)

        used_margin = self._get_required_margin(symbol_info, order.size, order.price)
        pnl_ccy = symbol_info['pnl_ccy']
        self.available_balances[pnl_ccy] -= used_margin
        self.frozen_balances[pnl_ccy] += used_margin

    def release_after_close(self, order: ClientOrder):
        symbol_info = self._get_symbol_detail(order.global_symbol)
        pnl_ccy = symbol_info['pnl_ccy']

        release_margin = self._get_required_margin(symbol_info, order.remaining_size, order.price)
        release_margin = min(release_margin, self.available_balances[pnl_ccy])

        if release_margin > Decimal("0"):
            self.frozen_balances[pnl_ccy] -= release_margin
            self.available_balances[pnl_ccy] += release_margin
from strategy_trading.ExchangeHelper.constants import EXCHANGE_NAME_BINANCE_SWAP
from strategy_trading.STUtils.dbengine import get_db_engine, read_sql
import asyncio
from decimal import Decimal
from datetime import datetime, timedelta
from strategy_trading.ExchangeHelper.constants import EXCHANGE_NAME_BINANCE_SWAP, EXCHANGE_NAME_BINANCE_CONTRACT, EXCHANGE_NAME_OKEX_CONTRACT


class SymbolHelper():
    def __init__(self):
        self.market_info = {}
        self.market_info_by_exchange_symbol = {}

    async def load_symbol_reference(self):
        # TODO: only load specific exchanges' settings
        engine = get_db_engine("data", "trading")
        markets = await read_sql("select * from symbol_reference", engine)
        for market in markets:
            # standardize the float
            for col in ['size_multiplier', 'tick_size', 'min_order_size', 'min_order_size_in_value', 'order_size_incremental']:
                if market[col] is not None:
                    market[col] = Decimal("{:.12f}".format(float(market[col]))).normalize()

            if market['size_multiplier'] is None:
                market['size_multiplier'] = Decimal("1")
            if market['min_order_size_in_value'] is None:
                market['min_order_size_in_value'] = Decimal("0")

            if market['exchange'] in ['HUOBI_SPOT', 'OKEX_SPOT']:
                market['base_as_margin'] = False
                market['quote_as_margin'] = False
                market['pnl_ccy'] = market['price_ccy']
            elif market['exchange'] in ['HUOBI_SWAP', 'HUOBI_CONTRACT', EXCHANGE_NAME_BINANCE_CONTRACT]:
                market['base_as_margin'] = True
                market['quote_as_margin'] = False
                market['pnl_ccy'] = market['price_ccy']
            elif market['exchange'] in ['OKEX_SWAP', EXCHANGE_NAME_OKEX_CONTRACT]:
                if market['price_quote_ccy'] == 'USD':
                    market['base_as_margin'] = True
                    market['quote_as_margin'] = False
                    market['pnl_ccy'] = market['price_ccy']
                elif market['price_quote_ccy'] == 'USDT':
                    market['base_as_margin'] = False
                    market['quote_as_margin'] = True
                    market['pnl_ccy'] = market['price_quote_ccy']
                else:
                    raise Exception
            elif market['exchange'] == EXCHANGE_NAME_BINANCE_SWAP:
                market['base_as_margin'] = False
                market['quote_as_margin'] = True
                market['pnl_ccy'] = market['price_quote_ccy']

            if market['exchange'] not in self.market_info:
                self.market_info[market['exchange']] = {}
            self.market_info[market['exchange']][market['global_symbol']] = market

            if market['exchange'] not in self.market_info_by_exchange_symbol:
                self.market_info_by_exchange_symbol[market['exchange']] = {}
            market['symbol'] = market['exchange_symbol']
            market['base'] = market['price_ccy']
            market['quote'] = market['price_quote_ccy']
            self.market_info_by_exchange_symbol[market['exchange']][market['symbol']] = market

        #print(self.market_info)

    def get_info(self, global_symbol, exchange):
        return self.market_info[exchange][global_symbol]

    def get_info_from_symbol(self, symbol, exchange):
        return self.market_info_by_exchange_symbol[exchange][symbol]

    def is_inverse_contract(self, global_symbol, exchange):
        info = self.get_info(global_symbol, exchange)
        if info['symbol_type'] in ['CONTRACT', 'FUTURES', 'PERPETUAL SWAP']:
            if info['price_quote_ccy'] == info['size_ccy']:
                return True
        return False

    def is_inverse_contract_from_symbol(self, symbol, exchange):
        info = self.get_info_from_symbol(symbol, exchange)
        if info['symbol_type'] in ['CONTRACT', 'FUTURES', 'PERPETUAL SWAP']:
            if info['price_quote_ccy'] == info['size_ccy']:
                return True
        return False

    def get_huobi_contract_symbol(self, global_symbol, curr_time: datetime = None):
        if curr_time is None:
            curr_time = datetime.utcnow()

        info = self.get_info(global_symbol, 'HUOBI_CONTRACT')
        remaining_time = info['expiry_datetime'] - curr_time
        if remaining_time <= timedelta(seconds=0):
            raise ValueError("cannot get huobi native md symbol for {}".format(global_symbol))
        if remaining_time <= timedelta(days=7):
            return "{}_CW".format(info['price_ccy'])
        elif remaining_time <= timedelta(days=14):
            return "{}_NW".format(info['price_ccy'])
        return "{}_CQ".format(info['price_ccy'])

    def is_contract(self, global_symbol, exchange):
        info = self.get_info(global_symbol, exchange)
        if info['symbol_type'] in ['CONTRACT', 'FUTURES', 'PERPETUAL SWAP']:
            return True
        return False

    def is_swap(self, global_symbol, exchange):
        info = self.get_info(global_symbol, exchange)
        if info['symbol_type'] in ['PERPETUAL SWAP']:
            return True
        return False

    def is_future(self, global_symbol, exchange):
        info = self.get_info(global_symbol, exchange)
        if info['symbol_type'] in ['FUTURES']:
            return True
        return False

    def get_all_global_symbols_in_quote(self, exchange, quote_ccy):
        global_symbols = []
        for global_symbol, info in self.market_info[exchange].items():
            if info['quote'] == quote_ccy:
                global_symbols.append(global_symbol)
        return global_symbols


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    sh = SymbolHelper()
    loop.run_until_complete(sh.load_symbol_reference())
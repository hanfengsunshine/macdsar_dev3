from enum import Enum
from loguru import logger
from decimal import Decimal
from collections import defaultdict
from sqlalchemy import create_engine

from strategy_trading.easy_strategy.singleton import Singleton
from strategy_trading.easy_strategy.utils import normalize_fraction


class Instrument(object):
    def __init__(self,
        exchange: str,
        global_symbol: str,
        exchange_symbol: str,
        symbol_type: str,
        price_tick: Decimal = None,
        lot_size: Decimal = None,
        min_order_size: Decimal = None,
        min_order_notional: Decimal = None,
        multiplier: Decimal = None,
        base_currency: str = None,
        quote_currency: str = None):
        self.exchange = exchange
        self.global_symbol = global_symbol
        self.exchange_symbol = exchange_symbol
        self.full_symbol = '%s.%s' % (exchange, global_symbol)
        self.symbol_type = symbol_type
        self.price_tick = price_tick
        self.price_precision = len(str(price_tick).split('.')[-1].rstrip('0'))
        self.lot_size = lot_size
        self.quantity_precision = len(str(lot_size).split('.')[-1].rstrip('0'))
        self.min_order_size = min_order_size
        self.min_order_notional = min_order_notional
        self.multiplier = multiplier
        self.base_currency = base_currency
        self.quote_currency = quote_currency
        self.is_traded_in_notional = self.symbol_type in ['CONTRACT', 'FUTURES', 'PERPETUAL SWAP'] and self.quote_currency == self.base_currency
        self.is_twoway_position = self.symbol_type in ['CONTRACT', 'FUTURES', 'PERPETUAL SWAP'] and ('OKEX' in self.exchange or 'HUOBI' in self.exchange)


class InstrumentManager(metaclass=Singleton):
    def __init__(self):
        self._instruments = dict()
        self._exchange_instruments = dict()
        self._mds_instruments = dict()

    def init(self, config):
        engine = create_engine(config['database'])
        with engine.connect() as connection:
            result = connection.execute("select exchange, global_symbol, exchange_symbol, \
                symbol_type, tick_size, order_size_incremental, min_order_size, min_order_size_in_value, \
                size_multiplier, price_quote_ccy, size_ccy from symbol_reference")
            for row in result:
                row = dict(zip(row.keys(), row))
                if row['exchange'] == 'BINANCE_SWAP' and row['price_quote_ccy'] == 'USDT':
                    continue
                try:
                    instrument = Instrument(
                        exchange=row['exchange'],
                        global_symbol=row['global_symbol'],
                        exchange_symbol=row['exchange_symbol'],
                        symbol_type=row['symbol_type'],
                        price_tick=normalize_fraction(Decimal(str(row['tick_size']))),
                        lot_size=normalize_fraction(Decimal(str(row['order_size_incremental']))),
                        min_order_size=normalize_fraction(Decimal(str(row['min_order_size']))),
                        min_order_notional=normalize_fraction(Decimal(str(row['min_order_size_in_value']))) if row['min_order_size_in_value'] else Decimal('0'),
                        multiplier=normalize_fraction(Decimal(str(row['size_multiplier']))) if row['size_multiplier'] else Decimal('1'),
                        base_currency=row['price_quote_ccy'],
                        quote_currency=row['size_ccy'])
                except:
                    if row['symbol_type'] == 'INDEX':
                        instrument = Instrument(
                            exchange=row['exchange'],
                            global_symbol=row['global_symbol'],
                            exchange_symbol=row['exchange_symbol'],
                            symbol_type=row['symbol_type']
                        )
                    else:
                        continue
                self._instruments[(row['exchange'], row['global_symbol'])] = instrument
                self._exchange_instruments[(row['exchange'], row['exchange_symbol'])] = instrument
                self._mds_instruments['{}#{}#{}'.format(row['exchange'], instrument.symbol_type.split(' ')[-1], row['exchange_symbol'])] = instrument

    def get_instrument(self, exchange: str, global_symbol: str):
        symbol_key = (exchange, global_symbol)
        return self._instruments[symbol_key] if symbol_key in self._instruments else None

    def get_instrument_by_exchange_symbol(self, exchange: str, exchange_symbol: str):
        symbol_key = (exchange, exchange_symbol)
        return self._exchange_instruments[symbol_key] if symbol_key in self._exchange_instruments else None

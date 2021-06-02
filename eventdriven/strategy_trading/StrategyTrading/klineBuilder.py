from strategy_trading.StrategyTrading.marketData import MarketTrade, Kline
import pandas
from datetime import datetime, timedelta
from decimal import Decimal
from sortedcontainers import SortedDict
import asyncio
import logging
from strategy_trading.StrategyTrading.symbolHelper import SymbolHelper
import random
from strategy_trading.ExchangeHelper.huobiRestHelper import get_accurate_huobi_fut_klines, get_klines, get_swap_klines


class KlineBuilder():
    def __init__(self, exchange, global_symbol, freq_seconds, init_klines: list, max_length: int, symbol_helper: SymbolHelper = None, shift_seconds = 0, curr_time = None, timezone = 'UTC'):
        self.exchange = exchange
        self.global_symbol = global_symbol
        self.freq_seconds = freq_seconds
        self.shift_seconds = shift_seconds
        self.klines = SortedDict()
        self.max_length = max_length
        self.logger = logging.getLogger("KB|{}|{}|{}|{}".format(exchange, global_symbol, freq_seconds, shift_seconds))
        self.symbol_helper = symbol_helper
        self.is_inverse = symbol_helper.is_inverse_contract(global_symbol, exchange)
        self.symbol = symbol_helper.get_info(self.global_symbol, self.exchange)['symbol']
        if self.is_inverse:
            self.logger.warning("exchange not recognized. the volume and amount might be inaccurate")
            self.size_multiplier = float(symbol_helper.get_info(global_symbol, exchange)['size_multiplier'])
        self.timezone = timezone
        self._reset_current_window(curr_time)
        if init_klines:
            self.on_klines(init_klines)
        self.kline_adjustment_opened = False

        self.update_kline_sync_freq_seconds()

    def _reset_current_window(self, curr_time = None):
        curr_time = self.get_recent_window_start(curr_time)
        self._curr_window = {
            "open": None,
            "high": None,
            "low": None,
            "close": None,
            "volume": 0,
            "amount": 0,
            "count": 0,
            "time": curr_time
        }

    def get_recent_window_start(self, curr_time = None):
        if curr_time is None:
            if self.timezone == 'UTC':
                curr_time = datetime.utcnow()
            elif self.timezone == 'HKT':
                curr_time = datetime.utcnow() + timedelta(hours=8)
        window_start = (pandas.to_datetime(curr_time).floor("{}s".format(self.freq_seconds))).to_pydatetime() + timedelta(seconds=self.shift_seconds)
        if window_start > curr_time:
            window_start -= timedelta(seconds=self.freq_seconds)
        return window_start

    def update_trade(self, trade: MarketTrade):
        trade_size = float(trade.size)
        trade_price = float(trade.price)
        # append size
        if self._curr_window['close'] is None:
            self._curr_window['open'] = trade_price
            self._curr_window['high'] = trade_price
            self._curr_window['low'] = trade_price
        else:
            if self._curr_window['high'] < trade_price:
                self._curr_window['high'] = trade_price
            if self._curr_window['low'] > trade_price:
                self._curr_window['low'] = trade_price
        self._curr_window['close'] = trade_price
        if not self.is_inverse:
            self._curr_window['volume'] += trade_size
            self._curr_window['amount'] += trade_size * trade_price
        else:
            self._curr_window['volume'] += trade_size / trade_price * self.size_multiplier
            self._curr_window['amount'] += trade_size
        self._curr_window['count'] += 1

    def get_klines(self):
        return self.klines

    def _print(self, _time, kline):
        self.logger.info("KLINE {}: {}|{}|{}|{} {}|{}|{}".format(_time.strftime("%Y-%m-%d_%H:%M:%S"),
                                                                 kline['open'],
                                                                 kline['high'],
                                                                 kline['low'],
                                                                 kline['close'],
                                                                 kline['volume'],
                                                                 kline['amount'],
                                                                 kline['count']))
    
    def finalize_last_window(self):
        self.klines[self._curr_window['time']] = self._curr_window
        self._print(self._curr_window['time'], self._curr_window)
        while len(self.klines) > self.max_length:
            self.klines.popitem(0)

    def on_klines(self, klines: list, replace_curr_window = True):
        # Here allows REST klines to adjust local klines
        for kline in klines:
            if kline['time'] < self._curr_window['time']:
                self.klines[kline['time']] = kline
                self._print(kline['time'], kline)
            elif kline['time'] == self._curr_window['time'] and replace_curr_window:
                self._curr_window = kline
                self._print(kline['time'], kline)
        while len(self.klines) > self.max_length:
            self.klines.popitem(0)

    def _kline_to_dict(self, kline: Kline):
        return {
            'open': float(kline.open_price),
            'high': float(kline.high),
            'low': float(kline.low),
            'close': float(kline.close_price),
            'volume': float(kline.volume),
            'amount': float(kline.amount),
            'count': float(kline.count)
        }

    def on_kline(self, kline: Kline):
        if self.shift_seconds != 0:
            self.logger.warning("there should be no klines form exch when shift_seconds = {}".format(self.shift_seconds))
            return
        _time = datetime.utcfromtimestamp(kline.start_timestamp / 1000000)
        if self.timezone == 'HKT':
            _time += timedelta(hours=8)

        # official klines should not be affecting the current kline
        if _time < self._curr_window['time']:
            if (_time not in self.klines and (len(self.klines) < self.max_length or _time > min(self.klines.keys()))) \
                    or (_time in self.klines):
                kline_dict = self._kline_to_dict(kline)
                kline_dict['time'] = _time
                self.klines[_time] = kline_dict
                self._print(_time, kline_dict)

    async def auto_refresh_when_next_window(self, on_kline_update, adjust_by_rest_first = False):
        while True:
            curr_time = datetime.utcnow()
            if self.timezone == 'HKT':
                curr_time += timedelta(hours=8)
            try:
                next_window_time = self._curr_window['time'] + timedelta(seconds=self.freq_seconds)
                time_diff_seconds = (next_window_time - curr_time) / timedelta(seconds=1)
                if time_diff_seconds > 0:
                    await asyncio.sleep(time_diff_seconds)
                self.finalize_last_window()
                self._reset_current_window(next_window_time)

                if adjust_by_rest_first:
                    await self._adjust_klines_from_rest()

                if on_kline_update is not None:
                    # if self.klines:
                    #     self.logger.info("latest kline: {}".format(self.klines.peekitem(-1)))
                    on_kline_update(self.klines)
            except Exception as e:
                self.logger.exception(e)
                await asyncio.sleep(0.1)

    def _resample(self, klines, freq_seconds, shift_seconds):
        if freq_seconds % 60 == 0 and shift_seconds % 60 == 0:
            raw_df = pandas.DataFrame(klines)
            raw_df['time'] = raw_df['time'] - pandas.Timedelta('{}s'.format(shift_seconds))
            raw_df.set_index(raw_df['time'], inplace=True)
            df = raw_df.resample('{}s'.format(freq_seconds), how = {'open': 'first', 'high': 'max', 'low': 'min', 'close': 'last', 'volume': 'sum', 'amount': 'sum', 'count': 'sum'})
            df['time'] = df.index
            df['time'] = df['time'] + pandas.Timedelta('{}s'.format(shift_seconds))
            return list(df.T.to_dict().values())

    def update_kline_sync_freq_seconds(self):
        self.rest_freq_seconds = None

        supported_freq_seconds = [60, 300, 900, 1800, 3600, 3600 * 4, 3600 * 24]
        if not (self.exchange in ('HUOBI_SPOT', 'HUOBI_CONTRACT', 'HUOBI_SWAP', 'BINANCE_SWAP', 'BINANCE_SPOT') and (self.freq_seconds in supported_freq_seconds or self.freq_seconds % 60 == 0)):
            self.logger.warning("{}/{} is not supported".format(self.exchange, self.freq_seconds))
            return

        if self.shift_seconds % 60 != 0:
            self.logger.warning("shift_seconds {} not supported".format(self.shift_seconds))
            return

        freq_seconds = self.freq_seconds if self.shift_seconds == 0 else self.shift_seconds  # there might be bugs.
        if freq_seconds not in supported_freq_seconds:
            able_resample_freq = []
            for tmp_seconds in supported_freq_seconds:
                if tmp_seconds < freq_seconds and freq_seconds % tmp_seconds == 0:
                    able_resample_freq.append(tmp_seconds)
            if not able_resample_freq:
                self.logger.warning(
                    "cannot find any supported freq in order to resample to {}".format(self.freq_seconds))
                return
            freq_seconds = max(able_resample_freq)
        self.rest_freq_seconds = freq_seconds

    async def _adjust_klines_from_rest(self):
        if self.rest_freq_seconds is None:
            return

        try:
            curr_time = datetime.utcnow()
            if self.timezone == 'HKT':
                curr_time += timedelta(hours=8)
            if self.exchange == 'HUOBI_SPOT':
                klines = await get_klines(self.symbol, self.rest_freq_seconds, ret_dataframe=False, timezone=self.timezone, limit = 5 * (self.freq_seconds // self.rest_freq_seconds))
            elif self.exchange == 'HUOBI_CONTRACT':
                klines = await get_accurate_huobi_fut_klines(self.global_symbol, self.rest_freq_seconds, ret_dataframe=False, timezone=self.timezone, limit = 5 * (self.freq_seconds // self.rest_freq_seconds))
            elif self.exchange == 'HUOBI_SWAP':
                klines = await get_swap_klines(self.symbol, self.rest_freq_seconds, ret_dataframe=False, timezone=self.timezone, limit=5 * (self.freq_seconds // self.rest_freq_seconds))
            else:
                return
            if self.rest_freq_seconds != self.freq_seconds:
                adjusted_klines = self._resample(klines, self.freq_seconds, self.shift_seconds)
            else:
                adjusted_klines = klines
            adjusted_klines = adjusted_klines[1:]
            # filter kline whose end_time > curr_time
            adjusted_klines = [kline for kline in adjusted_klines if kline['time'] + timedelta(seconds=self.freq_seconds) < curr_time]
            self.on_klines(adjusted_klines, replace_curr_window=False)
            self.logger.info("kline adjusted by REST {}".format(self.rest_freq_seconds))
        except Exception as e:
            self.logger.exception(e)

    async def adjust_klines_from_rest(self):
        if self.kline_adjustment_opened:
            self.logger.warning("kline_adjustment_opened = true already")
            return

        self.kline_adjustment_opened = True

        while True:
            sleep_seconds = random.randint(int(max(1, self.freq_seconds * 0.8)), int(self.freq_seconds * 1.2))
            await asyncio.sleep(sleep_seconds)
            try:
                await self._adjust_klines_from_rest()
            except Exception as e:
                self.logger.exception(e)
                await asyncio.sleep(60)


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    sh = SymbolHelper()
    loop.run_until_complete(sh.load_symbol_reference())

    builder = KlineBuilder('HUOBI_SPOT', 'SPOT-BTC/USDT', 20 * 60, [], 100, symbol_helper = sh, shift_seconds=600)
    loop.run_until_complete(builder.adjust_klines_from_rest())

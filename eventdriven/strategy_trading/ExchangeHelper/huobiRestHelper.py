import aiohttp
import pandas
from datetime import datetime, timedelta
import asyncio
import json
from strategy_trading.STUtils.retryDeco import retry
from datetime import time as dt_time


freq_seconds_to_freq = {
    60: '1min',
    300: '5min',
    900: '15min',
    1800: '30min',
    3600: '60min',
    3600 * 4: '4hour',
    3600 * 24: '1day'
}


def get_next_last_friday_of_one_quarter(curr_time: datetime, _time: dt_time):
    # get friday, one week ago, at 16 o'clock
    last_friday = (curr_time.date() - timedelta(days=curr_time.weekday()) + timedelta(days=4, weeks=-1))
    last_friday_at_16 = datetime.combine(last_friday, _time)

    # if today is also friday, and after 16 o'clock, change to the current date
    one_week = timedelta(weeks=1)
    while (last_friday_at_16 + one_week).month == last_friday_at_16.month or last_friday_at_16 <= curr_time:
        last_friday_at_16 += one_week
    if last_friday_at_16.month % 3 != 0:
        return get_next_last_friday_of_one_quarter(last_friday_at_16, _time)
    return last_friday_at_16


def get_next_friday(curr_time: datetime, _time: dt_time):
    last_friday = (curr_time.date() - timedelta(days=curr_time.weekday()) + timedelta(days=4, weeks=-1))
    last_friday_at_16 = datetime.combine(last_friday, _time)

    one_week = timedelta(weeks=1)
    while last_friday_at_16 + one_week <= curr_time:
        last_friday_at_16 += one_week
    last_friday_at_16 += one_week
    return last_friday_at_16


@retry
async def get_klines(exchange_symbol, freq_seconds, limit = 2000, ret_dataframe = True, timezone = 'HKT'):
    # freq_seconds to freq
    assert freq_seconds in freq_seconds_to_freq
    freq = freq_seconds_to_freq[freq_seconds]
    limit = min(limit, 2000)
    assert limit > 0

    if exchange_symbol[-3:] in ['_CQ', '_CW', '_NW']: # contract
        url = 'https://api.hbdm.com/market/history/kline?period=%s&size=%s&symbol=%s' % (freq, limit, exchange_symbol) # use huobi pro REST ipo directly
    else: # spot
        url = 'https://api.huobi.pro/market/history/kline?period=%s&size=%s&symbol=%s' % (freq, limit, exchange_symbol) # use huobi pro REST ipo directly

    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            resp = await response.text()

    tmp_data = json.loads(resp)['data']

    for data in tmp_data:
        data['volume'] = data.pop('amount')
        data['amount'] = data.pop('vol')

    if ret_dataframe:
        kline_df = pandas.DataFrame.from_dict(tmp_data)
        kline_df.index = pandas.to_datetime(kline_df['id'], unit="s", utc=False)
        if timezone == 'HKT':
            kline_df.index += timedelta(hours=8) # time was in UTC, change to HKT
        kline_df.sort_index(inplace=True)
        return kline_df
    else:
        if timezone == 'UTC':
            for kline in tmp_data:
                kline['time'] = datetime.utcfromtimestamp(kline.pop('id'))
        elif timezone == 'HKT':
            for kline in tmp_data:
                kline['time'] = datetime.utcfromtimestamp(kline.pop('id')) + timedelta(hours=8)
        return tmp_data


@retry
async def get_swap_klines(exchange_symbol, freq_seconds, limit = 2000, ret_dataframe = True, timezone = 'HKT'):
    # freq_seconds to freq
    assert freq_seconds in freq_seconds_to_freq
    freq = freq_seconds_to_freq[freq_seconds]
    limit = min(limit, 2000)
    assert limit > 0

    url = 'https://api.hbdm.com/swap-ex/market/history/kline?contract_code={}&period={}&size={}'.format(exchange_symbol, freq, limit) # use huobi pro REST ipo directly

    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            resp = await response.text()

    tmp_data = json.loads(resp)['data']

    for data in tmp_data:
        data['volume'] = data.pop('amount')
        data['amount'] = data.pop('vol')

    if ret_dataframe:
        kline_df = pandas.DataFrame.from_dict(tmp_data)
        kline_df.index = pandas.to_datetime(kline_df['id'], unit="s", utc=False)
        if timezone == 'HKT':
            kline_df.index += timedelta(hours=8) # time was in UTC, change to HKT
        kline_df.sort_index(inplace=True)
        return kline_df
    else:
        if timezone == 'UTC':
            for kline in tmp_data:
                kline['time'] = datetime.utcfromtimestamp(kline.pop('id'))
        elif timezone == 'HKT':
            for kline in tmp_data:
                kline['time'] = datetime.utcfromtimestamp(kline.pop('id')) + timedelta(hours=8)
        return tmp_data


async def get_accurate_huobi_fut_klines(global_symbol, freq_seconds, limit = 2000, ret_dataframe = True, timezone = 'HKT'):
    assert timezone in ['HKT', 'UTC']

    def update_global_symbol(df, symbol):
        if symbol[-2:] == 'CW':
            df['expiry'] = df['start_datetime'].apply(lambda x: get_next_friday(x, dt_time(hour=8)))
        elif symbol[-2:] == 'NW':
            df['expiry'] = df['start_datetime'].apply(lambda x: get_next_friday(x, dt_time(hour=8)) + timedelta(days=7))
        elif symbol[-2:] == 'CQ':
            df['expiry'] = df['start_datetime'].apply(lambda x: get_next_last_friday_of_one_quarter(x, dt_time(hour=8)))
            next_expiry_indexes = df[df['start_datetime'] >= df['expiry'] - timedelta(days=14)].index
            df.loc[next_expiry_indexes, 'expiry'] = df.loc[next_expiry_indexes, 'start_datetime'].apply(lambda x: get_next_last_friday_of_one_quarter(x + timedelta(days=30), dt_time(hour=8)))
        ccy = symbol.split("_")[0]
        df['global_symbol'] = df['expiry'].apply(lambda x: "FUTU-{}/USD-{}".format(ccy, x.strftime("%Y%m%d")))
        df.drop('expiry', inplace=True, axis=1)

    ccy = global_symbol.split('-')[1].split('/')[0]
    cleaned_klines = pandas.DataFrame()

    for contract_type in ['CW', 'NW', 'CQ']:
        contract = "{}_{}".format(ccy, contract_type)
        klines = await get_klines(contract, freq_seconds, limit = 2000, ret_dataframe = True, timezone = 'UTC')

        if not klines.empty:
            klines['start_datetime'] = klines.index
            update_global_symbol(klines, contract)
            klines.drop(klines[klines['global_symbol'] != global_symbol].index, inplace=True)
            if not klines.empty:
                cleaned_klines = pandas.concat([cleaned_klines, klines])

    if not cleaned_klines.empty:
        cleaned_klines.sort_index(inplace=True)
        cleaned_klines = cleaned_klines.tail(limit)

        if not cleaned_klines.empty:
            if 'start_datetime' in cleaned_klines.columns:
                cleaned_klines.drop('start_datetime', axis = 1, inplace = True)

        if timezone == 'HKT':
            cleaned_klines.index += timedelta(hours=8)

        if not ret_dataframe:
            cleaned_klines['time'] = cleaned_klines.index
            cleaned_klines = list(cleaned_klines.T.to_dict().values())
        return cleaned_klines
    else:
        if ret_dataframe:
            return pandas.DataFrame()
        else:
            return []


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    #loop.run_until_complete(get_klines('btcusdt', 60, ret_dataframe=False))
    loop.run_until_complete(get_accurate_huobi_fut_klines("FUTU-BTC/USD-20191213", 60, limit = 2000, ret_dataframe = False, timezone = 'UTC'))

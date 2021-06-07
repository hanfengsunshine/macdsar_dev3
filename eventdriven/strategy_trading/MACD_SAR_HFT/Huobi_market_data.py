from websocket import create_connection
import gzip
import json
import pandas as pd

import json,urllib,time,math
from urllib.parse import urlencode
import urllib.request as urllib
import urllib.request
import json
import talib
import pandas as pd
import numpy as np
#import utils.utils as utils
import matplotlib.pyplot as plt
import datetime as dt
from talib import abstract



import time
from datetime import datetime

# macdhist = MACD(close, fastperiod=12, slowperiod=26, signalperiod=9)


def Huobi_kline(symbol, period='1min', client_id='huobi_id',
                start_time=None, end_time=None, is_fr=False,
                get_full_market_data=False, adjust_time=True,
                col_with_asset_name=False):
    # 用来确定websocket行情请求地址
    if symbol[-2:] in {'CW', 'CQ', 'NW', 'NQ'}:
        ws_add = 'wss://api.hbdm.vn/ws'
        symbol_name = ''.join(symbol.split('_'))
    elif symbol[-3:] == 'USD':
        if is_fr:
            ws_add = 'wss://api.hbdm.vn/ws_index'
        else:
            ws_add = 'wss://api.hbdm.vn/swap-ws'
        symbol_name = symbol
    elif symbol[-4:] == 'USDT':
        if is_fr:
            ws_add = 'wss://api.hbdm.vn/ws_index'
        else:
            ws_add = 'wss://api.hbdm.vn/linear-swap-ws'
        symbol_name = symbol
    else:
        ws_add = "wss://api-aws.huobi.pro/ws"
        symbol_name = symbol

    # 用来进行循环取数
    time_interval = 60
    if period == '1min':
        time_interval = 60
    elif period == '5min':
        time_interval = 300
    elif period == '15min':
        time_interval = 900
    elif period == '30min':
        time_interval = 1800
    elif period == '60min':
        time_interval = 3600
    elif period == '4hour':
        time_interval = 14400
    elif period == '1day':
        time_interval = 86400

    if end_time == None:
        end_timestamp = int(datetime.timestamp(datetime.now()))
        end_timestamp -= end_timestamp % time_interval
    else:
        end_timestamp = end_time

    if start_time == None:
        start_timestamp = end_timestamp - 299 * time_interval
    else:
        start_timestamp = start_time

    iteration = (end_timestamp - start_timestamp)//(time_interval*300) + 1

    while True:
        try:
            ws = create_connection(ws_add)
            break
        except Exception as e:
            print(e)
            print(symbol+' connect ws error,retry...')
            time.sleep(2)

    if is_fr:
        result_data = pd.DataFrame(columns=['datetime', 'hbfr'+symbol_name+'_close'])
    else:
        if get_full_market_data:
            result_data = pd.DataFrame(columns=['datetime', 'hb'+symbol_name+'_open', 'hb'+symbol_name+'_high',
                                                'hb'+symbol_name+'_low', 'hb'+symbol_name+'_close', 'hb'+symbol_name+'_volume'])
        else:
            result_data = pd.DataFrame(columns=['datetime', 'hb'+symbol_name+'_close'])

    for i in range(iteration):
        interval_time = end_timestamp - 299 * time_interval
        if interval_time < start_timestamp:
            interval_time = start_timestamp
        if start_timestamp > end_timestamp:
            break
        if not is_fr:
            kline_request = json.dumps({"req": "market.{symbol}.kline.{period}".format(symbol=symbol, period=period),
                                        'id': str(client_id),
                                        "from": interval_time,
                                        "to": end_timestamp})
        else:
            kline_request = json.dumps({"req": "market.{symbol}.estimated_rate.{period}".format(symbol=symbol, period=period),
                                        'id': str(client_id),
                                        "from": interval_time,
                                        "to": end_timestamp})

        ws.send(kline_request)
        # break_signal = False
        while True:
            break_count = 0
            while True:
                try:
                    compressData = ws.recv()
                    break
                except Exception as e:
                    print(e)
                    print(symbol+' receive fail, retry...')
                    time.sleep(0.3)
                    break_count += 1
                    if break_count >= 10:
                        raise Exception('There may be some problems, please retry later.')
            result = gzip.decompress(compressData).decode('utf-8')
            if result[:7] == '{"ping"':
                ts = result[8:21]
                pong = '{"pong":'+ts+'}'
                ws.send(pong)
                end_timestamp = interval_time - time_interval
#             elif result[2:5] == 'err':
#                 raise Exception('get error!')
            else:
                result = json.loads(result)
                # try:
                if not result['data'] and result['status'] == 'ok':
                    break
                elif result['status'] != 'ok':
                    raise Exception('Got nothing. There may be some problems in symbol or start/end timestamp.')
                else:
                    if get_full_market_data:
                        cache = pd.DataFrame(result["data"])[['id', 'open', 'high', 'low', 'close', 'vol']]
                        if not is_fr:
                            cache = cache.rename(columns={'id': 'datetime', 'open': 'hb'+symbol_name+'_open', 'high': 'hb'+symbol_name+'_high',
                                                          'low': 'hb'+symbol_name+'_low', 'close': 'hb'+symbol_name+'_close', 'vol': 'hb'+symbol_name+'_volume'})
                        else:
                            cache = cache.rename(columns={'id': 'datetime', 'open': 'hbfr'+symbol_name+'_open', 'high': 'hbfr'+symbol_name+'_high',
                                                          'low': 'hbfr'+symbol_name+'_low', 'close': 'hbfr'+symbol_name+'_close', 'vol': 'hbfr'+symbol_name+'_volume'})
                    else:
                        cache = pd.DataFrame(result["data"])[['id', 'close']]
                        if not is_fr:
                            cache = cache.rename(columns={'id': 'datetime', 'close': 'hb'+symbol_name+'_close'})
                        else:
                            cache = cache.rename(columns={'id': 'datetime', 'close': 'hbfr'+symbol_name+'_close'})
                    result_data = result_data.append(cache)
                    end_timestamp = interval_time - time_interval
                    break
                # except Exception as e:
                #     print(e)
                #     print(symbol, result)

        # if break_signal:
        #     break

        time.sleep(0.5)

# fred  no need any time stamp

    result_data.sort_values('datetime', inplace=True)
    # 调整时间, 并且把时间列的格式改成datetime格式
    if adjust_time:
        result_data['datetime'] += 28800
        result_data['datetime'] = pd.to_datetime(result_data['datetime'], unit='s')


    result_data.reset_index(drop=True, inplace=True)
    for i in result_data.columns[1:]:
        result_data[i] = result_data[i].astype('float')
    if col_with_asset_name:
        return result_data
    else:
        rename_dict = {}
        for i in result_data.columns:
            rename_dict[i] = i.split("_")[-1]
        result_data.rename(columns=rename_dict, inplace=True)
        result_data['adj_close'] = result_data['close']
        # result_data.drop(['index'], axis=1, inplace=True)
        return result_data


class SAR_agent:
    def __init__(self, time_bar,
                 sar_acce=0.02,
                 sar_max=0.2, macd_fast=12,
                 macd_slow=26, macd_period=9):
        self.time_bar = time_bar
        self.sar = talib.SAR(time_bar, time_bar, maximum=sar_max)
        self.macd = talib.MACD(time_bar, fastperiod=12,
                                        slowperiod=macd_slow,
                                        signalperiod=macd_period)

    def generate_signal_in_sar_stra(self, roll_length=5, vol_ratio=1.5):
        up = (self.sar > self.time_bar.close).astype(int)
        up_diff = -up.diff()

        self.vol_avg = self.time_bar.volume.rolling(roll_length, min_period=1).mean()
        vol_exceed = (self.time_bar.volume > self.vol_avg * vol_ratio).astype(int).rolling(roll_length,
                                                                                           min_periods=1).sum()
        # macdhist = MACD(close, fastperiod=12, slowperiod=26, signalperiod=9)


        macd_buy = (self.macd.macdhist > 0).astype(int).rolling(roll_length, min_periods=1).sum() # macdhist
        macd_sell = (self.macd.macdhist < 0).astype(int).diff() # macdhist
        nega_macd = (self.macd.macd < -10).astype(int).rolling(roll_length, min_periods=1).sum()

        trading_cond = (vol_exceed > 0)

        sar_sell = up.diff().rolling(roll_length, min_periods=1).sum()

        buy = (up_diff == 1) & (macd_buy > 0) & (macd_sell < 0) & trading_cond & (nega_macd > 0)

        sell = (sar_sell > 0) & (macd_sell > 0) & trading_cond

        return buy, sell
        print(time_bar)
        print(buy)
        print(sell)

# def sar_stra_v1(time_bar, print_fig=false):
#     agent = SAR_agent(time_bar)
#     buy, sell =


class SAR_agent_v1(SAR_agent):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def generate_signal_in_sar_stra(self, roll_length=5, vol_ratio=1.5):
      #  up = (self.sar > self.time_bar[1]).astype(int)  # .close
        up = (self.sar > self.time_bar.close).astype(int)  # .close
        going_up = up.rolling(4, min_periods=1).sum() == 0
        going_up = 1 # fred

      #  down = (self.sar < self.time_bar[1]).astype(int)  # .close

        down = (self.sar < self.time_bar.close).astype(int)  # .close

        down_diff = -down.diff()  # fred

        # self.vol_avg = self.time_bar.volume.rolling(roll_length, min_periods=1).mean()


        # vol_exceed = (self.time_bar.volume > self.vol_avg * vol_ratio).astype(int).rolling(roll_length, min_periods=1).sum()
        # trading_cond = (vol_exceed > 0)

        macd_buy = (self.macd.macdhist > 0).astype(int).diff()  # macdhist
        macd_sell = (self.macd.macdhist < 0).astype(int).diff()  # macdhist
        nega_macd = (self.macd.macd < 0).astype(int)

        sar_sell = up.diff().rolling(roll_length, min_periods=1).sum()

        buy = (going_up == 1) & (macd_buy > 0) & (nega_macd > 0)
        sell = (down_diff == 1)

        return buy, sell
        print('SAR_agent_v1')


class SAR_agent_v2(SAR_agent_v1):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def generate_signal_in_sar_stra(self, roll_length=5, vol_ratio=1.5):
        buy, sell = super().generate_signal_in_sar_stra(roll_length, vol_ratio)

        sell = self.generate_sell_signal(self.time_bar, buy, sell_start=5, sell_end=-1, profit_coef=1.005)

        return buy, sell

    def generate_sell_signal(self, df_input, buy_signal, sell_start=10, sell_end=60, profit_coef=1.005):
        df = df_input.copy()
        df['buy'] = buy_signal
        # init a sell-price vector
        df = df.apply(func=lambda x: x['close'] * profit_coef if x['buy'] else np.nan, axis=1)

        # generate a sell-price table with a drift
        df = pd.DataFrame(np.diag(df), columns=df.index)
        df = df.drop(df.index[df.sum() == 0], axis=0).replace(0, np.nan)

        if sell_end == -1:
            df = df.apply(func=lambda x: x.ffill(axis=0, inplace=False), axis=1)
        else:
            df = df.apply(func=lambda x: x.ffill(axis=0, inplace=False, limit=(sell_end - sell_start)), axis=1)
        df = df.shift(periods=sell_start, axis=1).fillna(0)

        def transform(row):
            p = ((row > 0) & (row <= df_input.close)).astype(int)
            if sum(p) > 0:
                return row[p == 1].index[0]
            elif sum(row != 0) > 0:
                return row[row != 0].index[-1]
            else:
                return row.index[-1]

        sell_loc = df.apply(func=transform, axis=1).values
        sell_loc = sell_loc[sell_loc != df_input.index[-1]]
        if sell_loc.size > 0:
            sell = pd.Series(data=1, index=sell_loc).groupby(level=0).sum()
            sell = sell.reindex(df_input.index, fill_value=0)
        else:
            sell = pd.Series(data=0, index=df_input.index)

        return sell


def sar_stra_v3(time_bar, agent=SAR_agent, print_fig=False, use_fee=True):
    agent = agent(time_bar)

    buy, sell = agent.generate_signal_in_sar_stra(vol_ratio=1.25)

    def generate_sell_signal(df_input, buy_signal, sell_start=10, sell_end=60, profit_coef=1.005):
        df = df_input.copy()
        df['buy'] = buy_signal
        # init a sell-price vector
        df = df.apply(func=lambda x: x['close'] * profit_coef if x['buy'] else np.nan, axis=1)

        # generate a sell-price table with a drift
        df = pd.DataFrame(np.diag(df), columns=df.index)
        df = df.drop(df.index[df.sum() == 0], axis=0).replace(0, np.nan)

        if sell_end == -1:
            df = df.apply(func=lambda x: x.ffill(axis=0, inplace=False), axis=1)
        else:
            df = df.apply(func=lambda x: x.ffill(axis=0, inplace=False, limit=(sell_end - sell_start)), axis=1)
        df = df.shift(periods=sell_start, axis=1).fillna(0)

        def transform(row):
            p = ((row > 0) & (row <= df_input.close)).astype(int)
            if sum(p) > 0:
                return row[p == 1].index[0]
            elif sum(row != 0) > 0:
                return row[row != 0].index[-1]
            else:
                return row.index[-1]

        sell_loc = df.apply(func=transform, axis=1).values
        sell = pd.Series(data=0, index=df_input.index)
        sell.loc[sell_loc] = 1

        return sell

    sell = generate_sell_signal(time_bar, buy, sell_start=5, sell_end=-1, profit_coef=1.005)

    # position
    pos_rec = pd.Series(buy.astype(int))
    for i in range(1, len(pos_rec)):
        if (pos_rec[i - 1] == 0) and sell[i]:
            pos_rec[i] = pos_rec[i - 1]
        else:
            pos_rec[i] = pos_rec[i - 1] + (1 if buy[i] and not sell[i] else -int(sell[i]))

    a = pos_rec

    real_buy = a.diff() > 0
    real_sell = a.diff() < 0

    # from trading
    if use_fee:
        fee = 0.0000065

        c = (-(a.diff()) * time_bar.close)
        c[c > 0] = c[c > 0] * (1 - fee)
        c[c < 0] = c[c < 0] * (1 + fee)
        # print(c[c > 0])
        # print(c[c < 0])
        c = c.cumsum()
    else:
        c = (-(a.diff()) * time_bar.close).cumsum()

    b = (a) * time_bar.close  # from holding stock

    value = (b + c)

    #     if print_fig:
    #         fig = agent.print_stat(real_buy, real_sell)
    #         ax = fig.add_subplot(5, 1, 4)
    #         ax.plot(value)

    #         ax = fig.add_subplot(5, 1, 5)
    #         ax.plot(a)

    #        fig.show()

    # plt.show()
    # plt.plot(time_bar.index, value)

    # print(value.min(), value.iloc[-1], value.min()/value.iloc[-1], buy.sum()/sell.sum())

    return value.iloc[-1]
    print('sar_stra_v3')


def get_huobi_data():
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:23.0) Gecko/20100101 Firefox/23.0'}

    url_huobi = 'https://api.huobi.pro/market/history/kline'
    params_h_btc_uncode = {
        'symbol': 'btcusdt',
        'period': '1min',
        'size': 1

    }
    params_h_btc = urlencode(params_h_btc_uncode)

    params_h_eth_uncode = {
        'symbol': 'ethusdt',
        'period': '1min',
        'size': 1,
        'open': 1935.2000,
        'close': 1,
        'low': 1,
        'high': 1
    }
    params_h_eth = urlencode(params_h_eth_uncode)

    # f_h_btc = urllib.request.urlopen('%s?%s' % (url_huobi, params_h_btc))
    req_btc = urllib.request.Request(url='%s?%s' % (url_huobi, params_h_btc), headers=headers)
    f_h_btc = urllib.request.urlopen(req_btc)

    # f_h_eth = urllib.request.urlopen('%s?%s' % (url_huobi, params_h_eth))
    req_eth = urllib.request.Request(url='%s?%s' % (url_huobi, params_h_eth), headers=headers)
    f_h_eth = urllib.request.urlopen(req_eth)
    print(f_h_btc)

    data_api_h_btc = f_h_btc.read()
    data_api_h_eth = f_h_eth.read()

    h_btc = json.loads(data_api_h_btc)
    h_eth = json.loads(data_api_h_eth)
    # print(h_btc["mid_price"],b_eth["mid_price"])

    return h_btc['data'][0]['close'], h_eth['data'][0]['close']


h_btc_price, h_eth_price = get_huobi_data()


# def realtime_data():
#     #dataset = []
#
# #while True:
#     #dataset = []
#     h_btc_price, h_eth_price = get_huobi_data()
#     #print(f"{h_eth_price}")
#
#     dataset.append([h_eth_price])
#     #time.sleep(10)
#     dataset1 = pd.DataFrame(dataset, columns=['time'] )
#     print(dataset1)
#
#     return dataset1
# while(1):
#     realtime_data()
#     time.sleep()

class SAR_agent_v3(SAR_agent_v2):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def generate_signal_in_sar_stra(self, roll_length=5, vol_ratio=1.5):
        buy, sell = super().generate_signal_in_sar_stra(roll_length, vol_ratio)

        tb = pd.Series(data=False, index=buy.index)
        tb[buy.ne(0).idxmax()] = True
        buy = tb

        sell = self.generate_sell_signal(self.time_bar, buy, sell_start=5, sell_end=-1, profit_coef=1.005)

        return buy, sell

        dataset2 = pd.read_csv(r'tpyrced_ETH_1min_data.csv')

        print('SAR_agent_v3')


if __name__ == '__main__':
    start_time = int(time.time()) - 1 * 86400
    period = '1min'
    target = 'ETH'
    dataset = []

# while True:
    hb_spot_1min_data = Huobi_kline(symbol=target.lower()+'usdt', period=period, start_time=start_time,
                                      get_full_market_data=True)
    hb_spot_1min_data.to_csv('2hb_ETH_1min.csv')

    #print(hb_spot_1min_data)
    #dataset.append(hb_spot_1min_data)
    #dataset.pop(appen)
    #del(dataset[1])
   # print(hb_spot_1min_data)

    # hb_cw_5min_data = Huobi_kline(symbol=target+'_CW', period=period, start_time=start_time,
    #                               get_full_market_data=True)
    # hb_cw_5min_data.to_csv('~/PycharmProjects/ljw_basis_strategy/Data/huobi_data/hb_btc_cw_1hour_data_with_name.csv')
    # hb_nw_5min_data = Huobi_kline(symbol=target+'_NW', period=period, start_time=start_time,
    #                               get_full_market_data=True)
    # hb_nw_5min_data.to_csv('~/PycharmProjects/ljw_basis_strategy/Data/huobi_data/hb_btc_nw_1hour_data_with_name.csv')
    # hb_cq_5min_data = Huobi_kline(symbol=target+'_CQ', period=period, start_time=start_time,
    #                               get_full_market_data=True)
    # hb_cq_5min_data.to_csv('~/PycharmProjects/ljw_basis_strategy/Data/huobi_data/hb_btc_cq_1hour_data_with_name.csv')
    # hb_nq_5min_data = Huobi_kline(symbol=target+'_NQ', period=period, start_time=start_time,
    #                               get_full_market_data=True)
    # hb_nq_5min_data.to_csv('~/PycharmProjects/ljw_basis_strategy/Data/huobi_data/hb_btc_nq_1hour_data_with_name.csv')
    #doge_fr = Huobi_kline(symbol=target, period=period, start_time=start_time, get_full_market_data=True)
    # doge_fr = doge_fr.iloc[:, 1:]
    #print(doge_fr)

    #dataset = pd.DataFrame(dataset)
    #dataset = dataset.drop(['datatime'], axis =1 )

    #dataset.to_csv('hb_ETH_1min.csv')

    #dataset2 = pd.read_csv(r'tpyrced_ETH_1min_data.csv')
    #dataset2 = dataset2.to_numpy()
  #  dataset2 = dataset2.tolist()


 #   dataset2 = pd.read_csv(r'hb_ETH_1min.csv')

    #dataset2 = dataset2.flatten()
          #dataset1 = dataset1.values
    #dataset = np.array(dataset, dtype='f8')
    #dataset = list(dataset)



  #  dataset = np.array(dataset)
    #print(dataset)

 #   sar_stra_v3(dataset2, SAR_agent_v3, True, 0.0000065)
#time.sleep(1)
    print('OK')
    #realtime_data()

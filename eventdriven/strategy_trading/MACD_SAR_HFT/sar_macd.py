import json
import talib
import pandas as pd
import numpy as np
#import utils.utils as utils

import matplotlib.pyplot as plt
import datetime as dt


from loguru import logger
from decimal import Decimal, ROUND_UP, ROUND_DOWN

from strategy_trading.easy_strategy.env import StrategyEnv
from strategy_trading.easy_strategy.strategy_base import StrategyImplBase
from strategy_trading.easy_strategy.datafeed import FullLevelOrderBook, FixedLevelOrderBook, Trade, Kline, Ticker, IndexTicker
from strategy_trading.easy_strategy.order import Order
from strategy_trading.easy_strategy.data_type import Side, OrderType, PositionEffect

class SAR_agent:
    def __init__(self, time_bar,
                 sar_acce= 0.02,
                 sar_max = 0.2, macd_fast = 12,
                 macd_slow = 26, macd_period =9):

        self.time_bar = time_bar
        self.sar =talib.abstract.SAR(time_bar,accelation=sar_acce, maximum=sar_max)
        self.macd = talib.abstract.MACD(time_bar,fastperiod=macd_fast,
                                        slowperiod=macd_slow,
                                        signalperiod=macd_period)
    def generate_signal_in_sar_stra(self, roll_length=5, vol_ratio=1.5):
        up = (self.sar> self.time_bar.close).astype(int)
        up_diff = -up.diff()

        self.vol_avg = self.time_bar.volume.rolling(roll_length, min_period=1).mean()
        vol_exceed = (self.time_bar.volume > self.vol_avg * vol_ratio).astype(int).rolling(roll_length, min_periods=1).sum()


        macd_buy  = (self.macd.macdhist > 0).astype(int).rolling(roll_length, min_periods=1).sum()
        macd_sell = (self.macd.macdhist < 0).astype(int).diff()
        nega_macd = (self.macd.macd< -10).astype(int).rolling(roll_length, min_periods=1).sum()

        trading_cond = (vol_exceed > 0)

        sar_sell = up.diff().rolling(roll_length, min_periods=1).sum()

        buy = (up_diff == 1) & (macd_buy > 0) & (macd_sell < 0) & trading_cond & (nega_macd > 0)

        sell = (sar_sell > 0) & (macd_sell > 0) & trading_cond

        return buy, sell


# def sar_stra_v1(time_bar, print_fig=false):
#     agent = SAR_agent(time_bar)
#
#     buy, sell =

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



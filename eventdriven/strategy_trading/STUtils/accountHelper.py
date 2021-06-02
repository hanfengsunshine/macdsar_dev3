# -*- coding: utf-8 -*-
# @Time    : 2020/7/20 9:31
# @Author  : Nicole
# @File    : accountHelper.py
# @Software: PyCharm
# @Description:

from datetime import datetime
import time
import os
import json
import psycopg2
import psycopg2.extras
from strategy_trading.STUtils.dbengine import get_db_config


class AccountClient:
    def __init__(self):
        self.db_config = get_db_config('data', 'trade_data')
        self.db_config['database'] = 'trade_data'

    def get_account_records(self, exchange: str = None, account: str = None, uid=None,
                            main_account: str = None, note: str = None):
        conn = psycopg2.connect(**self.db_config)
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        sql = "select * from accounts"
        sql_condition = ()
        values_condition = ()
        if exchange:
            sql_condition += ('exchange=%s',)
            values_condition += (exchange,)
        if account:
            sql_condition += ('account=%s',)
            values_condition += (account,)
        if uid:
            sql_condition += ('uid=%s',)
            values_condition += (str(uid),)
        if main_account:
            sql_condition += ('main_account=%s',)
            values_condition += (main_account,)
        if note:
            sql_condition += ('note=%s',)
            values_condition += (note,)
        if sql_condition:
            sql_condition = ' and '.join(sql_condition)
            sql = sql + ' where ' + sql_condition + ';'
        data = None
        try:
            cur.execute(sql, values_condition)
            data = cur.fetchall()
        except Exception as e:
            print(e)
        if conn:
            cur.close()
            conn.close()
        print(data)
        return data

    def add_account(self, exchange, account, uid, main_account, note=None):
        if account == main_account:
            main_account = None
        data = self.get_account_records(exchange=exchange, account=account, uid=uid, main_account=main_account,
                                        note=note)
        if data:
            print('account already exists: {}'.format(data))
            return
        sql = """insert into accounts (exchange, account, uid, main_account, "time", note) values (%s,%s,%s,%s,%s,%s)"""
        record_to_insert = (exchange, account, uid, main_account, int(time.time() * 1e6), note)
        conn = psycopg2.connect(**self.db_config)
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        try:
            cur.execute(sql, record_to_insert)
            conn.commit()
        except Exception as e:
            print(e)
        if conn:
            cur.close()
            conn.close()
        print('success')


if __name__ == '__main__':
    pass
    # c = AccountClient()
    # c.get_account_records()
    # c.add_account('binance', 'hbambinancetest', 'laconic030@hottalk.im', 'hbambinance')

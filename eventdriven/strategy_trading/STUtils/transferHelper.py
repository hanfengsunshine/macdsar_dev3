# -*- coding: utf-8 -*-
# @Time    : 2020/7/16 11:19
# @Author  : Nicole
# @File    : transferHelper.py
# @Software: PyCharm
# @Description:

import requests
from decimal import Decimal
from datetime import datetime
import json
import time
import sys
import psycopg2
import psycopg2.extras
from projectX1.utils import redisHelper as rh
from projectX1.exchangeConnection.huobiGlobal.huobiGlobal_rest_api import huobiGlobalAPI
from projectX1.exchangeConnection.profutures.profutures_rest_api import ProFuturesAPI
from projectX1.exchangeConnection.proswap.proswap_rest_api import ProSwapAPI
from projectX1.exchangeConnection.binance.binance_service import Client
from projectX1.exchangeConnection.okex.okex_rest_api import okexAPI
from strategy_trading.STUtils.dbengine import get_db_config

DING_URL = 'https://oapi.dingtalk.com/robot/send?access_token=8f8e3604521aa31e50bc257ebde8abd8ae3c7771d9825b1f01bfbd1a0286e48d'

OKEX_TRADE_PWD = '87Qw187'

okex_account_type_map = {'spot': 1, 'futures': 3, 'margin': 5, 'funding': 6, 'swap': 9, 'options': 12}
huobi_chain_map = {'btc': 'btc', 'usdt': 'usdterc20', 'eth': 'eth'}
binance_chain_map = {'btc': 'BTC', 'usdt': 'ETH', 'eth': 'ETH'}
okex_chain_map = {'btc': 'btc', 'usdt': 'usdt-erc20', 'eth': 'eth'}

ding_mobile_map = {
    'shawn': '+852-68750001',
    'nicole': '+852-68169630',
    'issac': '+852-62177487'
}


def notify_to_dingding(url, msg, msg_type, phone_nums=[], isatall=False):
    headers = {
        "Content-Type": "application/json",
        "Charset": "UTF-8"
    }
    post_data = {
        "msgtype": msg_type,
        "at": {
            "atMobiles": phone_nums,
            "isAtAll": isatall
        }
    }
    post_data[msg_type] = msg
    resp = requests.post(url, json.dumps(post_data), headers=headers, timeout=30)
    if resp.status_code == 200:
        print('send dingding notification success')
    else:
        print('send dingding notification fail')


def query_yes_no(question, default="yes"):
    """Ask a yes/no question via raw_input() and return their answer.

    "question" is a string that is presented to the user.
    "default" is the presumed answer if the user just hits <Enter>.
        It must be "yes" (the default), "no" or None (meaning
        an answer is required of the user).

    The "answer" return value is True for "yes" or False for "no".
    """
    valid = {"yes": True, "y": True, "ye": True,
             "no": False, "n": False}
    if default is None:
        prompt = " [y/n] "
    elif default == "yes":
        prompt = " [Y/n] "
    elif default == "no":
        prompt = " [y/N] "
    else:
        raise ValueError("invalid default answer: '%s'" % default)

    while True:
        sys.stdout.write(question + prompt)
        choice = input().lower()
        if default is not None and choice == '':
            return valid[default]
        elif choice in valid:
            return valid[choice]
        else:
            sys.stdout.write("Please respond with 'yes' or 'no' "
                             "(or 'y' or 'n').\n")


def get_address(exchange, account, ccy):
    addr = None
    if exchange == 'huobi':
        api_client = huobiGlobalAPI('pro', 'https://api.huobi.pro', apikey_list=rh.get_account('pro', account))
        try:
            addr_list = api_client.blockchain_deposit_address(ccy)
            for item in addr_list:
                if item['chain'] == huobi_chain_map[ccy]:
                    addr = item.copy()
                    break
        except Exception as e:
            print(e)
    elif exchange == 'binance':
        key_dict = rh.get_account('binance', account)
        api_client = Client(api_key=key_dict['ACCESS_KEY'], api_secret=key_dict['SECRET_KEY'])
        try:
            addr = api_client.get_deposit_address(coin=ccy.upper(), network=binance_chain_map[ccy])
        except Exception as e:
            addr = None
            print(e)
    elif exchange == 'okex':
        key_dict = rh.get_account('okex', account)
        if not isinstance(key_dict, list):
            key_dict = [key_dict]
        api_client = okexAPI(api_list=key_dict)
        try:
            addr_list = api_client.blockchain_deposit_address(ccy.upper())
            for item in addr_list:
                if item['currency'] == okex_chain_map[ccy]:
                    addr = item.copy()
                    break
        except Exception as e:
            print(e)
    elif exchange == 'deribit' and ccy == 'btc':
        return {
            'address': '34NBChyaMtVrCRhwgREe2CXwqsEbMEWKor'
        }

    return addr


class TransferClient:
    def __init__(self, person):
        # assert person in (
        #     'zhiji', 'yiyong', 'shawn', 'shea', 'wenhao', 'jeff', 'issac', 'nicole', 'payton', 'jiahao', 'yifeng')
        self.person = person
        self.db_config = get_db_config('data', 'trade_data')
        self.db_config['database'] = 'trade_data'
        conn = psycopg2.connect(**self.db_config)
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cur.execute("select exchange, account,uid,main_account from accounts;")
        self.exchange_account_map = {}
        for exchange, account, uid, main_account in cur.fetchall():
            if not exchange in self.exchange_account_map:
                self.exchange_account_map[exchange] = {}
            self.exchange_account_map[exchange][account] = {'uid': uid, 'main_account': main_account}
        if conn:
            cur.close()
            conn.close()

    def _book_into_db(self, data):
        ts = int(time.time() * 1e6)
        sql = """insert into transfers (exchange, account, account_type, ccy, transact_amount, "time", record_id, person, symbol, note) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
        record_to_insert = []
        for item in data:
            record_to_insert.append((
                item['exchange'],
                item['account'],
                item['account_type'],
                item['ccy'],
                item['transact_amount'],
                ts,
                item['record_id'],
                self.person,
                item.get('symbol'),
                item.get('note')
            ))
        conn = psycopg2.connect(**self.db_config)
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        try:
            cur.executemany(sql, record_to_insert)
            conn.commit()
            print('book {} transfers'.format(len(data)))
        except (Exception, psycopg2.Error) as e:
            print(e)
            print('fail to book transfers: {}'.format(data))
        if conn:
            cur.close()
            conn.close()

    def transfer_across_exchanges(self, from_exchange: str, from_acct: str, to_exchange: str, to_acct: str, ccy: str,
                                  amount: float):
        # if self.person not in ('shawn', 'issac', 'nicole'):
        #     print('Pls contact shawn/issac/nicole to help with transferring cross exchanges.')
        #     return
        ccy = ccy.lower()
        # assert ccy in ['btc', 'usdt', 'eth']
        # assert from_exchange in ['huobi', 'okex', 'binance']
        # assert to_exchange in ['huobi', 'okex', 'binance', 'deribit']
        # assert from_exchange != to_exchange
        assert from_acct in self.exchange_account_map[from_exchange]
        assert to_acct in self.exchange_account_map[to_exchange]
        from_acct_uid = self.exchange_account_map[from_exchange][from_acct]['uid']
        to_acct_uid = self.exchange_account_map[to_exchange][to_acct]['uid']

        addr = get_address(to_exchange, to_acct, ccy)
        if addr is None or not addr.get('address'):
            print('{} {} {} fail to get deposit address'.format(to_exchange, to_acct, ccy))
            return

        fee = None
        if from_exchange == 'huobi':
            chain = huobi_chain_map[ccy]
            api_client = huobiGlobalAPI('pro', 'https://api.huobi.pro', apikey_list=rh.get_account('pro', from_acct))
            try:
                reference_currencies = api_client.reference_currencies(currency=ccy)
                for item in reference_currencies[0]['chains']:
                    if item['chain'] == chain:
                        fee = item['transactFeeWithdraw']
                        break
            except Exception as e:
                print(e)
        elif from_exchange == 'binance':
            chain = binance_chain_map[ccy]
            key_dict = rh.get_account('binance', from_acct)
            api_client = Client(api_key=key_dict['ACCESS_KEY'], api_secret=key_dict['SECRET_KEY'])
            try:
                reference_currencies = api_client.get_all_currencies()
                for item in reference_currencies:
                    if item['coin'] == ccy.upper():
                        for network in item['networkList']:
                            if network['network'] == chain:
                                fee = network['withdrawFee']
                                break
            except Exception as e:
                print(e)
        elif from_exchange == 'okex':
            chain = okex_chain_map[ccy]
            key_dict = rh.get_account('okex', from_acct)
            if not isinstance(key_dict, list):
                key_dict = [key_dict]
            api_client = okexAPI(api_list=key_dict)
            try:
                reference_currencies = api_client.blockchain_withdraw_fee(symbol=ccy.upper())
                for item in reference_currencies:
                    if item['currency'] == okex_chain_map[ccy].upper():
                        fee = item['min_fee']
                        break
            except Exception as e:
                print(e)

        if fee is None:
            print('{} {} {} fail to get withdraw fee {}'.format(from_exchange, from_acct, ccy, fee))
            return
        if amount <= float(fee):
            print('transact amount {} is smaller than fee {}'.format(amount, fee))
            return
        # query_transfer_result = query_yes_no(
        #     'Deposit address is {}. Fee is {}. Do u want to proceed?'.format(addr, fee))
        query_transfer_result = True
        print('Pls Confirm and Record: Deposit address is {}. Fee is {}.'.format(addr, fee))
        if query_transfer_result:
            addr = addr['address']
            if from_exchange == 'huobi':
                try:
                    tamount = str(Decimal(str(amount)) - Decimal(fee))
                    if '.' in tamount:
                        tamount = '.'.join([tamount.split('.')[0], tamount.split('.')[1][:6]])
                    res = api_client.blockchain_withdraw(address=addr, amount=tamount,
                                                         currency=ccy, fee=fee, chain=chain)
                except Exception as e:
                    res = None
                    print(e)
            elif from_exchange == 'binance':
                try:
                    res = api_client.withdraw(asset=ccy.upper(), network=binance_chain_map[ccy],
                                              address=addr, amount=amount, recvWindow=5000,
                                              timestamp=int(time.time() * 1e3))
                    res = res['id']
                except Exception as e:
                    res = None
                    print(e)
            elif from_exchange == 'okex':
                try:
                    res = api_client.blockchain_withdraw(symbol=ccy, amount=str(Decimal(str(amount)) - Decimal(fee)),
                                                         fee=fee, to_address=addr, trade_pwd=OKEX_TRADE_PWD,
                                                         destination='4')
                    if res['result']:
                        res = res['withdrawal_id']
                    else:
                        res = None
                        print('okex transfer msg: {}'.format(res))
                except Exception as e:
                    res = None
                    print(e)
            print('get withdraw id: {}'.format(res))
            msg = {'url': DING_URL,
                   'msg': {'content': datetime.now().strftime(
                       '%m-%d %H:%M') + '  TRANSFER [{result}]' + '\nOperator: {}\n[{}-{}]\n{} {} -> {} {}\ndeposit address: [{}]\nfee: {}'.format(
                       self.person, amount, ccy, from_exchange, from_acct_uid, to_exchange, to_acct_uid, addr, fee
                   )}, 'msg_type': 'text', 'phone_nums': [], 'isatall': False}
            if self.person in ding_mobile_map:
                msg['phone_nums'].append(ding_mobile_map[self.person])
            if res is not None:
                data = [
                    {
                        'exchange': from_exchange,
                        'account': from_acct,
                        'account_type': 'spot',
                        'ccy': ccy,
                        'transact_amount': -amount,
                        'record_id': res,
                        'note': 'chain'
                    },
                    {
                        'exchange': to_exchange,
                        'account': to_acct,
                        'account_type': 'spot',
                        'ccy': ccy,
                        'transact_amount': Decimal(str(amount)) - Decimal(fee),
                        'record_id': res,
                        'note': 'chain'
                    }
                ]
                try:
                    self._book_into_db(data)
                except Exception as e:
                    print(e)
                    print('fail to book into db')
                msg['msg']['content'] = msg['msg']['content'].format(result=res)
            else:
                msg['msg']['content'] = msg['msg']['content'].format(result='FAIL')
            notify_to_dingding(**msg)

    def get_transfer_records(self, exchange=None, account=None, start_time: datetime = None, end_time: datetime = None,
                             size=None, sort_dir='desc'):
        '''
        :param exchange:
        :param account:
        :param start_time:
        :param end_time:
        :param size:
        :param sort_dir: string, 'asc','desc'
        :return:
        '''
        conn = psycopg2.connect(**self.db_config)
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        sql = "select * from transfers"
        sql_condition = ()
        values_condition = ()
        if exchange:
            sql_condition += ('exchange=%s',)
            values_condition += (exchange,)
        if account:
            sql_condition += ('account=%s',)
            values_condition += (account,)
        if start_time:
            sql_condition += ('"time">=%s',)
            values_condition += (int(start_time.timestamp() * 1e6),)
        if end_time:
            sql_condition += ('"time"<=%s',)
            values_condition += (int(end_time.timestamp() * 1e6),)
        if sql_condition:
            sql_condition = ' and '.join(sql_condition)
            sql = sql + ' where ' + sql_condition
        if sort_dir == 'desc':
            sql += '  order by "time" desc'
        if size:
            sql += ' limit {}'.format(size)
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

    def transfer(self, exchange, from_acct, from_acct_type, to_acct, to_acct_type, ccy, amount, from_symbol=None,
                 to_symbol=None):
        assert exchange in self.exchange_account_map
        assert from_acct in self.exchange_account_map[exchange]
        assert to_acct in self.exchange_account_map[exchange]
        assert amount >= 1e-6
        res = None
        if exchange == 'huobi':
            if from_acct == to_acct:
                res = self.huobi_transfer_within_one_account(from_acct, from_acct_type, to_acct_type, ccy, amount,
                                                             from_symbol, to_symbol)
            else:
                assert from_acct_type == to_acct_type == 'spot'
                res = self.huobi_transfer_between_main_sub_accounts(from_acct, to_acct, ccy, amount)
        elif exchange == 'binance':
            if from_acct == to_acct:
                res = self.binance_transfer_within_one_account(from_acct, from_acct_type, to_acct_type, ccy, amount)
            else:
                assert from_acct_type == to_acct_type == 'spot'
                res = self.binance_transfer_between_accounts(from_acct, to_acct, ccy, amount)
        elif exchange == 'okex':
            res = self.okex_transfer(from_acct, from_acct_type, to_acct, to_acct_type, ccy, amount, from_symbol,
                                     to_symbol)
        else:
            raise Exception('NOT SUPPORT [transfer] exchange={}'.format(exchange))
        if res:
            print(
                'SUCCESSS [transfer] exchange={}, from_acct={}, from_acct_type={}, to_acct={}, to_acct_type={}, ccy={}, amount={}, from_symbol={}, to_symbol={}'.format(
                    exchange, from_acct, from_acct_type, to_acct, to_acct_type, ccy, amount, from_symbol, to_symbol))
            self._book_into_db(res)
            return True

    def okex_transfer(self, from_acct, from_acct_type, to_acct, to_acct_type, ccy, amount, from_symbol=None,
                      to_symbol=None):
        exchange = 'okex'

        ccy = ccy.lower()
        res = None
        if from_acct == to_acct:
            assert from_acct_type != to_acct_type
            assert from_acct_type in list(okex_account_type_map)
            assert to_acct_type in list(okex_account_type_map)
            key_dict = rh.get_account('okex', from_acct)
            if not isinstance(key_dict, list):
                key_dict = [key_dict]
            try:
                c = okexAPI(api_list=key_dict)
                res = c.transfer(ccy, amount, _type=0, _from=okex_account_type_map[from_acct_type],
                                 _to=okex_account_type_map[to_acct_type], instrument_id=from_symbol,
                                 to_instrument_id=to_symbol)
            except Exception as e:
                print(e)
        else:
            from_acct_main_account = self.exchange_account_map[exchange][from_acct]['main_account']
            to_acct_main_account = self.exchange_account_map[exchange][to_acct]['main_account']
            if not from_acct_main_account and not to_acct_main_account:
                print('ABORT [okex_transfer] both main accounts')
                return
            elif from_acct_main_account and to_acct_main_account:
                print('ABORT [okex_transfer] both sub accounts')
                return
            if not from_acct_main_account:
                assert from_acct == to_acct_main_account
                key_dict = rh.get_account('okex', from_acct)
                _type = 1
                sub_account_uid = self.exchange_account_map[exchange][to_acct]['uid']
            if not to_acct_main_account:
                assert to_acct == from_acct_main_account
                key_dict = rh.get_account('okex', to_acct)
                _type = 2
                sub_account_uid = self.exchange_account_map[exchange][from_acct]['uid']
            if not isinstance(key_dict, list):
                key_dict = [key_dict]
            try:
                c = okexAPI(api_list=key_dict)
                res = c.transfer(ccy, amount, _type=_type, _from=okex_account_type_map[from_acct_type],
                                 _to=okex_account_type_map[to_acct_type],
                                 sub_account=sub_account_uid,
                                 instrument_id=from_symbol, to_instrument_id=to_symbol)
            except Exception as e:
                print(e)
        data = []
        if res and res.get('transfer_id'):
            data = [
                {
                    'exchange': exchange,
                    'account': from_acct,
                    'account_type': from_acct_type,
                    'ccy': ccy,
                    'transact_amount': -amount,
                    'record_id': res['transfer_id'],
                    'symbol': from_symbol
                },
                {
                    'exchange': exchange,
                    'account': to_acct,
                    'account_type': to_acct_type,
                    'ccy': ccy,
                    'transact_amount': amount,
                    'record_id': res['transfer_id'],
                    'symbol': to_symbol
                }
            ]
        else:
            print(res)
            print('FAIL [transfer] from_acct={}, from_acct_type={}, to_acct={}, to_acct_type={}, '
                  'ccy={}, amount={}, from_symbol={}, to_symbol={}'.format(
                from_acct, from_acct_type, to_acct, to_acct_type, ccy, amount, from_symbol, to_symbol))
        return data

    def binance_transfer_within_one_account(self, account, from_acct_type, to_acct_type, ccy, amount):
        assert from_acct_type != to_acct_type

        exchange = 'binance'
        key_dict = rh.get_account('binance', account)
        client = Client(api_key=key_dict['ACCESS_KEY'], api_secret=key_dict['SECRET_KEY'])

        ccy = ccy.lower()
        res = None
        if from_acct_type == 'spot' and to_acct_type == 'swap':
            _type = 1
        elif from_acct_type == 'swap' and to_acct_type == 'spot':
            _type = 2
        elif from_acct_type == 'spot' and to_acct_type == 'futures':
            _type = 3
        elif from_acct_type == 'futures' and to_acct_type == 'spot':
            _type = 4
        else:
            print('NOT SUPPORT [binance_transfer_within_one_account] account={}, from_acct_type={}, to_acct_type={}, '
                  'ccy={}, amount={}'.format(account, from_acct_type, to_acct_type, ccy, amount))
            return []
        try:
            res = client.swap_transfer(asset=ccy.upper(), amount=amount, direction=_type)
        except Exception as e:
            print(e)
        data = []
        if res and res.get('tranId'):
            data = [
                {
                    'exchange': exchange,
                    'account': account,
                    'account_type': from_acct_type,
                    'ccy': ccy,
                    'transact_amount': -amount,
                    'record_id': res['tranId']
                },
                {
                    'exchange': exchange,
                    'account': account,
                    'account_type': to_acct_type,
                    'ccy': ccy,
                    'transact_amount': amount,
                    'record_id': res['tranId']
                }
            ]
        else:
            print(res)
            print(
                'FAIL [binance_transfer_within_one_account] account={}, from_acct_type={}, to_acct_type={}, ccy={}, amount={}'.format(
                    account, from_acct_type, to_acct_type, ccy, amount))
        return data

    def binance_transfer_between_accounts(self, from_acct, to_acct, ccy, amount):
        assert from_acct != to_acct
        exchange = 'binance'
        from_acct_main_account = self.exchange_account_map[exchange][from_acct]['main_account']
        to_acct_main_account = self.exchange_account_map[exchange][to_acct]['main_account']
        if not from_acct_main_account and not to_acct_main_account:
            print('ABORT [binance_transfer_between_accounts] both main accounts')
            return
        if not from_acct_main_account:
            main_account = from_acct
            assert main_account == to_acct_main_account
        else:
            main_account = to_acct
            assert main_account == from_acct_main_account

        ccy = ccy.lower()
        res = None
        key_dict = rh.get_account('binance', main_account)
        client = Client(api_key=key_dict['ACCESS_KEY'], api_secret=key_dict['SECRET_KEY'])
        try:
            res = client.transfer_to_subaccount(_from=self.exchange_account_map[exchange][from_acct]['uid'],
                                                _to=self.exchange_account_map[exchange][to_acct]['uid'],
                                                symbol=ccy.upper(), amount=amount)
        except Exception as e:
            print(e)
        data = []
        if res and res.get('txnId'):
            data = [
                {
                    'exchange': exchange,
                    'account': from_acct,
                    'account_type': 'spot',
                    'ccy': ccy,
                    'transact_amount': -amount,
                    'record_id': res['txnId']
                },
                {
                    'exchange': exchange,
                    'account': to_acct,
                    'account_type': 'spot',
                    'ccy': ccy,
                    'transact_amount': amount,
                    'record_id': res['txnId']
                }
            ]
        else:
            print(res)
            print(
                'FAIL [binance_transfer_between_accounts] from_acct={}, to_acct={}, ccy={}, amount={}'.format(
                    from_acct, to_acct, ccy, amount))
        return data

    def huobi_transfer_within_one_account(self, account, from_acct_type, to_acct_type, ccy, amount, from_symbol=None,
                                          to_symbol=None):
        assert from_acct_type != to_acct_type
        exchange = 'huobi'
        ccy = ccy.lower()

        key_dict = rh.get_account('pro', account.replace('_', ''))
        if isinstance(key_dict, list):
            key_dict = key_dict[-1]

        res = None

        if sorted([from_acct_type, to_acct_type]) == sorted(['spot', 'swap']):
            api_client = ProSwapAPI('https://api.hbdm.com', access_key=key_dict['ACCESS_KEY'],
                                    secret_key=key_dict['SECRET_KEY'])
            try:
                res = api_client.swap_transfer(ccy, amount, from_acct_type, to_acct_type)
                if res:
                    res = res.get('data')
            except Exception as e:
                print(e)
        elif sorted([from_acct_type, to_acct_type]) == sorted(['spot', 'futures']):
            api_client = ProFuturesAPI('https://api.hbdm.com', access_key=key_dict['ACCESS_KEY'],
                                       secret_key=key_dict['SECRET_KEY'])
            if from_acct_type == 'futures':
                type = 'futures-to-pro'
            else:
                type = 'pro-to-futures'
            try:
                res = api_client.transfer(ccy, amount, type)
                if res:
                    res = res.get('data')
            except Exception as e:
                print(e)
        else:
            api_client = huobiGlobalAPI('pro', 'https://api.huobi.pro', apikey_list=key_dict)
            if from_acct_type == 'margin' and to_acct_type == 'spot':
                if not from_symbol:
                    print(
                        'FAIL [huobi_transfer_within_one_account] need to give from_symbol if transfer from margin to spot')
                else:
                    try:
                        res = api_client.margin_transfer_out(from_symbol, ccy, str(amount))
                    except Exception as e:
                        print(e)
            elif from_acct_type == 'spot' and to_acct_type == 'margin':
                if not to_symbol:
                    print(
                        'FAIL [huobi_transfer_within_one_account] need to give to_symbol if transfer from spot to margin')
                else:
                    try:
                        res = api_client.margin_transfer_in(to_symbol, ccy, str(amount))
                    except Exception as e:
                        print(e)
            elif from_acct_type == 'cross-margin' and to_acct_type == 'spot':
                try:
                    res = api_client.cross_margin_transfer_out(ccy, str(amount))
                except Exception as e:
                    print(e)
            elif from_acct_type == 'spot' and to_acct_type == 'cross-margin':
                try:
                    res = api_client.cross_margin_transfer_in(ccy, str(amount))
                except Exception as e:
                    print(e)
            else:
                print('NOT SUPPORT [huobi_transfer_within_one_account] account={}, from_acct_type={}, to_acct_type={}, '
                      'ccy={}, amount={}, from_symbol={}, to_symbol={}'.format(
                    account, from_acct_type, to_acct_type, ccy, amount, from_symbol, to_symbol))
        data = []
        if res:
            data = [
                {
                    'exchange': exchange,
                    'account': account,
                    'account_type': from_acct_type,
                    'ccy': ccy,
                    'transact_amount': -amount,
                    'record_id': res,
                    'symbol': from_symbol
                },
                {
                    'exchange': exchange,
                    'account': account,
                    'account_type': to_acct_type,
                    'ccy': ccy,
                    'transact_amount': amount,
                    'record_id': res,
                    'symbol': to_symbol
                }
            ]
        else:
            print(res)
            print('FAIL [huobi_transfer_within_one_account] account={}, from_acct_type={}, to_acct_type={}, ccy={}, '
                  'amount={}, from_symbol={}, to_symbol={}'.format(
                account, from_acct_type, to_acct_type, ccy, amount, from_symbol, to_symbol))
        return data

    def huobi_transfer_between_main_sub_accounts(self, from_acct, to_acct, ccy, amount):
        assert from_acct != to_acct

        exchange = 'huobi'
        from_acct_main_account = self.exchange_account_map[exchange][from_acct]['main_account']
        to_acct_main_account = self.exchange_account_map[exchange][to_acct]['main_account']
        if not from_acct_main_account and not to_acct_main_account:
            print('ABORT [huobi_transfer_between_main_sub_accounts] both main accounts')
            return
        if from_acct_main_account and to_acct_main_account:
            print('ABORT [huobi_transfer_between_main_sub_accounts] both sub accounts')
            return

        if not from_acct_main_account:
            main_account = from_acct
            assert main_account == to_acct_main_account
        else:
            main_account = to_acct
            assert main_account == from_acct_main_account

        ccy = ccy.lower()

        res = None
        if not from_acct_main_account:
            # from_acct is main account
            api_client = huobiGlobalAPI('pro', 'https://api.huobi.pro',
                                        apikey_list=rh.get_account('pro', from_acct.replace('_', '')))
            try:
                res = api_client.subuser_transfer(sub_uid=int(self.exchange_account_map[exchange][to_acct]['uid']),
                                                  currency=ccy,
                                                  amount=amount, transfer_type='master-transfer-out')
            except Exception as e:
                print(e)
        else:
            # to_acct is main account
            api_client = huobiGlobalAPI('pro', 'https://api.huobi.pro',
                                        apikey_list=rh.get_account('pro', to_acct.replace('_', '')))
            try:
                res = api_client.subuser_transfer(sub_uid=int(self.exchange_account_map[exchange][from_acct]['uid']),
                                                  currency=ccy,
                                                  amount=amount, transfer_type='master-transfer-in')
            except Exception as e:
                print(e)
        data = []
        if res:
            data = [
                {
                    'exchange': exchange,
                    'account': from_acct,
                    'account_type': 'spot',
                    'ccy': ccy,
                    'transact_amount': -amount,
                    'record_id': res
                },
                {
                    'exchange': exchange,
                    'account': to_acct,
                    'account_type': 'spot',
                    'ccy': ccy,
                    'transact_amount': amount,
                    'record_id': res
                },
            ]
        else:
            print(res)
            print(
                'FAIL [huobi_transfer_between_main_sub_accounts] from_acct={}, to_acct={}, ccy={}, amount={}'.format(
                    from_acct, to_acct, ccy, amount))
        return data


if __name__ == '__main__':
    pass

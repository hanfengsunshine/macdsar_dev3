from projectX1.exchangeConnection.huobiGlobal.huobiGlobal_rest_api import huobiGlobalAPI
from optparse import OptionParser
import os
import json
import pandas
import requests
pandas.options.display.float_format = '${:,.4f}'.format
from strategy_trading.StrategyTrading.symbolHelper import SymbolHelper
import asyncio


class SimpleRestHelper():
    def __init__(self, symbol_helper: SymbolHelper, config_file = None):
        if config_file is None:
            config_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), "api.json")
        with open(config_file) as json_file:
            db_config = json.load(json_file)
        self.client = huobiGlobalAPI('pro', host='https://api.huobi.pro', apikey_list= [db_config])
        self.account_id = self.client._get_account_id()["spot"]
        self.initial_balance = pandas.Series({
            'bch': 0.3,
            'btc': 0.01,
            'dash': 1,
            "eos": 25,
            "etc": 15,
            "eth": 0.5,
            "hpt": 10000,
            "ht": 25,
            "ltc": 1.4,
            "omg": 100,
            "usdt": 100,
            "xrp": 350,
            "zec": 2,
        })
        self.symbol_helper = symbol_helper

    def get_tickers(self):
        tickers = requests.get("https://api.huobi.pro/market/tickers").text
        tickers = json.loads(tickers)
        tickers = tickers['data']
        return tickers

    def get_ccy_vol_in_usd(self):
        tickers = self.get_tickers()
        df = pandas.DataFrame(tickers)

        # get vol in USDT/HUSD
        df.drop(df[~df['symbol'].isin(list(self.symbol_helper.market_info_by_exchange_symbol["HUOBI_SPOT"].keys()))].index, inplace=True)
        df['base'] = df['symbol'].apply(lambda x: self.symbol_helper.get_info_from_symbol(x, 'HUOBI_SPOT')['base'])
        df['quote'] = df['symbol'].apply(lambda x: self.symbol_helper.get_info_from_symbol(x, 'HUOBI_SPOT')['quote'])
        df.set_index('symbol', inplace=True)
        df['close'] = df['close'].astype(float)
        df['amount'] = df['amount'].astype(float)

        # get vol
        btc_price = df.loc['btcusdt', 'close']
        eth_price = df.loc['ethusdt', 'close']
        ht_price = df.loc['htusdt', 'close']
        trx_price = df.loc['trxusdt', 'close']
        df['vol_in_usd'] = df.apply(lambda x: x['amount'] * x['close'] if x['quote'] in ['USDT', 'HUSD'] else x['amount'] * x['close'] * btc_price if x['quote'] == 'BTC' else x['amount'] * x['close'] * eth_price if x['quote'] == 'ETH' else x['amount'] * x['close'] * ht_price if x['quote'] == 'HT' else x['amount'] * x['close'] * trx_price if x['quote'] == 'TRX' else None, axis = 1)

        base_vol = df.groupby('base')['vol_in_usd'].sum()
        quote_vol = df.groupby('quote')['vol_in_usd'].sum()
        info = pandas.concat([base_vol.to_frame('base'), quote_vol.to_frame('quote')], axis = 1).fillna(0).sum(axis = 1)

        info.sort_values(ascending=False, inplace=True)

        info = info.to_frame('vol')
        info['ccy'] = info.index
        info['ccy'] = info['ccy'].str.lower()
        info['price'] = info['ccy'].apply(lambda x: df.loc["{}usdt".format(x), 'close'] if "{}usdt".format(x) in df.index else df.loc["{}btc".format(x), 'close'] * df.loc["btcusdt", 'close'] if "{}btc".format(x) in df.index else df.loc["{}husd".format(x), 'close'] if "{}husd".format(x) in df.index else 1 if x == 'husd' else None)
        info['pairs'] = info.index.map(lambda x: len(df[(df['base'] == x) | (df['quote'] == x)]))
        return info

    def get_balances(self, look_at_initial_balance = True):
        response = self.client.get_spot_balance()
        balance = pandas.DataFrame(columns = ['trade', 'frozen'])
        for info in response:
            balance.loc[info['currency'], info['type']] = float(info['balance'])
        balance.drop(balance[balance.sum(axis = 1) == 0].index, inplace=True)

        info = self.get_ccy_vol_in_usd()
        balance['price'] = balance.index.map(lambda x: info.loc[x.upper(), 'price'] if x.upper() in info.index else None)
        balance['vol'] = balance.index.map(lambda x: info.loc[x.upper(), 'vol'] if x.upper() in info.index else None)
        balance['total'] = balance['trade'].fillna(0) + balance['frozen'].fillna(0)
        balance['value'] = balance['price'] * (balance['trade'].fillna(0) + balance['frozen'].fillna(0))

        if look_at_initial_balance:
            balance = pandas.concat([balance, self.initial_balance.to_frame('init')], axis=1)
            balance['diff'] = balance['trade'].fillna(0) - balance['init'].fillna(0)
            balance['diffValue'] = balance['diff'] * balance['price']

        return balance

    def get_active_orders(self, symbol):
        return self.client.active_orders(symbol)

    def create_order(self, symbol, side, price, size):
        return self.client.order(
            account_id=self.account_id,
            amount = size,
            symbol = symbol,
            order_type='{}-limit'.format(side.lower()),
            price = price
        )

    def cancel_order(self, order_id):
        return self.client.cancel_order(order_id)

    def cancel_all_orders(self, symbol):
        active_orders = self.get_active_orders(symbol)
        for order in active_orders:
            self.cancel_order(order['id'])
        return self.get_active_orders(symbol)

    def get_order_info(self, order_id):
        return self.client.order_info(order_id)




if __name__ == '__main__':
    symbol_helper = SymbolHelper()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(symbol_helper.load_symbol_reference())

    helper = SimpleRestHelper(symbol_helper)
    balance = helper.get_balances()
    print(balance)
    #print(helper.create_order('btcusdt', 'BUY', 5000, 0.002))
    #print(helper.get_active_orders('btcusdt'))
    #print(helper.cancel_all_orders('btcusdt'))


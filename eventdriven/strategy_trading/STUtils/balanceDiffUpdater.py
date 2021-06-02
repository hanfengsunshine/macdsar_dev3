from projectX1.exchangeConnection.huobiGlobal.huobiGlobal_rest_api import huobiGlobalAPI
from projectX1.exchangeConnection.profutures.profutures_rest_api import ProFuturesAPI
from optparse import OptionParser
import os
import json
import pandas
import requests
import logging
from monitor.utils.ding_utils import send_dataframe_as_image
from datetime import datetime, timedelta
pandas.set_option('display.max_rows', 500)
pandas.set_option('display.max_columns', 500)
pandas.set_option('display.width', 1000)

CHAT_ID_PNL_MONITOR = 'chat75efdb06984f69ee668c2933377805e8'


class SimpleRestHelper():
    def __init__(self, config_file):
        with open(config_file) as json_file:
            api_config = json.load(json_file)
        self.client = huobiGlobalAPI('pro', host='https://api.huobi.pro', apikey_list= [
            {
                "ACCESS_KEY": api_config["ACCESS_KEY"],
                "SECRET_KEY": api_config["SECRET_KEY"]
            }
        ])

        if 'futures' in api_config and api_config['futures']:
            self.fut_client = ProFuturesAPI(access_key=api_config["ACCESS_KEY"], secret_key=api_config["SECRET_KEY"], url="https://api.hbdm.com")
        else:
            self.fut_client = None

        self.account_id = self.client._get_account_id()["spot"]
        self.initial_balance = pandas.Series(api_config['initial_balance'])
        self.initial_balance.index = self.initial_balance.index.str.lower()
        self.account_name = api_config['account_name']
        self.dingtalk_access_token = api_config['ding']

    def get_balances(self):
        response = self.client.get_balance(self.account_id)
        balance = pandas.DataFrame(columns = ['trade', 'frozen'])
        for info in response:
            balance.loc[info['currency'], info['type']] = float(info['balance'])
        balance.drop(balance[balance.sum(axis = 1) == 0].index, inplace=True)
        balance = pandas.concat([balance, self.initial_balance.to_frame('init')], axis = 1)

        if self.fut_client is not None:
            account_info = self.fut_client.get_contract_account_info()
            if account_info:
                for one_account in account_info['data']:
                    if one_account['margin_balance'] > 0:
                        balance.loc['futu_{}'.format(one_account['symbol'].lower()), 'trade'] = one_account['margin_balance']

        balance['now'] = balance['trade'].fillna(0) + balance['frozen'].fillna(0)
        balance['diff'] = balance['now'] - balance['init'].fillna(0)
        return balance

    def get_balance_value_change(self, balance:pandas.DataFrame):
        balance['price'] = None
        # get price from exchange
        tickers = requests.get("https://api.huobi.pro/market/tickers").text
        tickers = json.loads(tickers)['data']
        tickers = pandas.DataFrame(tickers).set_index('symbol')
        for ccy in balance.index:
            if ccy in ['usdt', 'husd']:
                balance.loc[ccy, 'price'] = 1
                continue

            adjusted_ccy = ccy
            if "futu_" == ccy[:5]:
                adjusted_ccy = ccy[5:]

            symbol = '{}usdt'.format(adjusted_ccy)
            if symbol in tickers.index:
                balance.loc[ccy, 'price'] = tickers.loc[symbol, 'close']
            else:
                symbol = '{}btc'.format(adjusted_ccy)
                if symbol in tickers.index:
                    balance.loc[ccy, 'price'] = tickers.loc[symbol, 'close'] * tickers.loc['btcusdt', 'close']
                else:
                    logging.warning('cannot find price of {}'.format(ccy))

        balance['value'] = balance['now'] * balance['price']
        balance.drop(balance[(pandas.isnull(balance['init'])) & (balance['value'].fillna(0) < 10)].index, inplace=True)
        balance['valueChange'] = balance['price'] * balance['diff']
        balance.loc['sum', 'valueChange'] = balance['valueChange'].sum()

        return balance

    def notify(self, balance: pandas.DataFrame):
        # format balance
        #print(balance.style.bar(subset=['valueChange'], align='mid', color=['#5fba7d']).to_html())

        balance['init'] = balance['init'].apply(lambda x: "{:,.4f}".format(x) if not pandas.isnull(x) else "")
        balance['now'] = balance['now'].apply(lambda x: "{:,.4f}".format(x) if not pandas.isnull(x) else "")
        balance['price'] = balance['price'].apply(lambda x: "{:,.4f}".format(x) if not pandas.isnull(x) else "")
        balance['valueChange'] = balance['valueChange'].apply(lambda x: "{:,.4f}".format(x) if not pandas.isnull(x) else "")
        balance = balance[['init', 'now', 'price', 'valueChange']].rename(columns = {
            'init': 'borrow',
            'now': "# coins",
            'price': "last ($)",
            'valueChange': "valueChg ($)"
        })

        send_dataframe_as_image(CHAT_ID_PNL_MONITOR, balance.reset_index(), os.path.abspath('mytable.png'), image_tile="{} {}".format(self.account_name, (datetime.utcnow() + timedelta(hours = 8)).strftime('%Y-%m-%d %H:%M HKT')))



if __name__ == '__main__':
    parser = OptionParser()
    parser.add_option("-c", type="string", dest="config", default="~")
    (options, args) = parser.parse_args()
    config_file = os.path.normpath(options.config)

    helper = SimpleRestHelper(config_file)
    balance = helper.get_balances()
    helper.get_balance_value_change(balance)
    #print(balance)
    helper.notify(balance)

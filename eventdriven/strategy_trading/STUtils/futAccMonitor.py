from projectX1.exchangeConnection.profutures.profutures_rest_api import ProFuturesAPI
import json
import pandas
from strategy_trading.STUtils.fairValue import FairValuePricer
from strategy_trading.StrategyTrading.symbolHelper import SymbolHelper
import asyncio
import os
from monitor.utils.ding_utils import send_dataframe_as_image
from datetime import datetime, timedelta
from optparse import OptionParser

CHAT_ID_POSITION_MONITOR = 'chat75efdb06984f69ee668c2933377805e8'


class FutAccMonitor():
    def __init__(self, config_file_path):
        with open(config_file_path) as json_file:
            config = json.load(json_file)

        self.fut_client = ProFuturesAPI(access_key=config["ACCESS_KEY"], secret_key=config["SECRET_KEY"], url="https://api.hbdm.com")

        self.fut_size_multiplier = {
            "BTC": 100,
            "ETH": 10,
            "EOS": 10,
            "XRP": 10,
            "LTC": 10,
            "BCH": 10,
            "BSV": 10,
            "TRX": 10,
            "ETC": 10,
        }

        self.account_name = config['account_name']
        self.dingtalk_access_token = config['ding']

    def get_position(self):
        positions = self.fut_client.get_contract_position_info()
        positions=  positions['data']

        account_info = self.fut_client.get_contract_account_info()
        account_info = account_info['data']

        df = pandas.DataFrame(columns=["MarginBal", "MarginUsed", "URPnL", "Leverage", "LiqPrice", "CWLongPos", "CWShortPos", "NWLongPos", "NWShortPos", "CQLongPos", "CQShortPos"])
        for one_account in account_info:
            df.loc[one_account['symbol'], "MarginBal"] = one_account['margin_balance']
            df.loc[one_account['symbol'], "MarginUsed"] = one_account['margin_position']
            df.loc[one_account['symbol'], "URPnL"] = one_account['profit_unreal']
            df.loc[one_account['symbol'], "LiqPrice"] = one_account['liquidation_price']

        for one_position in positions:
            ccy = one_position['symbol']

            ctype = "CW" if one_position['contract_type'] == "this_week" else "NW" if one_position['contract_type'] == "next_week" else "CQ" if one_position['contract_type'] == "quarter" else "None"
            typ = "Long" if one_position['direction'] == 'buy' else "Short" if one_position['direction'] == 'sell' else "None"
            df.loc[ccy, "{}{}Pos".format(ctype, typ)] = one_position['volume']

        return df

    def start(self):
        symbol_helper = SymbolHelper()
        loop = asyncio.get_event_loop()
        loop.run_until_complete(symbol_helper.load_symbol_reference())

        pricer = FairValuePricer(symbol_helper)
        ccy_info = pricer.get_ccy_vol_in_usd()

        df = self.get_position()
        df['ccyPrice'] = ccy_info['price']

        df.loc[df[df['MarginBal'] == 0].index, 'MarginBal'] = None
        df['Leverage'] = df[["CWLongPos", "CWShortPos", "NWLongPos", "NWShortPos", "CQLongPos", "CQShortPos"]].fillna(0).sum(axis = 1) / (df['ccyPrice'] * df['MarginBal']) * pandas.Series(self.fut_size_multiplier)

        df['URPnL(USD)'] = df['URPnL'] * df['ccyPrice']
        df['pos(USD)'] = 0
        for col in ["CWLongPos", "NWLongPos", "CQLongPos"]:
            df['pos(USD)'] += df[col].fillna(0) * pandas.Series(self.fut_size_multiplier)
        for col in ["CWShortPos", "NWShortPos", "CQShortPos"]:
            df['pos(USD)'] -= df[col].fillna(0) * pandas.Series(self.fut_size_multiplier)

        df['pos(Sum)'] = df['pos(USD)'] / df['ccyPrice']

        ccies = df.index.values
        df.loc['sum(USD)', 'URPnL(USD)'] = df.loc[ccies, 'URPnL(USD)'].sum()
        df.loc['sum(USD)', 'pos(USD)'] = df.loc[ccies, 'pos(USD)'].sum()
        df.loc['sum(BTC)', 'pos(Sum)'] = (df.loc[ccies, 'pos(USD)'] / df.loc['BTC', 'ccyPrice']).sum()
        df.loc['sum(USD)', "MarginBal"] = (df.loc[ccies, 'MarginBal'] * df.loc[ccies, 'ccyPrice']).sum()
        df.loc['sum(BTC)', "MarginBal"] = (df.loc[ccies, 'MarginBal'] * df.loc[ccies, 'ccyPrice']).sum() / df.loc['BTC', 'ccyPrice']
        df.loc['sum(BTC)', "URPnL"] = (df.loc[ccies, 'URPnL(USD)'] / df.loc['BTC', 'ccyPrice']).sum()
        # format
        for col in ["MarginBal", "MarginUsed", "URPnL", "Leverage", "LiqPrice", "ccyPrice", 'pos(Sum)']:
            df[col] = df[col].apply(lambda x: "{:,.4f}".format(x) if not pandas.isnull(x) else "")
        for col in ["CWLongPos", "CWShortPos", "NWLongPos", "NWShortPos", "CQLongPos", "CQShortPos", 'pos(USD)', 'URPnL(USD)']:
            df[col] = df[col].apply(lambda x: "{:,.0f}".format(x) if not pandas.isnull(x) else "")
        df.loc['sum(USD)', "MarginBal"] = "{:,.0f}".format(float(df.loc['sum(USD)', "MarginBal"].replace(",", "")))

        cols = ["MarginBal", "MarginUsed", "URPnL", 'URPnL(USD)', "Leverage", "LiqPrice", "pos(Sum)", 'pos(USD)', "CWLongPos", "CWShortPos", "NWLongPos", "NWShortPos", "CQLongPos", "CQShortPos", "ccyPrice"]
        send_dataframe_as_image(CHAT_ID_POSITION_MONITOR, df[cols].reset_index(), os.path.abspath('mytable.png'), image_tile="{} {}".format(self.account_name,(datetime.utcnow() + timedelta(hours=8)).strftime('%Y-%m-%d %H:%M HKT')))


if __name__ == '__main__':
    parser = OptionParser()
    parser.add_option("-c", type="string", dest="config", default="~")
    (options, args) = parser.parse_args()
    config_file = os.path.normpath(options.config)

    helper = FutAccMonitor(config_file)
    helper.start()

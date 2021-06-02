import requests
import json
import pandas
from strategy_trading.StrategyTrading.symbolHelper import SymbolHelper
import asyncio


class FairValuePricer():
    def __init__(self, symbol_helper: SymbolHelper):
        self.symbol_helper = symbol_helper
        self.ccy_info = None

    def get_tickers(self):
        tickers = requests.get("https://api.huobi.pro/market/tickers").text
        tickers = json.loads(tickers)
        tickers = tickers['data']
        return tickers

    def get_ccy_vol_in_usd(self):
        if self.ccy_info is None:
            tickers = self.get_tickers()
            df = pandas.DataFrame(tickers)

            # get vol in USDT/HUSD
            df.drop(df[df['symbol'].isin(['hb10', 'huobi10'])].index, inplace=True)
            df.drop(df[~df['symbol'].isin((self.symbol_helper.market_info_by_exchange_symbol['HUOBI_SPOT'].keys()))].index, inplace=True)
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
            df['vol_in_usd'] = df.apply(lambda x: x['amount'] * x['close'] if x['quote'] in ['USDT', 'HUSD'] else x['amount'] * x['close'] * btc_price if x['quote'] == 'BTC' else x['amount'] * x['close'] * eth_price if x['quote'] == 'ETH' else x['amount'] * x['close'] * ht_price if x['quote'] == 'HT' else x['amount'] * x['close'] * trx_price if x['quote'] == 'TRX' else None, axis=1)

            base_vol = df.groupby('base')['vol_in_usd'].sum()
            quote_vol = df.groupby('quote')['vol_in_usd'].sum()
            info = pandas.concat([base_vol.to_frame('base'), quote_vol.to_frame('quote')], axis=1, sort = True).fillna(0).sum(axis=1)

            info.sort_values(ascending=False, inplace=True)

            info = info.to_frame('vol')
            info['ccy'] = info.index
            info['ccy'] = info['ccy'].str.lower()
            info['price'] = info['ccy'].apply(lambda x: df.loc["{}usdt".format(x), 'close'] if "{}usdt".format(x) in df.index else df.loc["{}btc".format(x), 'close'] * df.loc["btcusdt", 'close'] if "{}btc".format(x) in df.index else df.loc["{}husd".format(x), 'close'] if "{}husd".format(x) in df.index else 1 if x == 'husd' else None)
            info['pairs'] = info.index.map(lambda x: len(df[(df['base'] == x) | (df['quote'] == x)]))
            self.ccy_info = info
        return self.ccy_info


if __name__ == '__main__':
    symbol_helper = SymbolHelper()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(symbol_helper.load_symbol_reference())

    pricer = FairValuePricer(symbol_helper)

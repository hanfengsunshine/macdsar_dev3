from optparse import OptionParser
import os
import json
import pandas
from monitor.utils.ding_utils import send_dataframe_as_image
from datetime import datetime, timedelta
pandas.set_option('display.max_rows', 500)
pandas.set_option('display.max_columns', 500)
pandas.set_option('display.width', 1000)
from strategy_trading.STUtils.dbengine import get_db_engine


class SimpleRestHelper():
    def __init__(self, config_file):
        with open(config_file) as json_file:
            api_config = json.load(json_file)
        self.database = api_config['database']
        self.exchange = api_config['exchange']
        self.dingtalk_access_token = api_config['ding']
        self.engine = get_db_engine('data', self.database, is_async=False)

    def get_volume(self, dt: datetime):
        sql = "select * from {}_ts_orders where time >= {} and time < {}".format(
            self.exchange.lower(), int(dt.timestamp() * 1000000), int((dt + timedelta(days=1)).timestamp() * 1000000)
        )
        orders = pandas.read_sql(sql, con = self.engine)
        if not orders.empty:
            orders.sort_values('time', inplace=True)
            orders.drop_duplicates(subset='strategy_order_id', keep='last')

            info_df = pandas.DataFrame(columns=['avgBuy', 'buySize', 'avgSell', 'sellSize', 'volume', 'lastTrade'],
                                       index = pandas.MultiIndex(levels=[[],[]], labels=[[],[]], names=['strategy', 'symbol']))

            for strategy, sub_info in orders.groupby('strategy'):
                for symbol, symbol_orders in sub_info.groupby('symbol'):
                    buy_orders = symbol_orders[symbol_orders['side'] == 'BUY']
                    if not buy_orders.empty:
                        info_df.loc[(strategy, symbol), 'buySize'] = buy_orders['executed_size'].sum()
                        info_df.loc[(strategy, symbol), 'avgBuy'] = (buy_orders['executed_size'] * buy_orders['avg_price']).sum() / buy_orders['executed_size'].sum()

                    sell_orders = symbol_orders[symbol_orders['side'] == 'SELL']
                    if not sell_orders.empty:
                        info_df.loc[(strategy, symbol), 'sellSize'] = sell_orders['executed_size'].sum()
                        info_df.loc[(strategy, symbol), 'avgSell'] = (sell_orders['executed_size'] * sell_orders['avg_price']).sum() / sell_orders['executed_size'].sum()

                    info_df.loc[(strategy, symbol), 'volume'] = (sell_orders['executed_size'] * sell_orders['avg_price']).sum()
                    info_df.loc[(strategy, symbol), 'lastTrade'] = (pandas.to_datetime(symbol_orders['time'].max(), unit = 'us') + timedelta(hours=8)).to_pydatetime().strftime("%m%d %H:%M")
            return info_df

    def notify(self, info_df: pandas.DataFrame):
        # format balance
        #print(balance.style.bar(subset=['valueChange'], align='mid', color=['#5fba7d']).to_html())

        info_df['avgBuy'] = info_df['avgBuy'].apply(lambda x: "{:,.4f}".format(x) if not pandas.isnull(x) else "")
        info_df['avgSell'] = info_df['avgSell'].apply(lambda x: "{:,.4f}".format(x) if not pandas.isnull(x) else "")
        info_df['buySize'] = info_df['buySize'].apply(lambda x: "{:,.2f}".format(x) if not pandas.isnull(x) else "")
        info_df['sellSize'] = info_df['sellSize'].apply(lambda x: "{:,.2f}".format(x) if not pandas.isnull(x) else "")
        info_df['volume'] = info_df['volume'].apply(lambda x: "{:,.2f}".format(x) if not pandas.isnull(x) else "")

        send_dataframe_as_image(self.dingtalk_access_token, info_df.reset_index(), os.path.abspath('{}-{}.png'.format(self.database, self.exchange)), image_tile="{} {} {}".format(self.database, self.exchange, (datetime.utcnow() + timedelta(hours = 8)).strftime('%Y-%m-%d %H:%M HKT')))



if __name__ == '__main__':
    parser = OptionParser()
    parser.add_option("-c", type="string", dest="config", default="~")
    (options, args) = parser.parse_args()
    config_file = os.path.normpath(options.config)

    helper = SimpleRestHelper(config_file)
    today = datetime.now()
    today = datetime(year=today.year, month=today.month, day=today.day)
    balance = helper.get_volume(today)
    #print(balance)
    helper.notify(balance)

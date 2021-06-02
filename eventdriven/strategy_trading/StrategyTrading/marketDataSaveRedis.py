# -*- coding: utf-8 -*-
# @Time    : 2019/12/31 11:13
# @Author  : Nicole
# @File    : marketDataSaveRedis.py
# @Software: PyCharm
# @Description:


import logging
from infrastructure.TradingUtils.rotateHandler import AsyncTimedRotatingFileHandler
import asyncio
from strategy_trading.StrategyTrading.symbolHelper import SymbolHelper
from optparse import OptionParser
import os
from strategy_trading.StrategyTrading.marketDataConnection import MarketDataConnection
from projectX1.utils import redisHelper as rh


class SaveMarketDataConnection(MarketDataConnection):
    def __init__(self, exchange, server, port, symbol_helper: SymbolHelper):
        super(SaveMarketDataConnection, self).__init__(exchange, server, port, symbol_helper)
        self.orderbook_per_symbol = {}

    def on_message(self, message):
        symbol = message['symbol']
        if symbol in self.data_q_per_symbol and message['dataType'] == 'DEPTH':
            self.orderbook_per_symbol[symbol] = message

    async def sub_n_save(self, symbols):
        for s in symbols:
            await self.subscribe(
                global_symbol="SPOT-{}".format(s),
                want_orderbook=True,
                want_trades=False
            )
        while True:
            for s in symbols:
                await asyncio.sleep(.2)
                symbol = self.global_symbol_to_symbol["SPOT-{}".format(s)]
                if not symbol in self.data_q_per_symbol:
                    raise ValueError('{} not subscribed yet'.format(symbol))
                if symbol in self.orderbook_per_symbol:
                    try:
                        depth = self._parse_msg_from_server(self.orderbook_per_symbol[symbol])[-1]
                        data = {
                            'time': depth.exch_timestamp * 1e-6,
                            'bids': [[float(bid[0]), float(bid[1])] for bid in depth.bids],
                            'asks': [[float(ask[0]), float(ask[1])] for ask in depth.asks],
                        }
                        save_key = "{}_{}".format(self.exchange.lower(), s.replace('/', '_').lower())
                        rh.set_marketdata(save_key, data)
                        logging.info(
                            "update orderbook.{} to {} with"
                            " bid1: {} and ask1: {}".format(
                                save_key, data.get("time", "error"),
                                data.get("bids", [])[:1],
                                data.get("asks", [])[:1]
                            )
                        )
                    except Exception as e:
                        self.logger.exception(e)
                        await asyncio.sleep(5)


if __name__ == '__main__':
    parser = OptionParser()
    parser.add_option("-o", type="string", dest="output_path")
    parser.add_option("-p", type="string", dest="port")
    parser.add_option("-e", type="string", dest="exchange")
    parser.add_option("-s", type="string", dest="symbols")
    (options, args) = parser.parse_args()

    exchange = options.exchange
    assert exchange == 'JUMPT'

    port = int(options.port)
    symbols = options.symbols.split(',')

    output_path = os.path.normpath(options.output_path)
    logformatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    log = AsyncTimedRotatingFileHandler(output_path, interval=1, encoding='utf-8')
    log.setLevel(logging.INFO)
    log.setFormatter(logformatter)

    root = logging.getLogger()
    root.setLevel(logging.INFO)
    root.addHandler(log)

    loop = asyncio.get_event_loop()
    symbol_helper = SymbolHelper()
    loop.run_until_complete(symbol_helper.load_symbol_reference())
    md = SaveMarketDataConnection(exchange, '127.0.0.1', port, symbol_helper)
    loop.run_until_complete(md.init(loop))
    loop.run_until_complete(md.sub_n_save(symbols))

    # symbols = ['ETH/USD']
    # loop = asyncio.get_event_loop()
    # symbol_helper = SymbolHelper()
    # loop.run_until_complete(symbol_helper.load_symbol_reference())
    # md = SaveMarketDataConnection('JUMPT', '127.0.0.1', 7402, symbol_helper)
    # loop.run_until_complete(md.init(loop))
    # loop.run_until_complete(md.sub_n_save(symbols))

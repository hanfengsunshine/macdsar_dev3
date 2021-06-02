import logging
import asyncio
from strategy_trading.STUtils.wssClient import BaseWebsocketClient
from strategy_trading.StrategyTrading.symbolHelper import SymbolHelper
from strategy_trading.StrategyTrading.marketData import CorrelationID, QUOTE


class MarketDataConnection():
    def __init__(self, exchange, server, port, symbol_helper: SymbolHelper):
        self.logger = logging.getLogger("{}-{}".format(self.__class__.__name__, exchange))
        self.exchange = exchange
        self.server = server
        self.port = port
        self.ws = BaseWebsocketClient("http://{}:{}".format(server, port), True,
                                      callback_when_data=self.on_message,
                                      callback_after_connection=self._re_subscribe_all,
                                      retry_seconds=10)
        self.requests = {}
        self.data_q_per_symbol = {}
        self.symbol_helper = symbol_helper
        self.symbol_to_global_symbol = {}
        self.global_symbol_to_symbol = {}

    async def init(self, loop):
        await self.ws.connect_to_server()
        asyncio.ensure_future(self.ws.receive_data(), loop=loop)

    def on_message(self, message):
        try:
            symbol = message['symbol']
            if symbol in self.data_q_per_symbol:
                self.data_q_per_symbol[symbol].put_nowait(message)
        except asyncio.QueueFull:
            self.logger.warning("message full, drop {}".format(message))
        except Exception as e:
            self.logger.exception(e)

    async def _re_subscribe_all(self):
        reqlist = [{
            'symbol': symbol, 'req_quotes': reqs
        } for symbol, reqs in self.requests.items()]
        await self.ws.send_message(reqlist)

    async def subscribe(self, global_symbol, req_quotes: list = None):
        if not isinstance(req_quotes, list):
            return
        symbol = self.global_symbol_to_symbol.get(global_symbol)
        if not symbol:
            info = self.symbol_helper.get_info(global_symbol, self.exchange)
            symbol = info['symbol']
            self.symbol_to_global_symbol[symbol] = global_symbol
            self.global_symbol_to_symbol[global_symbol] = symbol
            self.requests[symbol] = []
        request = {'symbol': symbol, 'req_quotes': []}
        for req in req_quotes:
            if not isinstance(req, tuple):
                continue
            if req in self.requests[symbol]:
                self.logger.warning("{} has been subscribed. ignore".format(req))
                continue
            self.requests[symbol].append(req)
            request['req_quotes'].append(req)
        self.data_q_per_symbol[symbol] = asyncio.Queue()
        await self.ws.send_message([request])

    def _parse_msg_from_server(self, msg):
        # TODO: data format validation
        if msg['dataType'] == 'QUOTE':
            quote = QUOTE(id=msg['id'], expire_timestamp=msg['expire_timestamp'],
                          **msg['quote'], correlation_id=CorrelationID(msg['correlationID']))
            return [quote]

    async def get_update(self, global_symbol, symbol):
        assert global_symbol or symbol
        if symbol is None:
            symbol = self.global_symbol_to_symbol[global_symbol]

        if symbol in self.data_q_per_symbol:
            data = await self.data_q_per_symbol[symbol].get()
            data = self._parse_msg_from_server(data)
            return data
        raise ValueError('{} not subscribed yet'.format(symbol))

    async def get_all_updates(self, global_symbol=None, symbol=None):
        assert global_symbol or symbol
        if symbol is None:
            symbol = self.global_symbol_to_symbol[global_symbol]

        if symbol in self.data_q_per_symbol:
            updates = []
            _update = await self.data_q_per_symbol[symbol].get()
            _update = self._parse_msg_from_server(_update)
            updates += _update
            while not self.data_q_per_symbol[symbol].empty:
                _update = await self.data_q_per_symbol[symbol].get()
                _update = self._parse_msg_from_server(_update)
                updates += _update
            return updates
        raise ValueError('{} not subscribed yet'.format(symbol))


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    symbol_helper = SymbolHelper()
    loop.run_until_complete(symbol_helper.load_symbol_reference())

    from datetime import datetime


    async def t(loop):
        md = MarketDataConnection('CUMBERLAND', '127.0.0.1', 7501, symbol_helper)
        await md.init(loop)
        await md.subscribe(
            global_symbol="SPOT-BTC/USD",
            req_quotes=[(10000, 'USD'), (100000, 'USD')]
        )

        while True:
            updates = await md.get_all_updates("SPOT-BTC/USD", None)
            print(datetime.now().timestamp(), updates)


    loop.run_until_complete(t(loop))

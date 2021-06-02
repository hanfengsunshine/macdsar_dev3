import logging
from strategy_trading.STUtils.wssClient import BaseWebsocketClient
import asyncio
from strategy_trading.StrategyTrading.symbolHelper import SymbolHelper
from strategy_trading.StrategyTrading.marketData import MarketTrade, OrderBookDepth, OrderBookDiff, CorrelationID, Ticker, Kline
from strategy_trading.StrategyTrading.order import Side
from datetime import datetime, timedelta
import json


class MarketDataConnection():
    def __init__(self, exchange, server, port, symbol_helper: SymbolHelper, dma = False):
        self.logger = logging.getLogger("{}-{}".format(self.__class__.__name__, exchange))
        self.exchange = exchange
        self.dma = dma

        if not dma:
            self.server = server
            self.port = port
            self.ws = BaseWebsocketClient("http://{}:{}".format(server, port), True,
                                          callback_when_data=self.on_message,
                                          callback_after_connection=self._re_subscribe_all,
                                          retry_seconds=10)
        else:
            assert exchange in ['HUOBI_CONTRACT', 'HUOBI_SPOT', 'HUOBI_SWAP']
        self.requests = []  # [{"symbol": "btcusdt", "orderBook": True, "trades": True}]
        self.data_q_per_symbol = {}
        self.symbol_helper = symbol_helper
        self.symbol_to_global_symbol = {}
        self.global_symbol_to_symbol = {}

    async def init(self, loop, enable_kline = True):
        if not self.dma:
            await self.ws.connect_to_server()
            asyncio.ensure_future(self.ws.receive_data(), loop=loop)
        else:
            if self.exchange == 'HUOBI_CONTRACT':
                from infrastructure.MarketDataService.HUOBI.huobiContractAdapter import HuobiContractMarketDataAdapter
                self.adapter = HuobiContractMarketDataAdapter(self.on_message, enable_kline = enable_kline)
                await self.adapter.start(loop)
            elif self.exchange == 'HUOBI_SPOT':
                from infrastructure.MarketDataService.HUOBI.huobiSpotAdapter import HuobiSpotMarketDataAdapter
                self.adapter = HuobiSpotMarketDataAdapter(self.on_message, enable_kline = enable_kline)
                await self.adapter.start(loop)
            elif self.exchange == 'HUOBI_SWAP':
                from infrastructure.MarketDataService.HUOBI.huobiSwapAdapter import HuobiSwapMarketDataAdapter
                self.adapter = HuobiSwapMarketDataAdapter(self.on_message, enable_kline=enable_kline)
                await self.adapter.start(loop)
            else:
                raise ValueError('exchange {} not supported'.format(self.exchange))

    def on_message(self, message):
        try:
            symbol = message['symbol'] if 'symbol' in message else message['Symbol']
            if symbol in self.data_q_per_symbol:
                self.data_q_per_symbol[symbol].put_nowait(message)
        except asyncio.QueueFull:
            self.logger.warning("message full, drop {}".format(message))
        except Exception as e:
            self.logger.exception(e)

    async def _re_subscribe_all(self):
        await self.ws.send_message(self.requests)

    def _get_huobi_contract_symbol(self, global_symbol, curr_time: datetime = None):
        if curr_time is None:
            curr_time = datetime.utcnow()

        info = self.symbol_helper.get_info(global_symbol, self.exchange)
        remaining_time = info['expiry_datetime'] - curr_time
        if remaining_time <= timedelta(seconds=0):
            raise ValueError("cannot get huobi native md symbol for {}".format(global_symbol))
        if remaining_time <= timedelta(days=7):
            return "{}_CW".format(info['price_ccy'])
        elif remaining_time <= timedelta(days=14):
            return "{}_NW".format(info['price_ccy'])
        return "{}_CQ".format(info['price_ccy'])

    async def subscribe(self, global_symbol, want_orderbook: bool = True, book_level=None, want_trades: bool = True,
                        want_ticker: bool = False, want_kline=False, kline_freq_seconds: list = None, want_diff: bool = True):
        info = self.symbol_helper.get_info(global_symbol, self.exchange)
        if self.exchange != 'HUOBI_CONTRACT':
            symbol = info['symbol']
        else:
            symbol = self._get_huobi_contract_symbol(global_symbol)
        if symbol not in self.symbol_to_global_symbol:
            self.symbol_to_global_symbol[symbol] = global_symbol
            self.global_symbol_to_symbol[global_symbol] = symbol

        # check if the symbol has been subscribed
        for reqeust in self.requests:
            if reqeust['symbol'] == symbol:
                self.logger.warning("{} has been subscribed. ignore".format(symbol))
                return

        reqeust = {
            "symbol": symbol,
            "orderBook": want_orderbook,
            "bookLevel": book_level,
            "bookDiff": want_diff,
            "trades": want_trades,
            'ticker': want_ticker,
            'kline': want_kline,
            'klineFreq': kline_freq_seconds
        }
        self.requests.append(reqeust)
        self.data_q_per_symbol[symbol] = asyncio.Queue()
        if not self.dma:
            await self.ws.send_message([reqeust])
        else:
            self.adapter.on_client_request({
                'connection_id': 0,
                'data': json.dumps(reqeust)
            })

    def _parse_msg_from_server(self, msg):
        # TODO: data format validation
        if msg['dataType'] == 'DEPTH':
            depth = OrderBookDepth(bids=msg['bids'] if msg['bids'] is not None else [],
                                   asks=msg['asks'] if msg['asks'] is not None else [],
                                   exch_timestamp=msg['exchTimestamp'],
                                   correlation_id=CorrelationID(msg['correlationID']),
                                   seq_id=msg['seq'])
            return [depth]
        elif msg['dataType'] == 'DIFF':
            depth = OrderBookDiff(bids=msg['bids'] if msg['bids'] is not None else [],
                                   asks=msg['asks'] if msg['asks'] is not None else [],
                                   exch_timestamp=msg['exchTimestamp'],
                                   correlation_id=CorrelationID(msg['correlationID']),
                                   seq_id = msg['seq'])
            return [depth]
        elif msg['dataType'] == 'TRADES':
            trades = msg['trades']
            if isinstance(trades, list):
                trades = [MarketTrade(
                    price=trade['price'],
                    size=trade['size'],
                    side=Side(trade['side']),
                    exch_timestamp=msg['exchTimestamp'],
                    correlation_id=CorrelationID(msg['correlationID']),
                    seq_id=msg['seq']
                ) for trade in trades]
                return trades
            else:
                trade = MarketTrade(
                    price=trades['price'],
                    size=trades['size'],
                    side=Side(trades['side']),
                    exch_timestamp=msg['exchTimestamp'],
                    correlation_id=CorrelationID(msg['correlation_id']),
                    seq_id=msg['seq']
                )
                return [trade]
        elif msg['dataType'] == 'TICKER':
            ticker = Ticker(
                bid1p=msg['ticker']['bid1p'],
                bid1s=msg['ticker']['bid1s'],
                ask1p=msg['ticker']['ask1p'],
                ask1s=msg['ticker']['ask1s'],
                exch_timestamp=msg['exchTimestamp'],
                correlation_id=CorrelationID(msg['correlationID']),
                seq_id=msg['seq']
            )
            return [ticker]
        elif msg['dataType'] == 'KLINE':
            kline = Kline(
                freq_seconds=msg['freq'],
                start_timestamp=msg['kline']['time'],
                open_price=msg['kline']['open'],
                high=msg['kline']['high'],
                low=msg['kline']['low'],
                close_price=msg['kline']['close'],
                volume=msg['kline']['volume'],
                amount=msg['kline']['amount'],
                count=msg['kline']['count'],
                exch_timestamp=msg['exchTimestamp'],
                correlation_id=CorrelationID(msg['correlationID'])
            )
            return [kline]

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
        md = MarketDataConnection('JUMPT', '127.0.0.1', 7401, symbol_helper)
        await md.init(loop)
        await md.subscribe(
            global_symbol="SPOT-BTC/JPY",
            want_orderbook=True,
            want_trades=False
        )

        while True:
            updates = await md.get_all_updates("SPOT-BTC/JPY", None)
            print(datetime.now().timestamp(), updates)


    loop.run_until_complete(t(loop))

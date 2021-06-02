import os
import zmq
import json
import time
import arrow
#import uvloop
import asyncio
import argparse

from loguru import logger
from decimal import Decimal, ROUND_UP, ROUND_DOWN
from collections import defaultdict
from zmq.asyncio import Context

from strategy_trading.easy_strategy.utils import get_current_us, decimal_to_readable
from strategy_trading.easy_strategy.env import StrategyEnv
from strategy_trading.easy_strategy.strategy_base import StrategyImplBase
from strategy_trading.easy_strategy.datafeed import FullLevelOrderBook, FixedLevelOrderBook, TopOrderBook, Trade, Kline, Ticker, IndexTicker
from strategy_trading.easy_strategy.data_type import Side, OrderType, PositionEffect
from strategy_trading.easy_strategy.instrument_manager import Instrument


class TestStrategy(StrategyImplBase):
    def __init__(self):
        super(TestStrategy, self).__init__()

    # async def on_raw_market_data(self, exchange:str, data: dict):
    #     print(data)

    async def on_fixed_level_orderbook(self, ob: FixedLevelOrderBook):
        print('on_fixed_level_orderbook. exchange={} symbol={} bid/ask={}/{}'.format(ob.instrument.exchange, ob.instrument.global_symbol, ob.bids()[0][0], ob.asks()[0][0]))

    async def on_full_level_orderbook(self, ob: FullLevelOrderBook):
        bids = ob.bids()
        asks = ob.asks()

        print('bid/ask={}/{}'.format(bids[0][0], asks[0][0]))

        if bids[0][0] >=  asks[0][0]:
            print('fuck')

    async def on_top_orderbook(self, ob: TopOrderBook):
        print('on_top_orderbook. exchange={} symbol={} bid/ask={}/{}'.format(ob.instrument.exchange, ob.instrument.global_symbol, ob.best_bid[0], ob.best_ask[0]))

    async def on_trade(self, trades):
        for trade in trades:
            print('on_trade. ts={} exchange={} symbol={} side={} price={} qty={}'.format(trade.exchange_timestamp, trade.instrument.exchange, trade.instrument.global_symbol, trade.side.name, trade.price, trade.quantity))

    async def on_kline(self, kline):
        print('on_kline. exchange={} symbol={} last={}'.format(kline.instrument.exchange, kline.instrument.global_symbol, kline.close))

    async def start(self):
        # await self.subscribe_fixed_orderbook(self.get_instrument('HUOBI_CONTRACT', 'FUTU-BTC/USD-20210326'), 20)
        # await self.subscribe_fixed_orderbook(self.get_instrument('OKEX_CONTRACT', 'FUTU-BTC/USD-20210326'), 5)
        # await self.subscribe_fixed_orderbook(self.get_instrument('BINANCE_CONTRACT', 'FUTU-BTC/USD-20210326'), 20)
        # await self.subscribe_trade(self.get_instrument('BINANCE_CONTRACT', 'FUTU-BTC/USD-20210326'))
        await self.subscribe_fixed_orderbook(self.get_instrument('BINANCE_SWAP_USDT', 'SWAP-ETH/USDT'), 20)

        # await self.subscribe_fixed_orderbook(self.get_instrument('HUOBI_SWAP', 'SWAP-XRP/USD'), 20)
        # await self.subscribe_fixed_orderbook(self.get_instrument('HUOBI_SPOT', 'SPOT-XRP/USDT'), 20)
        # await self.subscribe_full_orderbook(self.get_instrument('COINBASE_SPOT', 'SPOT-BTC/USD'))
        # await self.subscribe_trade(self.get_instrument('BINANCE_SWAP', 'SWAP-BTC/USDT'))
        # await self.subscribe_trade(self.get_instrument('BINANCE_SPOT', 'SPOT-BTC/USDT'))

        await self.subscribe_full_orderbook(self.get_instrument('HUOBI_SPOT', 'SPOT-BTC/USDT'))
        # await self.subscribe_trade(self.get_instrument('HUOBI_SPOT', 'SPOT-BTC/USDT'))
        # await self.subscribe_kline(self.get_instrument('HUOBI_SPOT', 'SPOT-BTC/USDT'), 3600)

        # await self.subscribe_top_orderbook(self.get_instrument('FTX_SWAP_USD', 'SWAP-BTC/USD'))
        # await self.subscribe_trade(self.get_instrument('FTX_SWAP_USD', 'SWAP-BTC/USD'))

        # await self.subscribe_kline(self.get_instrument('HUOBI_INDEX', 'INDEX-BTC/USD'), 60)
        # await self.subscribe_kline(self.get_instrument('BINANCE_INDEX', 'INDEX-BTC/USD'), 60)

        # await self.subscribe_fixed_orderbook(self.get_instrument('OKEX_SPOT', 'SPOT-BTC/USDT'), 5)

        # await self.subscribe_kline(self.get_instrument('HUOBI_CONTRACT', 'FUTU-BTC/USD-20210326'), 3600)

        # await self.subscribe_top_orderbook(self.get_instrument('FTX_CONTRACT', 'FUTU-BTC/USD-0326'))

        # await self.subscribe_top_orderbook(self.get_instrument('OKEX_CONTRACT', 'FUTU-BTC/USD-20210625'))
        # await self.subscribe_fixed_orderbook(self.get_instrument('OKEX_CONTRACT', 'FUTU-BTC/USD-20210625'), 5)
        # await self.subscribe_kline(self.get_instrument('OKEX_CONTRACT', 'FUTU-BTC/USD-20210625'), 3600)
        # await self.subscribe_trade(self.get_instrument('OKEX_CONTRACT', 'FUTU-BTC/USD-20210625'))

        # await self.subscribe_top_orderbook(self.get_instrument('OKEX_SPOT', 'SPOT-BTC/USDT'))
        # await self.subscribe_fixed_orderbook(self.get_instrument('OKEX_SPOT', 'SPOT-BTC/USDT'), 5)
        # await self.subscribe_kline(self.get_instrument('OKEX_SPOT', 'SPOT-BTC/USDT'), 3600)
        # await self.subscribe_trade(self.get_instrument('OKEX_SPOT', 'SPOT-BTC/USDT'))

        # await self.subscribe_top_orderbook(self.get_instrument('OKEX_SWAP', 'SWAP-BTC/USD'))
        # await self.subscribe_fixed_orderbook(self.get_instrument('OKEX_SWAP', 'SWAP-BTC/USD'), 5)
        # await self.subscribe_kline(self.get_instrument('OKEX_SWAP', 'SWAP-BTC/USD'), 3600)
        # await self.subscribe_trade(self.get_instrument('OKEX_SWAP', 'SWAP-BTC/USD'))

        # await self.subscribe_full_orderbook(self.get_instrument('COINBASEPRO_SPOT', 'SPOT-BTC/USDC'))
        # await self.subscribe_trade(self.get_instrument('COINBASEPRO_SPOT', 'SPOT-BTC/USDC'))


if __name__ == "__main__":
    #uvloop.install()

    parser = argparse.ArgumentParser()
    parser.add_argument('--server', type=str, required=True)
    parser.add_argument('--exchanges', required=False, nargs='*', action='store', type=str)
    args = parser.parse_args()

    strategy_config = json.load(open(args.server))
    env = StrategyEnv(strategy_config)

    strategy = TestStrategy()
    env.init(strategy, exchange_list=args.exchanges if args.exchanges else None)
    env.run()

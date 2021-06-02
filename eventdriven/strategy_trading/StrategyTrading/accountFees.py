from strategy_trading.STUtils.dbengine import get_db_engine, read_sql
from decimal import Decimal
import asyncio
import pandas


async def get_account_fees(exchange, account, global_symbol):
    engine = get_db_engine("data", "trading")
    possible_fees = await read_sql("select * from account_fees where exchange = '{}'".format(exchange), engine)
    # check if any fee specifically for this symbol
    for item in possible_fees:
        if global_symbol == item['global_symbol']:
            return Decimal("{:.8f}".format(item['taker_fee'])), Decimal("{:.8f}".format(item['maker_fee']))

    for item in possible_fees:
        if item['global_symbol'] == 'default':
            return Decimal("{:.8f}".format(item['taker_fee'])), Decimal("{:.8f}".format(item['maker_fee']))

    return None, None

def get_account_fees_sync(exchange, account, global_symbol):
    engine = get_db_engine("data", "trading", is_async=False)
    possible_fees = pandas.read_sql("select * from account_fees where exchange = '{}'".format(exchange), con = engine)
    # check if any fee specifically for this symbol
    for index, item in possible_fees.iterrows():
        if global_symbol == item['global_symbol']:
            return Decimal("{:.8f}".format(item['taker_fee'])), Decimal("{:.8f}".format(item['maker_fee']))

    for index, item in possible_fees.iterrows():
        if item['global_symbol'] == 'default':
            return Decimal("{:.8f}".format(item['taker_fee'])), Decimal("{:.8f}".format(item['maker_fee']))

    return None, None

async def get_safe_account_fee(exchange, account, global_symbol):
    taker_fee, maker_fee = await get_account_fees(exchange, account, global_symbol)
    if taker_fee is not None and maker_fee is not None:
        if exchange in ['BINANCE_SPOT', 'BINANCE_CONTRACT', 'BINANCE_SWAP']:
            maker_fee = max(Decimal("0"), (taker_fee + maker_fee) / Decimal("2"))
    return taker_fee, maker_fee


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(get_safe_account_fee("BINANCE_CONTRACT", "ambn", "SWAP-BTC/USD"))
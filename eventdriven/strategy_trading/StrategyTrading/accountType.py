from strategy_trading.ExchangeHelper.constants import EXCHANGE_NAME_BINANCE_SWAP, EXCHANGE_NAME_OKEX_CONTRACT, EXCHANGE_NAME_BINANCE_CONTRACT, EXCHANGE_NAME_BINANCE_SPOT


def get_account_type(exchange, cross_margin = False):
    if exchange in ["HUOBI_SWAP", "OKEX_SWAP", EXCHANGE_NAME_BINANCE_SWAP]:
        # TODO: given that OKEX_SWAP does not share inventory between multiple USDT swap accounts, we have to re-think about the design for this case
        return 'swap'
    if exchange in ['HUOBI_CONTRACT', EXCHANGE_NAME_OKEX_CONTRACT, EXCHANGE_NAME_BINANCE_CONTRACT]:
        # TODO: given that OKEX_SWAP does not share inventory between multiple USDT swap accounts, we have to re-think about the design for this case
        return 'contract'
    if exchange == 'HUOBI_SPOT':
        if cross_margin == "cross-margin" or cross_margin is True:
            return 'cross-margin'
        elif cross_margin == 'margin':
            return "margin"
        else:
            return 'spot'
    if exchange in ['OKEX_SPOT', EXCHANGE_NAME_BINANCE_SPOT]:
        return 'spot'
    raise ValueError('{} balance update is not supported'.format(exchange))
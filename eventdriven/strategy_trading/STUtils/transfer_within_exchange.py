# -*- coding: utf-8 -*-
# @Time    : 2020/9/15 18:42
# @Author  : Nicole
# @File    : transfer_within_exchange.py
# @Software: PyCharm
# @Description:

from strategy_trading.STUtils.transferHelper import TransferClient

if __name__ == '__main__':
    from optparse import OptionParser

    parser = OptionParser()
    parser.add_option("-p", type="string", dest="person")
    parser.add_option("-e", type="string", dest="exchange")
    parser.add_option("-a", type="string", dest="from_acct")
    parser.add_option("-t", type="string", dest="from_acct_type")
    parser.add_option("-A", type="string", dest="to_acct")
    parser.add_option("-T", type="string", dest="to_acct_type")
    parser.add_option("-c", type="string", dest="ccy")
    parser.add_option("-q", type="string", dest="amount")
    parser.add_option("-s", type="string", dest="from_symbol", default=None)
    parser.add_option("-S", type="string", dest="to_symbol", default=None)
    (options, args) = parser.parse_args()

    t = TransferClient(options.person)
    t.transfer(exchange=options.exchange,from_acct=options.from_acct, from_acct_type=options.from_acct_type,
               to_acct=options.to_acct, to_acct_type=options.to_acct_type,
               ccy=options.ccy, amount=float(options.amount), from_symbol=options.from_symbol,
               to_symbol=options.to_symbol)

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
    parser.add_option("-e", type="string", dest="from_exchange")
    parser.add_option("-a", type="string", dest="from_acct")
    parser.add_option("-E", type="string", dest="to_exchange")
    parser.add_option("-A", type="string", dest="to_acct")
    parser.add_option("-c", type="string", dest="ccy")
    parser.add_option("-q", type="string", dest="amount")
    (options, args) = parser.parse_args()

    t = TransferClient(options.person)
    t.transfer_across_exchanges(from_exchange=options.from_exchange, from_acct=options.from_acct,
                                to_exchange=options.to_exchange, to_acct=options.to_acct, ccy=options.ccy,
                                amount=float(options.amount))


import json,urllib,time,math
from urllib.parse import urlencode


def get_huobi_data():
   
    headers = {'User-Agent':'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:23.0) Gecko/20100101 Firefox/23.0'}

    url_huobi = 'https://api.huobi.pro/market/history/kline'
    params_h_btc_uncode = {
      'symbol' : 'btcusdt',
      'period' : '1min',
      'size' : 1
    }
    params_h_btc = urlencode(params_h_btc_uncode)

    params_h_eth_uncode = {
      'symbol' : 'ethusdt',
      'period' : '1min',
      'size' : 1
    }
    params_h_eth = urlencode(params_h_eth_uncode)

    #f_h_btc = urllib.request.urlopen('%s?%s' % (url_huobi, params_h_btc))
    req_btc = urllib.request.Request(url='%s?%s' %(url_huobi, params_h_btc), headers=headers)
    f_h_btc = urllib.request.urlopen(req_btc)

    #f_h_eth = urllib.request.urlopen('%s?%s' % (url_huobi, params_h_eth))
    req_eth = urllib.request.Request(url='%s?%s' %(url_huobi, params_h_eth), headers=headers)
    f_h_eth = urllib.request.urlopen(req_eth)
    print(f_h_btc)
    
    data_api_h_btc = f_h_btc.read()
    data_api_h_eth = f_h_eth.read()

    h_btc = json.loads(data_api_h_btc)
    h_eth = json.loads(data_api_h_eth)
    # print(h_btc["mid_price"],b_eth["mid_price"])


    return h_btc['data'][0]['close'],h_eth['data'][0]['close']


h_btc_price,h_eth_price = get_huobi_data()


def realtime_data():
    h_btc_price, h_eth_price = get_huobi_data()
    print(f"{h_eth_price}")

while(1): 
    realtime_data()
    time.sleep()


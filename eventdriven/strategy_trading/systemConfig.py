import socket

IPS = {
    "T7": "172.26.88.88",
    "T8": "172.26.89.9",
    "T9": "172.26.89.32",
    "T14": "172.26.88.93",
    "T10": "172.26.88.46",
    "T15": "172.26.89.193",
    "DEV3": "172.28.0.25",
    "DEV4": "172.28.0.47",
    "finance01": "172.26.89.141",
    "finance02": "172.26.89.245"
}

OMS_PORTS = {
    "T7": {
        "HUOBI_SPOT": {
            "liqamtheme": 6111
        },
        "BINANCE_SPOT": {
            "ambntheme": 6517
        },
        "COINBASEPRO_SPOT": {
            "hbamcoinbasepro": 6420
        }
    },
    "T8": {
        "HUOBI_SPOT": {
            "liqamfrarb1": 6123,
            "liqamarb1": 6125,
            "liqamarb2": 6126,
            "liqamfrarb2": 6127,
            "liqamfrarb3": 6128,
            "liqamfrarbeth1": 6129,
            "liqamarbeth1": 6130,
            "liqambcarb1": 6131,
            "liqamarbusdt1": 6132,
            "liqamarbusdt2": 6133,
            "liqamarbusdt3": 6134,
            "liqamarbusdt4": 6135,
            "liqamarbusdt5": 6136,
            "liqamarbusdt6": 6137,
            "liqamarbusdt7": 6138,
            "liqamwjarb": 6140,
            "liqamwjarb1": 6142,
            "liqamwjarb2": 6143,
            "liqamwjarb3": 6145
        },
        "HUOBI_CONTRACT": {
            "liqamfrarb1": 6310,
            "liqambcarb1": 6312,
            "liqamfutpsbtc1": 6313,
            "liqamarbfutbtcusd1": 6316,
            "liqamwjarb": 6318,
            "liqamwjarb1": 6325,
            "liqamwjarb2": 6326,
            "liqamarbusdt7": 6328,
            "liqamwjarb3": 6329
        },
        "HUOBI_SWAP": {
            "liqamfrarb1": 6413,
            "liqamarb1": 6415,
            "liqamfrarb2": 6416,
            "liqamfrarb3": 6417,
            "liqamusdtps": 6418,
            "liqamfrarbeth1": 6420,
            "liqamarbeth1": 6421,
            "liqamfutpsbtc1": 6422,
            "liqamfrarbeth2": 6424,
            "liqamfrarbeth3": 6425,
            "liqamfrarbeth4": 6426,
            "liqamarbusdt1": 6427,
            "liqamarbusdt2": 6428,
            "liqamarbusdt3": 6429,
            "liqamarbusdt4": 6430,
            "liqamarbusdt5": 6431,
            "liqamarbusdt6": 6432,
            "liqamarbusdt7": 6433,
            "liqamwjarb": 6434,
            "liqamwjarb1": 6442,
            "liqamwjarb2": 6443,
            "liqamwjarb3": 6444
        },
        "OKEX_CONTRACT": {
            "hbamokexarbfutbtcusd1": 6701
        },
        "OKEX_SWAP": {
            "hbamokexarb1": 6602,
            "hbamokexarbusdt1": 6603,
            "hbamokexusdtps": 6604,
            "hbamokexarbeth1": 6605
        },
        "BINANCE_SPOT": {
            "ambnarbusdt1": 6201,
            "ambnarbusdt2": 6202,
            "ambnarbusdt3": 6203,
            "ambnarbusdt4": 6204,
            "ambnarbusdt5": 6205,
            "ambnarbusdt6": 6206,
            "ambnarbusdt7": 6207,
            "ambnjyarb1": 6209,
            "ambnwjarb1": 6210
        },
        "BINANCE_CONTRACT": {
            "hbambinancearbfutbtcusd1": 6801,
            "ambnarbfutusd1": 6802,
            "ambnarbusd1": 6803,
            "ambnwjarb": 7802,
            "ambnjyarb1": 6808,
            "ambnwjarb1": 6809
        },
        "BINANCE_SWAP": {
            "hbambinancearbfutbtcusd1": 6801,
            "ambnarbfutusd1": 6802,
            "ambnarbusd1": 6803,
            "ambnwjarb": 7802,
            "ambnjyarb1": 6808,
            "ambnwjarb1": 6809
        },
        "BINANCE_SWAP_USDT": {
            "hbambinancearb1": 6511,
            "hbambinanceusdtps": 6512,
            "ambnarbusdt1": 6515,
            "ambnarbusdt2": 6516,
            "ambnarbusdt3": 6517,
            "ambnarbusdt4": 6518,
            "ambnarbusdt5": 6519,
            "ambnarbusdt6": 6520,
            "ambnarbusdt7": 6521,
            "ambnwjarb": 7502,
            "ambnjyarb1": 6524,
            "ambnwjarb1": 6525
        }
    },
    "T9": {
        "HUOBI_SPOT": {
            "liqam_triarb": 6104,
            "liqambcdarb1": 6105
        },
        "HUOBI_CONTRACT": {
            "liqambcarb1": 6312,
            "liqamfutpsbtc1": 6313,
            "liqamfutpsbtc2": 6314,
            "liqamfutpsbtc3": 6315,
            "liqamfutpsbtc4": 6316,
            "liqambcarb2": 6317,
            "liqambcdarb1": 6318,
            "liqambcdarb2": 6319,
            "liqambcdarb3": 6320
        },
        "HUOBI_SWAP": {
            "liqamfutpsbtc1": 6422,
            "liqamfutpsbtc2": 6423,
            "liqamfutpsbtc3": 6424,
            "liqamfutpsbtc4": 6425,
            "liqambcarb1": 6426,
            "liqambcarb2": 6427,
            "liqambcdarb1": 6428,
            "liqambcdarb2": 6429,
            "liqambcdarb3": 6430
        }
    },
    "T10": {
        "HUOBI_SPOT": {
            "broproarb": 6104
        },
        "HUOBI_SWAP": {
            "broproarb": 6420
        },
        "HUOBI_CONTRACT": {
            "broproarb": 6210
        },
        "BINANEC_SPOT": {
            "brobnarb": 6610
        },
        "BINANCE_SWAP_USDT": {
            "brobnarb": 6710
        },
        "BINANCE_CONTRACT": {
            "brobnarb": 6810
        }
    },
    "T14": {
        "HUOBI_SPOT": {
            "profitbigcoinsub3": 6108,
            "profitbigcoinsub4": 6109,
            "profitbigcoinsub5": 6111,
            "profitbigcoinsub6": 6112,
            "gridmmhico4": 6110,
            "gridmmhf1": 6113,
            "gridmmhf2": 6114,
            "gridmmhf3": 6115
        },
        "HUOBI_SWAP": {
            "perpcmm1": 6004,
            "perpcmmTrend": 6006
        }
    },
    "T15": {
        "HUOBI_SPOT": {
            "liqamarbusdt7": 6138,
            "liqamwjarb": 6140,
            "liqamwjarb1": 6142,
            "liqamwjarb2": 6143,
            "liqamwjarb3": 6145
        },
        "HUOBI_CONTRACT": {
            "liqamwjarb": 6318,
            "liqamwjarb1": 6325,
            "liqamwjarb2": 6326,
            "liqamarbusdt7": 6328,
            "liqamwjarb3": 6329
        },
        "HUOBI_SWAP": {
            "liqamarbusdt7": 6433,
            "liqamwjarb": 6434,
            "liqamwjarb1": 6442,
            "liqamwjarb2": 6443,
            "liqamwjarb3": 6444
        },
        "BINANCE_SPOT": {
            "ambnarbusdt7": 6207,
            "ambnjyarb1": 6209,
            "ambnwjarb1": 6210
        },
        "BINANCE_CONTRACT": {
            "ambnwjarb": 7802,
            "ambnjyarb1": 6808,
            "ambnwjarb1": 6809
        },
        "BINANCE_SWAP": {
            "ambnwjarb": 7802,
            "ambnjyarb1": 6808,
            "ambnwjarb1": 6809
        },
        "BINANCE_SWAP_USDT": {
            "ambnarbusdt7": 6521,
            "ambnwjarb": 7502,
            "ambnjyarb1": 6524,
            "ambnwjarb1": 6525
        }
    },
    "DEV3": {
        "HUOBI_SPOT": {
            "hbtest1am1": 6524
        },
        "HUOBI_SWAP": {
            "hbtest1am1": 6491,
        },
        "HUOBI_CONTRACT": {
            "hbtest1am1": 6601,
        },
        "BINANCE_SWAP_USDT": {
            "binancetest1am1": 6561,
        },
        "BINANCE_SWAP": {
            "binancetest1am1": 6565,
        },
        "BINANCE_CONTRACT": {
            "binancetest1am1": 6565
        },
        "BINANCE_SPOT": {
            "binancetest1am1": 6516,
            "binancetest1": 6513
        },
        "OKEX_SPOT": {
            "okextest1": 6701
        }
    },
    "DEV4": {
        "HUOBI_SPOT": {
            "CCRMBTC": 6200
        },
        "BINANCE_SPOT": {
            "binancemm3": 6210
        }
    },
    "finance01": {
        "HUOBI_SPOT": {
            "26346686": 8000
        }
    },
    "finance02":{
        "HUOBI_SPOT": {
            "26346686": 8000
        }
    }
}

def get_oms_port(exchange, account):
    private_ip = socket.gethostbyname(socket.gethostname())
    server_name = {v: k for k, v in IPS.items()}.get(private_ip)
    assert isinstance(server_name, str)
    assert server_name in OMS_PORTS
    return OMS_PORTS[server_name][exchange][account]

BALANCE_PORTS = {
    "T8": {
        "HUOBI": {
            "liqamfrarb1": 5810,
            "liqamarb1": 5811,
            "liqamarb2": 5812,
            "liqamfrarb2": 5813,
            "liqamfrarb3": 5814,
            "liqamfrarbeth1": 5815,
            "liqamarbeth1": 5816,
            "liqambcarb1": 5817,
            "liqamarbusdt1": 5818,
            "liqamarbusdt2": 5819,
            "liqamarbusdt3": 5820,
            "liqamarbusdt4": 5821,
            "liqamarbusdt5": 5822,
            "liqamarbusdt6": 5823,
            "liqamarbusdt7": 5824
        },
        "BINANCE": {
            "ambnarbusdt1": 5860,
            "ambnarbusdt2": 5861,
            "ambnarbusdt3": 5862,
            "ambnarbusdt4": 5863,
            "ambnarbusdt5": 5864,
            "ambnarbusdt6": 5865,
            "ambnarbusdt7": 5866
        }
    },
    "DEV3": {
        "HUOBI": {
            "hbtest1am1": 5800
        },
        "BINANCE": {
            "binancetest1am1": 5810
        }
    }
}


def get_balance_port(exchange, account):
    private_ip = socket.gethostbyname(socket.gethostname())
    server_name = {v: k for k, v in IPS.items()}.get(private_ip)
    assert isinstance(server_name, str)
    assert server_name in BALANCE_PORTS
    return BALANCE_PORTS[server_name][exchange][account]

from typing import List
from decimal import Decimal
from datetime import datetime
from optparse import OptionParser
from ApolloClient.client import ApolloClient
from strategy_trading.STUtils.dbengine import get_db_engine
from strategy_trading.StrategyTrading.msg import Msg, MsgType, Action
from strategy_trading.StrategyTrading.symbolHelper import SymbolHelper
from strategy_trading.STUtils.msgTransmitHelper import MsgClient, MsgServer
from strategy_trading.STUtils.rotateHandler import AsyncTimedRotatingFileHandler
from strategy_trading.StrategyTrading.marketDataConnection import MarketDataConnection
from strategy_trading.StrategyTrading.tradingGatewayConnection import TradingConnection
import sys
import json
import logging
import asyncio
import importlib
from strategy_trading.systemConfig import get_oms_port


class MyFormatter(logging.Formatter):
    converter=datetime.fromtimestamp
    def formatTime(self, record, datefmt=None):
        ct = self.converter(record.created)
        if datefmt:
            s = ct.strftime(datefmt)
        else:
            t = ct.strftime("%Y-%m-%d %H:%M:%S")
            s = "%s,%03d" % (t, record.msecs)
        return s


def update_logfile(config, log_suffix):
    if log_suffix:
        log_file = config['logfile']
        parts = log_file.split('.')
        if len(parts) > 0:
            log_file = parts[0] + log_suffix
        if len(parts) > 1:
            log_file = log_file + "." + parts[1]
        config.update({'logfile': log_file})


class StrategyTradingHelper:
    def __init__(self):
        # self.symbol_helper: SymbolHelper = None
        self.symbol_helper = None
        self.log_format = '%(asctime)s - %(name)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s'
        # self.msg_client: MsgClient = None
        self.msg_client = None
        self.logger = logging.getLogger(self.__class__.__name__)
        self.exch_balance_monitors = {}
        self.heartbeat_servers = []

    def set_parser(self, parser: OptionParser = None):
        if parser is None:
            parser = OptionParser()
        parser.add_option("-x", dest="apollo", default=False, action="store_true", help="Using Apollo to set the configurations")
        parser.add_option("--app_id", type="string", dest="application_id", help="Application ID on Apollo")
        parser.add_option("--ns", type="string", dest="namespace", help="Namespace on Apollo")
        parser.add_option("--token", type='string', dest="token", help="Token for updating/releasing config on Apollo")
        parser.add_option("--logfile", type="string", dest="logfile", help="logfile")
        parser.add_option("-c", type="string", dest="config")
        parser.add_option("-s", dest="stdout", default=False, action="store_true")
        parser.add_option("-a", type="string", dest="suffix", default=None)
        (options, args) = parser.parse_args()
        self.options = options

        if self.options.apollo:
            self.apollo_client = ApolloClient(
                application_id=self.options.application_id, namespace=self.options.namespace,
                environment="production", token=self.options.token
            )
            self.load_apollo_config()
        elif self.options.config:
            config_file = options.config
            self.load_config(config_file)
        else:
            raise Exception("Configurations are missing!")

    def load_config(self, config_file, log_suffix=None):
        assert config_file is not None, "config path should be str, not {}".format(config_file)
        with open(config_file) as json_file:
            config = json.load(json_file)

        assert isinstance(config, dict), "config should be a dict"

        update_logfile(config, log_suffix)

        self.config = config

        if "logfile" in config and not self.options.stdout:
            logformatter = MyFormatter(fmt=self.log_format, datefmt='%Y-%m-%d %H:%M:%S.%f')
            logwhen = 'D' if 'logwhen' not in config else config['logwhen']
            logfile = self.options.logfile if self.options.logfile is not None else config['logfile']
            log = AsyncTimedRotatingFileHandler(logfile, when=logwhen, interval=1)
            log.setLevel(logging.INFO)
            log.setFormatter(logformatter)

            root = logging.getLogger()
            root.setLevel(logging.INFO)
            root.addHandler(log)
        else:
            self.output_log_to_std()

        self.validate_lib_versions(config)

    def validate_lib_versions(self, config: dict):
        if "version" in config:
            version_conf_dict = config['version']
            if not isinstance(version_conf_dict, dict):
                self.logger.warning("version: {} not dict".format(version_conf_dict))
                return
            for lib_str, v in version_conf_dict.items():
                lib = importlib.import_module(lib_str)
                assert hasattr(lib,  "__version__"), "lib {} has no version set".format(lib_str)
                assert lib.__version__ == v, "lib {} version is {} while {} is required".format(lib_str, lib.__version__, v)
                self.logger.info("VERSION: {}: {}".format(lib_str, v))

    def output_log_to_std(self):
        root = logging.getLogger()
        root.setLevel(logging.INFO)

        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(logging.INFO)
        formatter = MyFormatter(fmt=self.log_format,datefmt='%Y-%m-%d %H:%M:%S.%f')
        handler.setFormatter(formatter)
        root.addHandler(handler)

    async def connect_to_trading_gateway(self, loop, gateway_name, strategy, process, receive_error = False, callback_when_exch_state = None) -> TradingConnection:
        assert "tradingGateway" in self.config, "tradingGateway not config-ed"
        assert gateway_name in self.config['tradingGateway'], "trading gateway for {} not config-ed".format(gateway_name)
        config = self.config['tradingGateway'][gateway_name]

        exchange = config['exchange']
        server = config['server']
        port = config['port']

        if self.symbol_helper is None:
            raise ValueError('you must load_symbol_reference first')

        connection = TradingConnection(exchange = exchange,
                                       server = server,
                                       port = port,
                                       strategy = strategy,
                                       process = process,
                                       symbol_helper = self.symbol_helper,
                                       receive_error = receive_error,
                                       callback_when_exch_state = callback_when_exch_state)
        await connection.init(loop)
        return connection

    async def connect_to_local_trading_gateway_by_account(self, loop, exchange, account, strategy, process, receive_error = False, callback_when_exch_state = None) -> TradingConnection:
        port = get_oms_port(exchange = exchange, account=account)

        if self.symbol_helper is None:
            raise ValueError('you must load_symbol_reference first')

        connection = TradingConnection(exchange = exchange,
                                       server = '127.0.0.1',
                                       port = port,
                                       strategy = strategy,
                                       process = process,
                                       symbol_helper = self.symbol_helper,
                                       receive_error = receive_error,
                                       callback_when_exch_state = callback_when_exch_state)
        await connection.init(loop)
        return connection

    async def connect_to_market_data_gateway(self, loop, md_name):
        assert 'marketData' in self.config, "marketData not config-ed"
        assert md_name in self.config['marketData'], 'market data gateway for {} not config-ed'.format(md_name)
        config = self.config['marketData'][md_name]

        exchange = config['exchange']
        server = config['server']
        port = config['port']

        connection = MarketDataConnection(exchange, server, port, self.symbol_helper, dma = False)
        await connection.init(loop)
        return connection

    async def _connect_to_market_data_adapter(self, loop, exchange, enable_kline = True):
        self.logger.warning("connecting to exchange directly is not recommended")
        connection = MarketDataConnection(exchange, None, None, self.symbol_helper, dma = True)
        await connection.init(loop, enable_kline = enable_kline)
        return connection

    async def load_symbol_helper(self):
        if self.symbol_helper is None:
            self.symbol_helper = SymbolHelper()
        await self.symbol_helper.load_symbol_reference()

    async def start_msg_client(self, loop, exchange, strategy, process, version = 'prod', host = "127.0.0.1"):
        channel = "{}.strategy.messages".format(version)
        topic = "{}.{}.{}".format(exchange, strategy, process)
        self.msg_client = MsgClient(loop, channel, [topic], ip = host)
        await self.msg_client.connect()

    async def get_msgs(self) -> List[Msg]:
        if self.msg_client:
            messages = await self.msg_client.get_messages()
            self.logger.info("receive messages {}".format(messages))
            formatted_messages = []
            for message in messages:
                data = message['data']
                if isinstance(data, dict):
                    try:
                        _type = data.get('type')
                        key = data.get('key')
                        value = data.get('value')
                        action = data.get('action')
                        action = Action(action) if action else None
                        _type = MsgType(_type)
                        msg = Msg(_type, key, value, action)
                        formatted_messages.append(msg)
                    except Exception as e:
                        self.logger.warning("fail to parse message {}. ignore. {}".format(data, e))
                else:
                    self.logger.warning("message type is {}. ignore".format(type(data)))
            return formatted_messages
        raise Exception("msg client is not started yet")

    async def start_exch_balance_monitors(self, loop, configs: list, host = "127.0.0.1"):
        if host in self.exch_balance_monitors:
            self.logger.warning("{} balance monitors have been set-up".format(host))
            return

        topics_config = {}
        for config in configs:
            assert isinstance(config, dict)
            assert "exchange" in config
            assert "account" in config
            assert "account_type" in config
            assert "ccies" in config
            assert "on_ccy_balance_change" in config
            exchange = config['exchange']
            account = config['account']
            account_type = config['account_type']
            ccies = config['ccies']
            on_ccy_balance_change = config['on_ccy_balance_change']

            assert account_type in ['spot', 'cross-margin', 'main', "margin", "contract", "swap"]
            assert callable(on_ccy_balance_change)

            if account_type == 'cross-margin' and exchange == 'HUOBI_SPOT':
                account_type = 'super-margin'
                config['account_type'] = account_type

            balance_type = 'trade' if exchange in ['HUOBI_SPOT', 'BINANCE_SPOT', 'OKEX_SPOT'] else 'marginAvailable'
            config['balance_type'] = balance_type

            topic = "{}.{}".format(exchange, account)
            if topic not in topics_config:
                topics_config[topic] = [config]
            else:
                topics_config[topic].append(config)

        channel = "prod.account"
        msg_client = MsgClient(loop, channel, list(topics_config.keys()), ip=host)
        self.exch_balance_monitors[host] = msg_client
        await msg_client.connect()
        asyncio.ensure_future(self._monitor_exch_balances(msg_client, topics_config), loop = loop)

    async def _monitor_exch_balances(self, msg_client, topics_config: dict):
        while True:
            messages = await msg_client.get_messages()
            for message in messages:
                try:
                    topic = message['topic']
                    data = message['data']
                    self.logger.info("receive exch {} balance messages {}".format(message['topic'], data))

                    configs = topics_config[topic]
                    for config in configs:
                        account_type = config['account_type']
                        ccies = config['ccies']
                        balance_type = config['balance_type']
                        on_ccy_balance_change = config['on_ccy_balance_change']

                        if isinstance(data, dict) and 'balance' in data:
                            if isinstance(data['balance'], dict) and account_type in data['balance']:
                                update_type = data['type']
                                ccies_balances = data['balance'][account_type]
                                if isinstance(ccies_balances, dict):
                                    updated_ccies = []
                                    for k, v in ccies_balances.items():
                                        if k in ccies and isinstance(v, dict) and balance_type in v:
                                            on_ccy_balance_change({
                                                "currency": k,
                                                "size": Decimal("{:.8f}".format(float(v[balance_type])))
                                            })
                                            updated_ccies.append(k)
                                    if update_type == 'full':
                                        for k in ccies:
                                            if k not in updated_ccies:
                                                on_ccy_balance_change({
                                                    "currency": k,
                                                    "size": Decimal("0")
                                                })
                except Exception as e:
                    self.logger.exception("fail to parse exch balance msg {}".format(e))

    async def start_exch_balance_monitor(self, loop, exchange, account, account_type, ccies: list, on_ccy_balance_change, host = "127.0.0.1"):
        assert account_type in ['spot', 'cross-margin', 'main']
        assert callable(on_ccy_balance_change)

        if account_type == 'cross-margin' and exchange == 'HUOBI_SPOT':
            account_type = 'super-margin'

        if exchange not in self.exch_balance_monitors:
            self.exch_balance_monitors[exchange] = {}
        if account not in self.exch_balance_monitors[exchange]:
            self.exch_balance_monitors[exchange][account] = {}
        if account_type in self.exch_balance_monitors[exchange][account]:
            self.logger.warning("{}-{}-{} balance monitor has been set-up".format(exchange, account, account_type))
            return

        channel = "prod.account"
        topic = "{}.{}".format(exchange, account)
        msg_client = MsgClient(loop, channel, [topic], ip = host)
        self.exch_balance_monitors[exchange][account][account_type] = msg_client
        await msg_client.connect()
        balance_type = 'trade' if exchange in ['HUOBI_SPOT', 'BINANCE_SPOT', 'OKEX_SPOT'] else 'marginAvailable'
        asyncio.ensure_future(self._monitor_exch_balance(msg_client, account_type, ccies, on_ccy_balance_change, balance_type), loop = loop)

    async def _monitor_exch_balance(self, msg_client, account_type, ccies: list, on_ccy_balance_change, balance_type = 'trade'):
        while True:
            messages = await msg_client.get_messages()
            for message in messages:
                data = message['data']
                self.logger.info("receive exch {} balance messages {}".format(message['topic'], data))
                try:
                    if isinstance(data, dict) and 'balance' in data:
                        if isinstance(data['balance'], dict) and account_type in data['balance']:
                            update_type = data['type']
                            ccies_balances = data['balance'][account_type]
                            if isinstance(ccies_balances, dict):
                                updated_ccies = []
                                for k, v in ccies_balances.items():
                                    if k in ccies and isinstance(v, dict) and balance_type in v:
                                        on_ccy_balance_change({
                                            "currency": k,
                                            "size": Decimal("{:.8f}".format(float(v[balance_type])))
                                        })
                                        updated_ccies.append(k)

                                if update_type == 'full':
                                    for k in ccies:
                                        if k not in updated_ccies:
                                            on_ccy_balance_change({
                                                "currency": k,
                                                "size": Decimal("0")
                                            })
                except Exception as e:
                    self.logger.exception("fail to parse exch balance msg {}".format(e))

    async def send_heartbeat(self, loop, strategy, process, freq_seconds = 30, work_as_main_corou = False):
        self.logger.info('Start Heartbeat Server for {}/{}'.format(strategy, process))
        heartbeat_server = MsgServer(loop, 'prod.event', 'heartbeat')
        try:
            await heartbeat_server.connect()
            asyncio.ensure_future(heartbeat_server.start(), loop=loop)

            async def _send(heartbeat_server, strategy, process, freq_seconds):
                while True:
                    await heartbeat_server.send(
                        {
                            'strategy': strategy,
                            'process': process,
                            'event': 'heartbeat',
                            'time': int(datetime.now().timestamp() * 1000000)
                        }
                    )
                    self.logger.info("send heartbeat")
                    await asyncio.sleep(freq_seconds)

            self.heartbeat_servers.append(heartbeat_server)
            if not work_as_main_corou:
                asyncio.ensure_future(_send(heartbeat_server, strategy, process, freq_seconds), loop=loop)
            else:
                await _send(heartbeat_server, strategy, process, freq_seconds)
        except Exception as e:
            self.logger.warning("Fail to start heartbeat service")

    async def store_params(self, strategy, process, params_dict: dict):
        try:
            curr_time = int(datetime.now().timestamp() * 1000000)
            engine = get_db_engine('data', 'trading')
            async with engine.connect() as con:
                for name, value in params_dict.items():
                    sql = "insert into parameters(strategy, process, name, value, time) values('{}', '{}', '{}', '{}', {})".format(
                        strategy, process, str(name)[:255], str(value)[:255], curr_time
                    )
                    await con.execute(sql)
        except Exception as e:
            self.logger.exception("Fail to store parameters into db")

    def load_apollo_config(self):
        assert self.options.application_id and self.options.namespace, "Apollo configurations are missing!"
        # Loading string content into dict
        config = self.apollo_client.load_config_synchronous(config_only=True)
        config = config['content']
        config = json.loads(config)

        assert isinstance(config, dict), "Config should be dictionary."
        self.config = config

        if "logfile" in config and not self.options.stdout:
            logformatter = MyFormatter(fmt=self.log_format, datefmt='%Y-%m-%d %H:%M:%S.%f')
            logwhen = 'D' if 'logwhen' not in config else config['logwhen']
            logfile = self.options.logfile if self.options.logfile is not None else config['logfile']
            log = AsyncTimedRotatingFileHandler(logfile, when=logwhen, interval=1)
            log.setLevel(logging.INFO)
            log.setFormatter(logformatter)

            root = logging.getLogger()
            root.setLevel(logging.INFO)
            root.addHandler(log)
        else:
            self.output_log_to_std()

        self.validate_lib_versions(config)

    async def update_apollo_config(self, update_info: dict, release_info: dict = None, auto_release: bool = False):
        """
        :param update_info: Dictionary for updating the configuration.
        :param release_info: Dictionary for releasing the configuration.
        :param auto_release: Automatically release the configuration or not. Default False.
        """
        assert self.options.application_id and self.options.namespace and self.options.token, "Apollo configurations are missing!"

        await self.apollo_client.update_config(data=update_info)

        if auto_release:
            assert release_info, "Release information is a must if you would like to auto_release."
            await self.apollo_client.release_config(data=release_info)


if __name__ == '__main__':
    helper = StrategyTradingHelper()
    helper.output_log_to_std()
    i = 0
    while i < 100:
        logging.info("hello world {}".format(datetime.now()))
        i += 1
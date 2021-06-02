import socket
import time

from strategy_trading.STUtils.msgTransmitHelper import MsgClient
import logging
from strategy_trading.STUtils.dbengine import get_db_engine, read_sql
from datetime import datetime, timedelta
import asyncio
from strategy_trading.STUtils.dingTalkHelper import DingTalk_Disaster
from sortedcontainers import SortedDict
from optparse import OptionParser
from strategy_trading.StrategyTrading.tradingHelper import StrategyTradingHelper


ZHOUSHENGKUAN_PHONE = '+1-2893807666'


def is_night_time():
    ts = int(time.time())
    time_local = time.localtime(ts)
    dt = time.strftime("%H:%M:%S", time_local)
    hr = dt.split(":")[0]
    if 9 < int(hr) <= 21:
        return False
    else:
        return True


def check_port_status(ip, port):
    a_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    location = (ip, port)
    result_of_check = a_socket.connect_ex(location)
    a_socket.close()
    # 0 open else fail
    return result_of_check == 0


class HeartbeatMonitor():
    def __init__(self, server, error_ding_token, info_ding_token, dbname = 'hbdata', channel = 'prod.event', topic = 'heartbeat', max_delayed_seconds = 300):
        self.server = server
        self.channel = channel
        self.topic = topic
        self.logger = logging.getLogger("HBM")
        self.engine = get_db_engine('data', dbname)
        self.info = SortedDict()
        self.ding_error_client = DingTalk_Disaster(error_ding_token)
        self.ding_info_client = DingTalk_Disaster(info_ding_token)
        self.max_delayed_seconds = max_delayed_seconds
        self.all_contacts = []
        self.rabbit_mq_status = False
        self.rabbit_mq_last_check = None


    def get_contacts(self, strategy, process):
        contacts = []
        for contact in self.all_contacts:
            if contact['strategy'] == strategy or contact['process'] == process:
                contacts.extend(contact['mobile'].split(','))
        return contacts

    async def auto_refresh(self):
        while True:
            await asyncio.sleep(120)
            try:
                # find the ones who have been killed by traders
                all_instances = await read_sql("select * from system_states where server = '{}'".format(self.server), engine = self.engine)
                self.all_contacts = await read_sql(
                    "select * from system_states_contact where server = '{}'".format(self.server), engine=self.engine)
                if all_instances:
                    for instance in all_instances:
                        if instance['state'] == 'stopped' and instance['strategy'] in self.info and instance['process'] in self.info[instance['strategy']]:
                            try:
                                update_time = datetime.fromtimestamp(instance['update_time'] / 1000000)
                                if update_time > self.info[instance['strategy']][instance['process']]:
                                    self.logger.info("IGNORE {}".format(instance))
                                    self.info[instance['strategy']].pop(instance['process'])
                            except Exception as e:
                                self.logger.exception(e)

                # find those instances which are not stored in db, store it
                for strategy, process_info in self.info.items():
                    for process, timestamp in process_info.items():
                        b_find_active = False
                        b_find_inactive = False
                        for instance in all_instances:
                            if instance['strategy'] == strategy and instance['process'] == process:
                                if instance['state'] == 'active':
                                    b_find_active = True
                                elif instance['state'] == 'stopped':
                                    b_find_inactive = True

                        if b_find_active:
                            continue

                        if b_find_inactive:
                            await self.update_to_db(strategy, process, append=False)
                            continue

                        await self.update_to_db(strategy, process, append=True)

            except Exception as e:
                self.logger.exception(e)

    async def update_to_db(self, strategy, process, append = True):
        if append:
            sql = "insert into system_states (server, strategy, process, state, update_time) values('{}', '{}', '{}', 'active', {})".format(
                self.server, strategy, process, int(datetime.now().timestamp() * 1000000)
            )
        else:
            sql = "update system_states set state = 'active' where server = '{}' and strategy = '{}' and process = '{}'".format(
                self.server, strategy, process
            )

        try:
            async with self.engine.connect() as con:
                await con.execute(sql)
        except Exception as e:
            self.logger.exception(e)

    async def start(self, loop):
        self.client = MsgClient(loop, self.channel, [self.topic])

        await self.client.connect()
        asyncio.ensure_future(self.alert_error(), loop = loop)
        asyncio.ensure_future(self.auto_refresh(), loop=loop)
        asyncio.ensure_future(self.send_snapshot(), loop=loop)
        while True:
            msgs = await self.client.get_messages()
            try:
                for msg in msgs:
                    data = msg['data']
                    if data['event'] == 'heartbeat':
                        if data['strategy'] not in self.info:
                            self.info[data['strategy']] = SortedDict()
                        self.info[data['strategy']][data['process']] = datetime.fromtimestamp(data['time'] / 1000000)
            except Exception as e:
                self.logger.exception(e)

    async def alert_error(self):
        while True:
            await asyncio.sleep(300)
            # check if any strategy and process have been not working for a while
            curr_time = datetime.now()
            errors = []
            phone_nums = []
            for strategy, process_info in self.info.items():
                if strategy == 'TEST':
                    continue
                for process, last_reveive in process_info.items():
                    if curr_time - last_reveive > timedelta(seconds=self.max_delayed_seconds):
                        errors.append("%-20s  %-20s  %-20s" % (strategy, process, last_reveive.strftime("%Y-%m-%d %H:%M:%S")))
                        phone_nums.extend(self.get_contacts(strategy, process))
            self.check_rabbit_mq(errors, phone_nums)
            if errors:
                errors.insert(0, "%-20s  %-20s  %-20s" % ('strategy', 'process', 'last update'))
                errors.insert(0, "============================================")
                errors.insert(0, "STOPPED instances at server {}. current time: {}".format(self.server, curr_time.strftime("%Y-%m-%d %H:%M:%S")))

                try:
                    if is_night_time():
                        phone_nums.append(ZHOUSHENGKUAN_PHONE)
                    else:
                        is_at_all = (len(phone_nums) == 0)
                    await self.ding_error_client.async_send_more("\n".join(errors) + "\n", phone_nums, is_at_all)
                    # notify_to_dingding(self.error_ding_token, phone_nums, "\n".join(errors), True, len(phone_nums)==0)
                except Exception as e:
                    self.logger.exception("fail to send to dd {}".format(e))

    async def send_snapshot(self):
        while True:
            await asyncio.sleep(1800)
            curr_time = datetime.now()

            infos = []
            invalid_num = 0
            for strategy, process_info in self.info.items():
                for process, last_reveive in process_info.items():
                    infos.append("%-20s  %-20s  %-20s" % (strategy, process, last_reveive.strftime("%Y-%m-%d %H:%M:%S")))
                    if curr_time - last_reveive > timedelta(seconds=self.max_delayed_seconds):
                        invalid_num += 1
            infos.append(self.rabbit_mq_check_str())
            if not self.rabbit_mq_status:
                invalid_num += 1
            if infos:
                all_num = len(infos)
                infos.insert(0, "%-20s  %-20s  %-20s" % ('strategy', 'process', 'last update'))
                infos.insert(0, "============================================")
                infos.insert(0, "server {}: running: {}, error: {}. current time: {}".format(self.server, all_num, invalid_num, curr_time.strftime("%Y-%m-%d %H:%M:%S")))

                try:
                    await self.ding_info_client.async_send("\n".join(infos))
                except Exception as e:
                    self.logger.exception("fail to send to dd {}".format(e))

    def check_rabbit_mq(self, errors, phone_nums):
        if not check_port_status('127.0.0.1', 5672):
            self.rabbit_mq_status = False
            errors.append(self.rabbit_mq_check_str())
            phone_nums.extend(self.get_contacts('rabbitmq', '5672'))
        else:
            self.rabbit_mq_last_check = datetime.now()
            self.rabbit_mq_status = True

    def rabbit_mq_check_str(self):
        return "%-20s  %-20s  %-20s" % ('rabbitmq', '5672', self.rabbit_mq_last_check.strftime(
            "%Y-%m-%d %H:%M:%S") if self.rabbit_mq_last_check else 'None')


if __name__ == '__main__':
    parser = OptionParser()
    parser.add_option("-s", type="string", dest="server")
    parser.add_option("-d", type="string", dest="database", default='hbdata')
    (options, args) = parser.parse_args()
    server = options.server
    db = options.database

    # 买方Critical报警群
    error_ding_token = '8ea47eb7e145056ef3dae2dcd8370c44a13dd1a98ff935375f55eba705aafd1e'
    if server in ['T3', 'T5', 'T6', 'T10', 'T11']:
        # 流动运营监控群
        error_ding_token = '2ac0c1405d8a0a82de2f233bf8e1a56c5be28982f2ceb8c39a97dbc98a9f561d'

    monitor = HeartbeatMonitor(
        server=server,
        error_ding_token=error_ding_token,
        info_ding_token='480a5ab1d4650da565b78da6eed2c9201e84d4c1e829ad38e2d8f70fb5e4f7b1',
        dbname=db,
        max_delayed_seconds=300
    )

    helper = StrategyTradingHelper()
    helper.output_log_to_std()

    loop = asyncio.get_event_loop()
    loop.run_until_complete(monitor.start(loop))

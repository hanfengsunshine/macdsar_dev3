from aio_pika import ExchangeType, IncomingMessage, Message, DeliveryMode, connect_robust
import ujson
import asyncio
import logging


class MsgClient():
    def __init__(self, loop, channel, topics: list, ip = '127.0.0.1', user = 'guest', password = 'guest'):
        self.loop = loop
        self.ip = ip
        self.topics = topics
        self.channel = channel
        self.user = user
        self.password = password
        self.data_q = asyncio.Queue(maxsize=1000000)
        self.logger = logging.getLogger("{}-{}".format(self.__class__.__name__, channel))

    async def connect(self):
        connection = await connect_robust("amqp://{}:{}@{}/".format(self.user, self.password, self.ip), loop = self.loop)

        channel = await connection.channel()
        await channel.set_qos(prefetch_count=1)

        exchange = await channel.declare_exchange(self.channel, ExchangeType.TOPIC)
        queue = await channel.declare_queue(durable=True)
        for topic in self.topics:
            await queue.bind(exchange, routing_key =topic)

        await queue.consume(self._receive_data)

    async def _receive_data(self, message: IncomingMessage):
        try:
            with message.process():
                data = message.body
                self.logger.info(data)
                data = ujson.loads(data.decode())
                self.data_q.put_nowait({
                    'topic': message.routing_key,
                    'data': data
                })
        except Exception as e:
            self.logger.error("{}-{}".format(e, message))

    async def get_messages(self):
        messages = []
        msg = await self.data_q.get()
        messages.append(msg)

        while not self.data_q.empty():
            msg = await self.data_q.get()
            messages.append(msg)
        return messages


class MsgServer():
    def __init__(self, loop, channel, topic, ip = '127.0.0.1', user = 'guest', password = 'guest'):
        self.logger = logging.getLogger("{}-{}".format(self.__class__.__name__, channel))
        self.loop = loop
        self.channel = channel
        self.topic = topic
        self.ip = ip
        self.user = user
        self.password = password
        self.data_q = asyncio.Queue(maxsize=1000000)

    async def connect(self):
        connection = await connect_robust("amqp://{}:{}@{}/".format(self.user, self.password, self.ip), loop = self.loop)

        channel = await connection.channel()
        self.exchange = await channel.declare_exchange(self.channel, ExchangeType.TOPIC)

    async def send(self, data):
        await self.data_q.put(data)

    async def start(self):
        while True:
            try:
                message = await self.data_q.get()
                message = ujson.dumps(message).encode()
                formatted_message = Message(message, delivery_mode=DeliveryMode.PERSISTENT)
                pub_success = await self.exchange.publish(formatted_message, routing_key = self.topic)
                if pub_success:
                    self.logger.info('pub {} successfully'.format(message))
                else:
                    self.logger.error("fail tp pub {}".format(message))
            except Exception as e:
                self.logger.error("when pub msg, {}-{}".format(e, message))


class MsgServerCusTopic():
    def __init__(self, loop, channel, ip = '127.0.0.1', user = 'guest', password = 'guest'):
        self.logger = logging.getLogger("{}-{}".format(self.__class__.__name__, channel))
        self.loop = loop
        self.channel = channel
        self.ip = ip
        self.user = user
        self.password = password
        self.data_q = asyncio.Queue(maxsize=1000000)

    async def connect(self):
        connection = await connect_robust("amqp://{}:{}@{}/".format(self.user, self.password, self.ip), loop = self.loop)

        channel = await connection.channel()
        self.exchange = await channel.declare_exchange(self.channel, ExchangeType.TOPIC)

    async def send(self, data, topic):
        await self.data_q.put([data, topic])

    async def start(self):
        while True:
            try:
                message, topic = await self.data_q.get()
                message = ujson.dumps(message).encode()
                formatted_message = Message(message, delivery_mode=DeliveryMode.PERSISTENT)
                pub_success = await self.exchange.publish(formatted_message, routing_key = topic)
                if pub_success:
                    self.logger.info('pub {} to {} successfully'.format(message, topic))
                else:
                    self.logger.error("fail tp pub {} to {}".format(message, topic))
            except Exception as e:
                self.logger.error("when pub msg, {}-{}-{}".format(e, message, topic))


if __name__ == '__main__':
    async def client_t():
        await client.connect()
        while True:
            print(await client.get_messages())

    async def server_t():
        await server.connect()
        asyncio.ensure_future(server.start(), loop = loop)
        i = 0
        while True:
            await server.send(i)
            await asyncio.sleep(1)
            i += 1

    loop = asyncio.get_event_loop()
    server = MsgServer(loop, 'test', 'hello.world')
    client = MsgClient(loop, 'test', ['hello.world'])
    asyncio.ensure_future(server_t(), loop = loop)
    asyncio.ensure_future(client_t(), loop = loop)
    loop.run_forever()

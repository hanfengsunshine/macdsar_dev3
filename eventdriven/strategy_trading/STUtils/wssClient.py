import aiohttp
import logging
import asyncio
import json
import inspect


class BaseWebsocketClient():
    def __init__(self, url: str, allow_reconnect: bool, callback_when_data, callback_after_connection = None, heartbeat = 5, ssl = False, retry_seconds: int = None):
        self.url = url
        self.allow_reconnect = allow_reconnect
        if callback_after_connection is not None:
            assert callable(callback_after_connection) or inspect.iscoroutinefunction(callback_after_connection)
        self.callback_after_connection = callback_after_connection
        assert callable(callback_when_data)
        self.callback_when_data = callback_when_data
        self.ws = None
        self.logger = logging.getLogger(self.__class__.__name__)
        self.heartbeat = heartbeat
        self.ssl = ssl
        if isinstance(retry_seconds, int) and retry_seconds:
            self.retry_seconds = max(retry_seconds, 1)
        else:
            self.retry_seconds = None

    async def connect_to_server(self, retry = None):
        if retry is None:
            retry = self.retry_seconds

        if not retry:
            # try to connect/reconnect. Not handle error when connecting yet. Depends.
            session = aiohttp.ClientSession()
            self.logger.info('connecting to {}'.format(self.url))
            self.ws = await session.ws_connect(self.url, ssl=self.ssl, heartbeat=self.heartbeat)
            self.logger.info('connected to {}'.format(self.url))
            if self.callback_after_connection is not None:
                if inspect.iscoroutinefunction(self.callback_after_connection):
                    await self.callback_after_connection()
                else:
                    self.callback_after_connection()
        else:
            while True:
                try:
                    await self.connect_to_server(False)
                    return
                except Exception as e:
                    self.logger.exception("{} when connecting to {}".format(e, self.url))

                    if self.ws is not None:
                        try:
                            self.logger.info("closing connection to {}".format(self.url))
                            await self.ws.close()
                        except Exception as e:
                            self.logger.warning("fail to close connection to {}. hard reset it now".format(self.url))
                        self.ws = None

                    self.logger.warning("RECONNECT in {}s".format(self.retry_seconds))
                    await asyncio.sleep(self.retry_seconds)

    async def receive_data(self):
        if self.ws is not None:
            while True:
                try:
                    msg = await self.ws.receive()
                    if msg.type == aiohttp.WSMsgType.CLOSED or msg.type == aiohttp.WSMsgType.ERROR:
                        raise Exception('receiving {} from server'.format(msg))
                except Exception as e:
                    self.logger.warning("Error when receiving msg from {}: {}".format(self.url, e))
                    if self.ws is not None:
                        await self.ws.close()
                        self.ws = None

                    if self.allow_reconnect:
                        await self.connect_to_server()
                        continue
                    break

                if msg.type == aiohttp.WSMsgType.TEXT:
                    try:
                        self.callback_when_data(json.loads(msg.data))
                    except Exception as e:
                        self.logger.exception("{} when client processing {}".format(e, msg.data))
                elif msg.type == aiohttp.WSMsgType.BINARY:
                    data = msg.data
                    data = data.decode('utf-8')
                    data = json.loads(data)
                    try:
                        self.callback_when_data(data)
                    except Exception as e:
                        self.logger.exception("{} when client processing {}".format(e, data))
                else:
                    self.logger.warning("receive msg {} with type {}. Not recognized. Ignore"
                                        .format(msg.data, msg.type))

    async def send_message(self, messages: list):
        if self.ws is not None:
            for msg in messages:
                await self.ws.send_json(msg)


if __name__ == '__main__':
    import sys
    root = logging.getLogger()
    root.setLevel(logging.INFO)

    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    root.addHandler(handler)

    async def send():
        await client.connect_to_server()
        asyncio.ensure_future(client.receive_data(), loop=loop)
        from datetime import datetime
        while True:
            await asyncio.sleep(1)
            try:
                logging.info("sending message")
                await client.send_message([str(datetime.now())])
            except Exception as e:
                logging.warning(e)
    client = BaseWebsocketClient("http://127.0.0.1:5000", True, print)
    loop = asyncio.get_event_loop()
    loop.run_until_complete(send())

import logging
import asyncio

MAX_RETRY = 10

def retry(func):
    async def decorator(*args, **kwargs):
        try_i = 0
        while try_i < MAX_RETRY:
            try:
                res = await func(*args, **kwargs)
                return res
            except Exception as e:
                logging.exception(e)
                try_i += 1
                logging.info("retry in {} seconds".format(try_i))
                await asyncio.sleep(try_i)

        if try_i >= MAX_RETRY:
            raise Exception('fail to call {} with args {}, {}'.format(func.__name__, args, kwargs))

    return decorator

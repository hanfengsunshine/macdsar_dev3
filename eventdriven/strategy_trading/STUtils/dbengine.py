import sqlalchemy
import json
import os
from sqlalchemy_aio import ASYNCIO_STRATEGY
import asyncio


def get_db_config(server, database, setting_file=None):
    if setting_file is None:
        setting_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'db.json')
    with open(setting_file) as json_file:
        db_config = json.load(json_file)
    assert isinstance(db_config, dict), "DB config should be a dict"
    assert server in db_config, "server {} is not config-ed".format(server)
    assert database in db_config[server], "db {} is not config-ed in server {}".format(database, server)
    return db_config[server][database]


def get_db_engine(server, database, setting_file=None, is_async=True):
    if setting_file is None:
        setting_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'db.json')
    with open(setting_file) as json_file:
        db_config = json.load(json_file)
    assert isinstance(db_config, dict), "DB config should be a dict"
    assert server in db_config, "server {} is not config-ed".format(server)
    assert database in db_config[server], "db {} is not config-ed in server {}".format(database, server)
    username = db_config[server][database]['user']
    password = db_config[server][database]['password']
    host = db_config[server][database]['host']
    port = db_config[server][database]['port']

    if is_async:
        engine = sqlalchemy.create_engine(
            "postgresql+psycopg2://{}:{}@{}:{}/{}".format(username, password, host, port, database),
            strategy=ASYNCIO_STRATEGY)
    else:
        engine = sqlalchemy.create_engine(
            "postgresql+psycopg2://{}:{}@{}:{}/{}".format(username, password, host, port, database))
    return engine


async def read_sql(sql, engine):
    async with engine.connect() as con:
        result = await con.execute(sql)
        data = await result.fetchall()
        # convert data into list
        dlist = []
        for one_row in data:
            dlist.append(
                {one_row._parent.keys[ki]: one_row._row[ki] for ki in range(len(one_row._parent.keys))}
            )
        return dlist


if __name__ == '__main__':
    engine = get_db_engine('data', 'trading')
    loop = asyncio.get_event_loop()
    loop.run_until_complete(read_sql("select * from symbol_reference", engine))

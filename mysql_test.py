import time

import tornado.gen
import tornado.ioloop

from asyncdb.mysql import AmysqlConnection


def my_function(callback):
    print('do some work')
    time.sleep(2)
    callback(123)


@tornado.gen.engine
def f():
    print('sta rt')
    sql = "update site_info set status=status+1 where id=1"

    connection = AmysqlConnection()
    yield tornado.gen.Task(connection.connect)
    yield tornado.gen.Task(connection.query, sql)
    tornado.ioloop.IOLoop.instance().stop()


if __name__ == "__main__":
    f()
    tornado.ioloop.IOLoop.instance().start()

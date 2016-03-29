import tornado.gen
import tornado.ioloop

from asyncdb.mysql import AsyncClient


@tornado.gen.engine
def f():
    sql = "select * from site_info where id=1"
    connection = AsyncClient()
    yield tornado.gen.Task(connection.connect)
    cursor = connection.cursor()
    yield tornado.gen.Task(cursor.execute, sql)
    result = cursor.fetchone()
    print(result)
    tornado.ioloop.IOLoop.instance().stop()


if __name__ == "__main__":
    f()
    tornado.ioloop.IOLoop.instance().start()

import tornado.gen
import tornado.ioloop

from asyncdb.mysql import MysqlClient


@tornado.gen.engine
def foo():
    connection = MysqlClient()
    yield tornado.gen.Task(connection.connect)
    cursor = connection.cursor()
    yield tornado.gen.Task(cursor.execute, "select * from site_info where id=1")
    result = cursor.fetchone()
    print(result)
    tornado.ioloop.IOLoop.instance().stop()


if __name__ == "__main__":
    foo()
    tornado.ioloop.IOLoop.instance().start()

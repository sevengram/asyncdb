import tornado.gen
import tornado.ioloop

from asyncdb.mysql import TorMysqlPool

pool = TorMysqlPool(host='127.0.0.1', port=3306, user='root', password='root',
                    database='wechat_platform')


@tornado.gen.engine
def foo():
    connection = pool.get_connection()
    yield tornado.gen.Task(connection.connect)
    cursor = connection.cursor()
    yield tornado.gen.Task(cursor.execute, "select * from site_info where id=1")
    result = cursor.fetchone()
    print(result)
    connection.close()
    tornado.ioloop.IOLoop.instance().stop()


if __name__ == "__main__":
    foo()
    tornado.ioloop.IOLoop.instance().start()

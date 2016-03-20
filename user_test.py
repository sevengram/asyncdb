import time
import tornado.gen
import tornado.ioloop
from asyncdb import MotorClient


def my_function(callback):
    print('do some work')
    time.sleep(2)
    callback(123)


@tornado.gen.engine
def f():
    print('start')
    db = MotorClient().astro_data
    result = yield tornado.gen.Task(db.deepsky.find_one, {'alias': 'M31'})
    # result = yield tornado.gen.Task(my_function)
    print('result is', result)
    tornado.ioloop.IOLoop.instance().stop()


if __name__ == "__main__":
    f()
    tornado.ioloop.IOLoop.instance().start()

import tornado.gen
import tornado.ioloop

from asyncdb import MotorClient


@tornado.gen.engine
def foo():
    db = MotorClient().astro_data
    result = yield tornado.gen.Task(db.deepsky.find_one, {'alias': 'M31'})
    print(result)
    tornado.ioloop.IOLoop.instance().stop()


if __name__ == "__main__":
    foo()
    tornado.ioloop.IOLoop.instance().start()

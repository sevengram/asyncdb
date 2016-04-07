#! /usr/bin/env python3
# -*- coding:utf8 -*-

import tornado.gen
import tornado.httpserver
import tornado.ioloop
import tornado.options
import tornado.web
from tornado.options import define, options

from asyncdb import MotorClient
from asyncdb import MysqlClient

define('port', default=33600, help="run on the given port", type=int)
define('env', default='dev', help="run on the given environment", type=str)
define('conf', default='config', help="config file dir", type=str)

tornado.options.parse_command_line()

motor_db = MotorClient().astro_data

mysql_client = MysqlClient()
mysql_client.connect()


class MotorHandler(tornado.web.RequestHandler):
    @tornado.gen.coroutine
    def get(self):
        result = yield motor_db.deepsky.find_one({'alias': 'M31'})
        del result['_id']
        self.write(result)
        self.finish()


class MysqlHandler(tornado.web.RequestHandler):
    @tornado.gen.coroutine
    def get(self):
        cursor = mysql_client.cursor()
        yield cursor.execute("select * from site_info where id=1")
        result = cursor.fetchone()
        del result['ctime']
        self.write(result)
        self.finish()


if __name__ == '__main__':
    application = tornado.web.Application(
        handlers=[
            (r'/mongo', MotorHandler),
            (r'/mysql', MysqlHandler)
        ]
    )
    http_server = tornado.httpserver.HTTPServer(application, xheaders=True)
    http_server.listen(options.port)
    tornado.ioloop.IOLoop.instance().start()
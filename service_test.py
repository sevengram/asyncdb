#! /usr/bin/env python3
# -*- coding:utf8 -*-
import logging
import time

import pymysql
import pymysql.cursors
import tornado.gen
import tornado.httpserver
import tornado.ioloop
import tornado.options
import tornado.web
from pymongo import MongoClient
from tornado.options import define, options

from asyncdb import MotorClient
from asyncdb.mysql import TorMysqlPool

# logging.getLogger('tornado.access').disabled = True

define('port', default=33600, help="run on the given port", type=int)
define('env', default='dev', help="run on the given environment", type=str)
define('conf', default='config', help="config file dir", type=str)

tornado.options.parse_command_line()

mongo_db = MongoClient().test
mongo_db2 = MongoClient().astro_data

motor_db = MotorClient().test
motor_db2 = MotorClient().astro_data

mysql_pool = TorMysqlPool(host='127.0.0.1', port=3306, user='root', password='root',
                          database='wechat_platform', max_size=100)

mysql_conn = pymysql.connect(host='localhost',
                             user='root',
                             password='root',
                             db='wechat_platform',
                             cursorclass=pymysql.cursors.DictCursor,
                             autocommit=True)


class MotorHandler(tornado.web.RequestHandler):
    @tornado.gen.coroutine
    def get(self):
        t = int(self.get_argument('type'))
        if t == 0:
            yield motor_db.driver_test.remove()
        elif t == 1:
            yield motor_db2.deepsky.find_one({'alias': 'M31'})
        elif t == 2:
            yield motor_db.driver_test.insert({'time': "%.9f" % time.time()})
        elif t == 3:
            yield motor_db.driver_test.find({'time': {"$regex": "116"}}).to_list(None)
        self.finish()


class MongoHandler(tornado.web.RequestHandler):
    @tornado.gen.coroutine
    def get(self):
        t = int(self.get_argument('type'))
        if t == 0:
            mongo_db.driver_test.remove()
        elif t == 1:
            mongo_db2.deepsky.find_one({'alias': 'M31'})
        elif t == 2:
            mongo_db.driver_test.insert({'time': "%.9f" % time.time()})
        elif t == 3:
            list(mongo_db.driver_test.find({'time': {"$regex": "116"}}))
        self.finish()


class AsyncMysqlHandler(tornado.web.RequestHandler):
    @tornado.gen.coroutine
    def get(self):
        t = int(self.get_argument('type'))
        mysql_client = mysql_pool.get_connection()
        yield mysql_client.connect()
        cursor = mysql_client.cursor()
        if t == 0:
            yield cursor.execute("delete from test where `id`>1")
        elif t == 1:
            yield cursor.execute("select `name` from test where `id`=1")
        elif t == 2:
            yield cursor.execute("insert into `test` (`name`) values (\"%.9f\")" % time.time())
        elif t == 3:
            yield cursor.execute("select * from `test` where name like \"%116%\"")
        mysql_client.close()
        self.finish()


class MysqlHandler(tornado.web.RequestHandler):
    @tornado.gen.coroutine
    def get(self):
        t = int(self.get_argument('type'))
        cursor = mysql_conn.cursor()
        if t == 0:
            cursor.execute("delete from test where `id`>1")
        elif t == 1:
            cursor.execute("select `name` from test where `id`=1")
        elif t == 2:
            cursor.execute("insert into `test` (`name`) values (\"%.9f\")" % time.time())
        elif t == 3:
            cursor.execute("select * from `test2` where name like \"%116%\"")
        self.finish()


if __name__ == '__main__':
    application = tornado.web.Application(
        handlers=[
            (r'/motor', MotorHandler),
            (r'/mongo', MongoHandler),
            (r'/amysql', AsyncMysqlHandler),
            (r'/mysql', MysqlHandler)
        ]
    )
    http_server = tornado.httpserver.HTTPServer(application, xheaders=True)
    http_server.listen(options.port)
    tornado.ioloop.IOLoop.instance().start()

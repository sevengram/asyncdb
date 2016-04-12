# Copyright 2011-2015 MongoDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Tornado support for Motor, an asynchronous driver for MySQL."""

from __future__ import unicode_literals, absolute_import

from . import core
from ..frameworks import tornado as tornado_framework
from ..frameworks.pool import SocketPool
from ..meta import create_class_with_framework


def create_mysql_class(cls):
    return create_class_with_framework(cls, tornado_framework, 'asyncdb')


MysqlClient = create_mysql_class(core.AgnosticConnection)

MysqlCursor = create_mysql_class(core.AgnosticCursor)


class MysqlConnPool(object):
    def __init__(self, framework,
                 host, port, user, password, database,
                 max_size=100, net_timeout=120, conn_timeout=120):
        io_loop = framework.get_event_loop()
        self.sock_pool = SocketPool(io_loop, framework,
                                    (host, port),
                                    max_size,
                                    net_timeout,
                                    conn_timeout)
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database

    def get_connection(self):
        return MysqlClient(self)

    def get_sock_info(self):
        return self.sock_pool.get_socket()

    def return_sock_info(self, sock_info):
        self.sock_pool.maybe_return_socket(sock_info)


class TorMysqlPool(MysqlConnPool):
    def __init__(self, host, port, user, password, database,
                 max_size=100, net_timeout=120, conn_timeout=120):
        super(self.__class__, self).__init__(tornado_framework,
                                             host, port, user, password, database,
                                             max_size, net_timeout, conn_timeout)

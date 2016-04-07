from __future__ import unicode_literals, absolute_import

import pymysql.connections
import pymysql.cursors

from ..frameworks.pool import SocketPool
from ..meta import *


class AgnosticBase(object):
    def __eq__(self, other):
        # TODO: verify this is well-tested, the isinstance test is tricky.
        if isinstance(other, self.__class__) and hasattr(self, 'delegate') and hasattr(other, 'delegate'):
            return self.delegate == other.delegate
        return NotImplemented

    def __init__(self, delegate):
        self.delegate = delegate

    def __repr__(self):
        return '%s(%r)' % (self.__class__.__name__, self.delegate)


class DelegateConnection(pymysql.connections.Connection):
    def create_pool(self, io_loop, framework):
        self.connection_pool = SocketPool(io_loop, framework, (self.host, self.port),
                                          10,
                                          self.connect_timeout,
                                          self.connect_timeout,
                                          self.ssl,
                                          True)

    def connect(self, sock=None):
        try:
            # TODO: Should use socket pool
            self.socket = self.connection_pool.get_socket().sock
            self._rfile = self.socket.makefile('rb')
            self._next_seq_id = 0

            self._get_server_information()
            self._request_authentication()

            if self.sql_mode is not None:
                c = self.cursor()
                c.execute("SET sql_mode=%s", (self.sql_mode,))

            if self.init_command is not None:
                c = self.cursor()
                c.execute(self.init_command)
                c.close()
                self.commit()

            if self.autocommit_mode is not None:
                self.autocommit(self.autocommit_mode)
        except BaseException as e:
            self._rfile = None
            if sock is not None:
                try:
                    sock.close()
                except:
                    pass
            # If e is neither DatabaseError or IOError, It's a bug.
            # But raising AssertionError hides original error.
            # So just reraise it.
            raise


class AgnosticConnection(AgnosticBase):
    __motor_class_name__ = 'AsyncClient'
    __delegate_class__ = DelegateConnection

    close = AsyncCommand()
    open = ReadOnlyProperty()
    autocommit = AsyncCommand()
    get_autocommit = DelegateMethod()
    begin = AsyncCommand()
    commit = AsyncCommand()
    rollback = AsyncCommand()
    show_warnings = AsyncRead()
    select_db = AsyncCommand()
    escape = DelegateMethod()
    literal = DelegateMethod()
    escape_string = DelegateMethod()
    cursor = DelegateMethod()
    query = AsyncCommand()
    next_result = AsyncCommand()
    affected_rows = DelegateMethod()
    kill = AsyncCommand()
    ping = AsyncCommand()
    set_charset = AsyncCommand()
    connect = AsyncCommand()
    write_packet = AsyncWrite()
    insert_id = AsyncCommand()
    thread_id = DelegateMethod()
    character_set_name = DelegateMethod()
    get_host_info = DelegateMethod()
    get_proto_info = DelegateMethod()
    get_server_info = DelegateMethod()

    def __init__(self, *args, **kwargs):
        self.io_loop = self._framework.get_event_loop()
        cursor_class = create_class_with_framework(AgnosticCursor, self._framework, self.__module__)
        delegate = self.__delegate_class__(host='localhost',
                                           user='root',
                                           password='root',
                                           defer_connect=True,
                                           db='wechat_platform',
                                           autocommit=True,
                                           cursorclass=cursor_class)
        delegate.create_pool(self.io_loop, self._framework)
        super(self.__class__, self).__init__(delegate)

    def get_io_loop(self):
        return self.io_loop


class AgnosticCursor(AgnosticBase):
    __motor_class_name__ = 'AsyncCursor'
    __delegate_class__ = pymysql.cursors.DictCursor

    close = AsyncCommand()
    setinputsizes = DelegateMethod()
    setoutputsizes = DelegateMethod()
    nextset = AsyncRead()
    mogrify = DelegateMethod()
    execute = AsyncCommand()
    executemany = AsyncCommand()
    callproc = AsyncCommand()
    fetchone = DelegateMethod()
    fetchmany = DelegateMethod()
    fetchall = DelegateMethod()
    scroll = DelegateMethod()

    def __init__(self, connection, *args, **kwargs):
        self.io_loop = self._framework.get_event_loop()
        delegate = self.__delegate_class__(connection)
        super(self.__class__, self).__init__(delegate)

    def get_io_loop(self):
        return self.io_loop

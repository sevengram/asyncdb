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

"""A connection pool that uses Motor's framework-specific sockets."""

from __future__ import unicode_literals, absolute_import

import collections
import functools
import greenlet
import socket
import time
from select import select

from ..errors import ConnectionFailure

HAS_SSL = True
try:
    import ssl
except ImportError:
    ssl = None
    HAS_SSL = False


class SocketInfo(object):
    """
    Store a socket with some metadata
    """

    def __init__(self, sock, pool_id, host=None):
        self.sock = sock
        self.host = host
        self.authset = set()
        self.closed = False
        self.last_checkout = time.time()
        self.forced = False
        self.connected = False

        self._min_wire_version = None
        self._max_wire_version = None

        # The pool's pool_id changes with each reset() so we can close sockets
        # created before the last reset.
        self.pool_id = pool_id

    def close(self):
        self.closed = True
        # Avoid exceptions on interpreter shutdown.
        try:
            self.sock.close()
        except:
            pass

    def set_wire_version_range(self, min_wire_version, max_wire_version):
        self._min_wire_version = min_wire_version
        self._max_wire_version = max_wire_version

    @property
    def min_wire_version(self):
        assert self._min_wire_version is not None
        return self._min_wire_version

    @property
    def max_wire_version(self):
        assert self._max_wire_version is not None
        return self._max_wire_version

    def __eq__(self, other):
        # Need to check if other is NO_REQUEST or NO_SOCKET_YET, and then check
        # if its sock is the same as ours
        return hasattr(other, 'sock') and self.sock == other.sock

    def __ne__(self, other):
        return not self == other

    def __hash__(self):
        return hash(self.sock)

    def __repr__(self):
        return "SocketInfo(%s)%s at %s" % (
            repr(self.sock),
            self.closed and " CLOSED" or "",
            id(self)
        )


def _closed(sock):
    """Return True if we know socket has been closed, False otherwise.
    """
    try:
        rd, _, _ = select([sock], [], [], 0)
    # Any exception here is equally bad (select.error, ValueError, etc.).
    except:
        return True
    return len(rd) > 0


class SocketOptions(object):
    def __init__(
            self,
            resolver,
            address,
            family,
            use_ssl,
            certfile,
            keyfile,
            ca_certs,
            cert_reqs,
            socket_keepalive
    ):
        self.resolver = resolver
        self.address = address
        self.family = family
        self.use_ssl = use_ssl
        self.certfile = certfile
        self.keyfile = keyfile
        self.ca_certs = ca_certs
        self.cert_reqs = cert_reqs
        self.socket_keepalive = socket_keepalive


class SocketPool(object):
    def __init__(
            self,
            io_loop,
            framework,
            pair,
            max_size,
            net_timeout,
            conn_timeout,
            use_ssl=False,
            use_greenlets=True,
            ssl_keyfile=None,
            ssl_certfile=None,
            ssl_cert_reqs=None,
            ssl_ca_certs=None,
            wait_queue_timeout=None,
            wait_queue_multiple=None,
            socket_keepalive=False):
        """
        A connection pool that uses Motor's framework-specific sockets.

        :Parameters:
          - `io_loop`: An IOLoop instance
          - `framework`: An asynchronous framework
          - `pair`: a (hostname, port) tuple
          - `max_size`: The maximum number of open sockets. Calls to
            `get_socket` will block if this is set, this pool has opened
            `max_size` sockets, and there are none idle. Set to `None` to
             disable.
          - `net_timeout`: timeout in seconds for operations on open connection
          - `conn_timeout`: timeout in seconds for establishing connection
          - `use_ssl`: bool, if True use an encrypted connection
          - `use_greenlets`: ignored.
          - `ssl_keyfile`: The private keyfile used to identify the local
            connection against mongod.  If included with the ``certfile` then
            only the ``ssl_certfile`` is needed.  Implies ``ssl=True``.
          - `ssl_certfile`: The certificate file used to identify the local
            connection against mongod. Implies ``ssl=True``.
          - `ssl_cert_reqs`: Specifies whether a certificate is required from
            the other side of the connection, and whether it will be validated
            if provided. It must be one of the three values ``ssl.CERT_NONE``
            (certificates ignored), ``ssl.CERT_OPTIONAL``
            (not required, but validated if provided), or ``ssl.CERT_REQUIRED``
            (required and validated). If the value of this parameter is not
            ``ssl.CERT_NONE``, then the ``ssl_ca_certs`` parameter must point
            to a file of CA certificates. Implies ``ssl=True``.
          - `ssl_ca_certs`: The ca_certs file contains a set of concatenated
            "certification authority" certificates, which are used to validate
            certificates passed from the other end of the connection.
            Implies ``ssl=True``.
          - `wait_queue_timeout`: (integer) How long (in milliseconds) a
            callback will wait for a socket from the pool if the pool has no
            free sockets.
          - `wait_queue_multiple`: (integer) Multiplied by max_pool_size to
            give the number of callbacks allowed to wait for a socket at one
            time.
          - `socket_keepalive`: (boolean) Whether to send periodic keep-alive
            packets on connected sockets. Defaults to ``False`` (do not send
            keep-alive packets).

        .. versionchanged:: 0.2
           ``max_size`` is now a hard cap. ``wait_queue_timeout`` and
           ``wait_queue_multiple`` have been added.
        """
        assert isinstance(pair, tuple), "pair must be a tuple"
        self.io_loop = io_loop
        self._framework = framework
        self.sockets = set()
        self.pair = pair
        self.max_size = max_size
        self.net_timeout = net_timeout
        self.conn_timeout = conn_timeout
        self.wait_queue_timeout = wait_queue_timeout
        self.wait_queue_multiple = wait_queue_multiple

        # Check if dealing with a unix domain socket
        host, port = pair
        if host.endswith('.sock'):
            if not hasattr(socket, 'AF_UNIX'):
                raise ConnectionFailure(
                    "UNIX-sockets are not supported on this system")

            self.is_unix_socket = True
            family = socket.AF_UNIX
        else:
            # Don't try IPv6 if we don't support it. Also skip it if host
            # is 'localhost' (::1 is fine). Avoids slow connect issues
            # like PYTHON-356.
            self.is_unix_socket = False
            family = socket.AF_INET
            if socket.has_ipv6 and host != 'localhost':
                family = socket.AF_UNSPEC

        if HAS_SSL and use_ssl and not ssl_cert_reqs:
            ssl_cert_reqs = ssl.CERT_NONE

        self._motor_socket_options = SocketOptions(
            resolver=self._framework.get_resolver(self.io_loop),
            address=pair,
            family=family,
            use_ssl=use_ssl,
            certfile=ssl_certfile,
            keyfile=ssl_keyfile,
            ca_certs=ssl_ca_certs,
            cert_reqs=ssl_cert_reqs,
            socket_keepalive=socket_keepalive)

        # Keep track of resets, so we notice sockets created before the most
        # recent reset and close them.
        self.pool_id = 0

        # How often to check sockets proactively for errors. An attribute
        # so it can be overridden in unittests.
        self._check_interval_seconds = 1

        self.motor_sock_counter = 0
        self.queue = collections.deque()

        # Timeout handles to expire waiters after wait_queue_timeout.
        self.waiter_timeouts = {}
        if self.wait_queue_multiple is None:
            self.max_waiters = None
        else:
            self.max_waiters = self.max_size * self.wait_queue_multiple

    def reset(self):
        self.pool_id += 1

        sockets, self.sockets = self.sockets, set()
        for sock_info in sockets:
            sock_info.close()

    def create_connection(self):
        """
        Connect and return a socket object.
        """
        async_sock = None
        try:
            self.motor_sock_counter += 1
            async_sock = self._framework.create_socket(
                self.io_loop,
                self._motor_socket_options)

            if not self.is_unix_socket:
                async_sock.settimeout(self.conn_timeout or 20.0)

            # MotorSocket pauses this greenlet, and resumes when connected.
            async_sock.connect()
            async_sock.settimeout(self.net_timeout)
            return async_sock
        except:
            self.motor_sock_counter -= 1
            if async_sock is not None:
                async_sock.close()
            raise

    def connect(self, force=False):
        """
        Connect to database and return a new connected MotorSocket. Note that
        the pool does not keep a reference to the socket -- you must call
        maybe_return_socket() when you're done with it.
        """
        child_gr = greenlet.getcurrent()
        parent = child_gr.parent
        assert parent is not None, "Should be on child greenlet"

        if not force and self.max_size and self.motor_sock_counter >= self.max_size:
            if self.max_waiters and len(self.queue) >= self.max_waiters:
                raise self._create_wait_queue_timeout()

            # TODO: waiter = stack_context.wrap(child_gr.switch)
            waiter = child_gr.switch
            self.queue.append(waiter)

            if self.wait_queue_timeout is not None:
                def on_timeout():
                    if waiter in self.queue:
                        self.queue.remove(waiter)

                    t = self.waiter_timeouts.pop(waiter)
                    self._framework.call_later_cancel(self.io_loop, t)
                    child_gr.throw(self._create_wait_queue_timeout())

                timeout = self._framework.call_later(
                    self.io_loop, self.wait_queue_timeout, on_timeout)

                self.waiter_timeouts[waiter] = timeout

            # Yield until maybe_return_socket passes spare socket in.
            return parent.switch()
        else:
            motor_sock = self.create_connection()
            return SocketInfo(motor_sock, self.pool_id, self.pair[0])

    def get_socket(self, force=False):
        """Get a socket from the pool.

        Returns a :class:`SocketInfo` object wrapping a connected
        :class:`MotorSocket`, and a bool saying whether the socket was from
        the pool or freshly created.

        :Parameters:
          - `force`: optional boolean, forces a connection to be returned
              without blocking, even if `max_size` has been reached.
        """
        forced = False
        if force:
            # If we're doing an internal operation, attempt to play nicely with
            # max_size, but if there is no open "slot" force the connection
            # and mark it as forced so we don't decrement motor_sock_counter
            # when it's returned.
            if self.motor_sock_counter >= self.max_size:
                forced = True

        if self.sockets:
            sock_info, from_pool = self.sockets.pop(), True
            sock_info = self._check(sock_info)
        else:
            sock_info, from_pool = self.connect(force=force), False

        sock_info.forced = forced
        sock_info.last_checkout = time.time()
        return sock_info

    def start_request(self):
        raise NotImplementedError("Motor doesn't implement requests")

    in_request = end_request = start_request

    def discard_socket(self, sock_info):
        """Close and discard the active socket."""
        if sock_info:
            sock_info.close()

    def maybe_return_socket(self, sock_info):
        """
        Return the socket to the pool.

        In PyMongo this method only returns the socket if it's not the request
        socket, but Motor doesn't do requests.
        """
        if not sock_info:
            return

        if sock_info.closed:
            # if not sock_info.forced:
            self.motor_sock_counter -= 1
            return

        # Give it to the greenlet at the head of the line, or return it to the
        # pool, or discard it.
        if self.queue:
            waiter = self.queue.popleft()
            if waiter in self.waiter_timeouts:
                timeout = self.waiter_timeouts.pop(waiter)
                self._framework.call_later_cancel(self.io_loop, timeout)
            self._framework.call_soon(self.io_loop,
                                      functools.partial(waiter, sock_info))
        elif self.motor_sock_counter <= self.max_size and sock_info.pool_id == self.pool_id:
            self.sockets.add(sock_info)
        else:
            sock_info.close()
            # if not sock_info.forced:
            self.motor_sock_counter -= 1

        if sock_info.forced:
            sock_info.forced = False

    def _check(self, sock_info):
        """
        This side-effecty function checks if this pool has been reset since
        the last time this socket was used, or if the socket has been closed by
        some external network error, and if so, attempts to create a new socket.
        If this connection attempt fails we reset the pool and reraise the
        error.

        Checking sockets lets us avoid seeing *some*
        :class:`~pymongo.errors.AutoReconnect` exceptions on server
        hiccups, etc. We only do this if it's been > 1 second since
        the last socket checkout, to keep performance reasonable - we
        can't avoid AutoReconnects completely anyway.
        """
        error = False
        interval = self._check_interval_seconds

        if sock_info.closed:
            error = True

        elif self.pool_id != sock_info.pool_id:
            sock_info.close()
            error = True

        elif interval is not None and time.time() - sock_info.last_checkout > interval:
            if _closed(sock_info.sock):
                sock_info.close()
                error = True
        elif time.time() - sock_info.last_checkout > 1:
            if _closed(sock_info.sock):
                sock_info.close()
                error = True

        if not error:
            return sock_info
        else:
            # This socket is out of the pool and we won't return it.
            self.motor_sock_counter -= 1
            try:
                return self.connect()
            except socket.error:
                self.reset()
                raise

    def __del__(self):
        # Avoid ResourceWarnings in Python 3.
        for sock_info in self.sockets:
            sock_info.close()

        self._framework.close_resolver(self._motor_socket_options.resolver)

    def _create_wait_queue_timeout(self):
        return ConnectionFailure(
            'Timed out waiting for socket from pool with max_size %r and'
            ' wait_queue_timeout %r' % (self.max_size, self.wait_queue_timeout))

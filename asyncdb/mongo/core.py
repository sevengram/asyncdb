# Copyright 2009-2014 MongoDB, Inc.
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

from __future__ import unicode_literals, absolute_import

import textwrap

import pymongo
import pymongo.bulk
import pymongo.command_cursor
import pymongo.cursor
import pymongo.database
import pymongo.errors
import pymongo.mongo_replica_set_client
import pymongo.son_manipulator

from ..errors import *
from ..event import MotorGreenletEvent
from ..frameworks.pool import SocketPool
from ..meta import *
from ..pycompat import PY35


class AgnosticBase(object):
    def __eq__(self, other):
        # TODO: verify this is well-tested, the isinstance test is tricky.
        if isinstance(other, self.__class__) and hasattr(self, 'delegate') and hasattr(other, 'delegate'):
            return self.delegate == other.delegate
        return NotImplemented

    name = ReadOnlyProperty()
    get_document_class = DelegateMethod()
    set_document_class = DelegateMethod()
    document_class = ReadWriteProperty()
    read_preference = ReadWriteProperty()
    tag_sets = ReadWriteProperty()
    secondary_acceptable_latency_ms = ReadWriteProperty()
    write_concern = ReadWriteProperty()
    uuid_subtype = ReadWriteProperty()

    def __init__(self, delegate):
        self.delegate = delegate

    def __repr__(self):
        return '%s(%r)' % (self.__class__.__name__, self.delegate)


class AgnosticClientBase(AgnosticBase):
    """MotorClient and MotorReplicaSetClient common functionality."""
    database_names = AsyncRead()
    server_info = AsyncRead()
    alive = AsyncRead()
    close_cursor = AsyncCommand()
    drop_database = AsyncCommand().unwrap('MotorDatabase')
    disconnect = DelegateMethod()
    tz_aware = ReadOnlyProperty()
    close = DelegateMethod()
    is_primary = ReadOnlyProperty()
    is_mongos = ReadOnlyProperty()
    max_bson_size = ReadOnlyProperty()
    max_message_size = ReadOnlyProperty()
    min_wire_version = ReadOnlyProperty()
    max_wire_version = ReadOnlyProperty()
    max_pool_size = ReadOnlyProperty()
    _ensure_connected = AsyncRead()

    def __init__(self, io_loop, *args, **kwargs):
        pool_class = functools.partial(SocketPool, io_loop, self._framework)
        kwargs['_pool_class'] = pool_class
        kwargs['_connect'] = False
        delegate = self.__delegate_class__(*args, **kwargs)
        super(AgnosticClientBase, self).__init__(delegate)
        if io_loop:
            self._framework.check_event_loop(io_loop)
            self.io_loop = io_loop
        else:
            self.io_loop = self._framework.get_event_loop()

    def get_io_loop(self):
        return self.io_loop

    def __getattr__(self, name):
        db_class = create_class_with_framework(
            AgnosticDatabase, self._framework, self.__module__)

        return db_class(self, name)

    __getitem__ = __getattr__

    def get_default_database(self):
        """Get the database named in the MongoDB connection URI.

        Useful in scripts where you want to choose which database to use
        based only on the URI in a configuration file.
        """
        attr_name = mangle_delegate_name(self.__class__, '__default_database_name')
        default_db_name = getattr(self.delegate, attr_name)
        if default_db_name is None:
            raise ConfigurationError('No default database defined')

        return self[default_db_name]


class AgnosticClient(AgnosticClientBase):
    __motor_class_name__ = 'MotorClient'
    __delegate_class__ = pymongo.mongo_client.MongoClient

    kill_cursors = AsyncCommand()
    fsync = AsyncCommand()
    unlock = AsyncCommand()
    nodes = ReadOnlyProperty()
    host = ReadOnlyProperty()
    port = ReadOnlyProperty()

    _simple_command = AsyncRead(attr_name='__simple_command')
    _socket = AsyncRead(attr_name='__socket')

    def __init__(self, *args, **kwargs):
        """Create a new connection to a single MongoDB instance at *host:port*.

        MotorClient takes the same constructor arguments as
        :class:`~pymongo.mongo_client.MongoClient`, as well as:

        :Parameters:
          - `io_loop` (optional): Special :class:`tornado.ioloop.IOLoop`
            instance to use instead of default
        """
        if 'io_loop' in kwargs:
            io_loop = kwargs.pop('io_loop')
        else:
            io_loop = self._framework.get_event_loop()

        event_class = functools.partial(MotorGreenletEvent, io_loop, self._framework)
        kwargs['_event_class'] = event_class

        # Our class is not actually AgnosticClient here, it's the version of
        # 'MotorClient' that create_class_with_framework created.
        super(self.__class__, self).__init__(io_loop, *args, **kwargs)

    def open(self, callback=None):
        """Connect to the server.

        Takes an optional callback, or returns a Future that resolves to
        ``self`` when opened. This is convenient for checking at program
        startup time whether you can connect.

        ``open`` raises a :exc:`~pymongo.errors.ConnectionFailure` if it
        cannot connect, but note that auth failures aren't revealed until
        you attempt an operation on the open client.

        :Parameters:
         - `callback`: Optional function taking parameters (self, error)

        .. versionchanged:: 0.2
           :class:`MotorClient` now opens itself on demand, calling ``open``
           explicitly is now optional.
        """
        return self._framework.future_or_callback(self._ensure_connected(True),
                                                  callback,
                                                  self.get_io_loop(),
                                                  self)

    def _get_member(self):
        # TODO: expose the PyMongo Member, or otherwise avoid this.
        return self.delegate._MongoClient__member

    def _get_pools(self):
        member = self._get_member()
        return [member.pool] if member else [None]

    def _get_primary_pool(self):
        return self._get_pools()[0]


class AgnosticReplicaSetClient(AgnosticClientBase):
    __motor_class_name__ = 'MotorReplicaSetClient'
    __delegate_class__ = pymongo.mongo_replica_set_client.MongoReplicaSetClient

    primary = ReadOnlyProperty()
    secondaries = ReadOnlyProperty()
    arbiters = ReadOnlyProperty()
    hosts = ReadOnlyProperty()
    seeds = DelegateMethod()
    close = DelegateMethod()

    _simple_command = AsyncRead(attr_name='__simple_command')
    _socket = AsyncRead(attr_name='__socket')

    def __init__(self, *args, **kwargs):
        """Create a new connection to a MongoDB replica set.

        MotorReplicaSetClient takes the same constructor arguments as
        :class:`~pymongo.mongo_replica_set_client.MongoReplicaSetClient`,
        as well as:

        :Parameters:
          - `io_loop` (optional): Special :class:`tornado.ioloop.IOLoop`
            instance to use instead of default
        """
        if 'io_loop' in kwargs:
            io_loop = kwargs.pop('io_loop')
        else:
            io_loop = self._framework.get_event_loop()

        kwargs['_monitor_class'] = functools.partial(
            MotorReplicaSetMonitor, io_loop, self._framework)

        # Our class is not actually AgnosticClient here, it's the version of
        # 'MotorClient' that create_class_with_framework created.
        super(self.__class__, self).__init__(io_loop, *args, **kwargs)

    def open(self, callback=None):
        """Connect to the server.

        Takes an optional callback, or returns a Future that resolves to
        ``self`` when opened. This is convenient for checking at program
        startup time whether you can connect.

        ``open`` raises a :exc:`~pymongo.errors.ConnectionFailure` if it
        cannot connect, but note that auth failures aren't revealed until
        you attempt an operation on the open client.

        :Parameters:
         - `callback`: Optional function taking parameters (self, error)

        .. versionchanged:: 0.2
           :class:`MotorReplicaSetClient` now opens itself on demand, calling
           ``open`` explicitly is now optional.
        """
        loop = self.get_io_loop()
        future = self._framework.get_future(loop)
        retval = self._framework.future_or_callback(future, callback, loop)
        connected_callback = functools.partial(self._connected_callback, future)
        self._ensure_connected(sync=True, callback=connected_callback)
        return retval

    def _connected_callback(self, future, result, error):
        if error:
            # TODO: exc_info.
            future.set_exception(error)
        elif not self._get_member():
            future.set_exception(pymongo.errors.AutoReconnect('no primary is available'))
        else:
            future.set_result(self)

    def _get_member(self):
        # TODO: expose the PyMongo RSC members, or otherwise avoid this.
        # This raises if the RSState's error is set.
        rs_state = self.delegate._MongoReplicaSetClient__get_rs_state()
        return rs_state.primary_member

    def _get_pools(self):
        rs_state = self._get_member()
        return [member.pool for member in rs_state._members]

    def _get_primary_pool(self):
        primary_member = self._get_member()
        return primary_member.pool if primary_member else None


# PyMongo uses a background thread to regularly inspect the replica set and
# monitor it for changes. In Motor, use a periodic callback on the IOLoop to
# monitor the set.
class MotorReplicaSetMonitor(pymongo.mongo_replica_set_client.Monitor):
    def __init__(self, loop, framework, rsc):
        msg = (
            "First argument to MotorReplicaSetMonitor must be"
            " MongoReplicaSetClient, not %r" % rsc)

        assert isinstance(
            rsc, pymongo.mongo_replica_set_client.MongoReplicaSetClient), msg

        # Super makes two MotorGreenletEvents: self.event and self.refreshed.
        # We only use self.refreshed.
        event_class = functools.partial(MotorGreenletEvent, loop, framework)

        pymongo.mongo_replica_set_client.Monitor.__init__(self, rsc, event_class=event_class)
        self.timeout_handle = None
        self.started = False
        self.loop = loop
        self._framework = framework

    def shutdown(self, _=None):
        self.stopped = True
        if self.timeout_handle:
            self._framework.call_later_cancel(self.loop, self.timeout_handle)
            self.timeout_handle = None

    def refresh(self):
        assert greenlet.getcurrent().parent is not None, "Should be on child greenlet"

        try:
            self.rsc.refresh()
        except (pymongo.errors.AutoReconnect, IOError, OSError):
            # Stream closed.
            pass
        except ReferenceError:
            # rsc was collected.
            return
        except Exception:
            pass
        finally:
            # Switch to greenlets blocked in wait_for_refresh().
            self.refreshed.set()

        self.timeout_handle = self._framework.call_later(
            self.loop, self._refresh_interval, self.async_refresh)

    def async_refresh(self):
        greenlet.greenlet(self.refresh).switch()

    def start(self):
        self.started = True
        self.timeout_handle = self._framework.call_later(
            self.loop, self._refresh_interval, self.async_refresh)

    start_sync = start

    def schedule_refresh(self):
        self.refreshed.clear()
        if self.timeout_handle:
            self._framework.call_later_cancel(self.loop, self.timeout_handle)
            self.timeout_handle = None

        self._framework.call_soon(self.loop, self.async_refresh)

    def join(self, timeout=None):
        # PyMongo calls join() after shutdown() -- this is not a thread, so
        # shutdown works immediately and join is unnecessary
        pass

    def wait_for_refresh(self, timeout_seconds):
        assert greenlet.getcurrent().parent is not None, "Should be on child greenlet"

        # self.refreshed is a util.MotorGreenletEvent.
        self.refreshed.wait(timeout_seconds)

    def is_alive(self):
        return self.started and not self.stopped

    isAlive = is_alive


class AgnosticDatabase(AgnosticBase):
    __motor_class_name__ = 'MotorDatabase'
    __delegate_class__ = pymongo.database.Database

    set_profiling_level = AsyncCommand()
    add_user = AsyncCommand()
    remove_user = AsyncCommand()
    logout = AsyncCommand()
    command = AsyncCommand()
    authenticate = AsyncCommand()
    eval = AsyncCommand()
    # TODO: for consistency, wrap() takes a string too.
    create_collection = AsyncCommand().wrap(pymongo.database.Collection)
    drop_collection = AsyncCommand().unwrap('MotorCollection')
    validate_collection = AsyncRead().unwrap('MotorCollection')
    collection_names = AsyncRead()
    current_op = AsyncRead()
    profiling_level = AsyncRead()
    profiling_info = AsyncRead()
    error = AsyncRead()
    last_status = AsyncRead()
    previous_error = AsyncRead()
    reset_error_history = AsyncCommand()
    dereference = AsyncRead()

    incoming_manipulators = ReadOnlyProperty()
    incoming_copying_manipulators = ReadOnlyProperty()
    outgoing_manipulators = ReadOnlyProperty()
    outgoing_copying_manipulators = ReadOnlyProperty()

    def __init__(self, connection, name):
        if not isinstance(connection, AgnosticClientBase):
            raise TypeError("First argument to MotorDatabase must be "
                            "a Motor client, not %r" % connection)

        self.connection = connection
        delegate = pymongo.database.Database(connection.delegate, name)
        super(self.__class__, self).__init__(delegate)

    def __getattr__(self, name):
        collection_class = create_class_with_framework(
            AgnosticCollection, self._framework, self.__module__)

        return collection_class(self, name)

    __getitem__ = __getattr__

    def __call__(self, *args, **kwargs):
        database_name = self.delegate.name
        client_class_name = self.connection.__class__.__name__
        if database_name == 'open_sync':
            raise TypeError(
                "%s.open_sync() is unnecessary Motor 0.2, "
                "see changelog for details." % client_class_name)

        raise TypeError(
            "MotorDatabase object is not callable. If you meant to "
            "call the '%s' method on a %s object it is "
            "failing because no such method exists." % (
                database_name, client_class_name))

    def wrap(self, collection):
        # Replace pymongo.collection.Collection with MotorCollection.
        return self[collection.name]

    def add_son_manipulator(self, manipulator):
        """Add a new son manipulator to this database.

        Newly added manipulators will be applied before existing ones.

        :Parameters:
          - `manipulator`: the manipulator to add
        """
        # We override add_son_manipulator to unwrap the AutoReference's
        # database attribute.
        if isinstance(manipulator, pymongo.son_manipulator.AutoReference):
            db = manipulator.database
            db_class = create_class_with_framework(
                AgnosticDatabase,
                self._framework,
                self.__module__)

            if isinstance(db, db_class):
                # db is a MotorDatabase; get the PyMongo Database instance.
                manipulator.database = db.delegate

        self.delegate.add_son_manipulator(manipulator)

    def get_io_loop(self):
        return self.connection.get_io_loop()


class AgnosticCollection(AgnosticBase):
    __motor_class_name__ = 'MotorCollection'
    __delegate_class__ = pymongo.database.Collection

    create_index = AsyncCommand()
    drop_indexes = AsyncCommand()
    drop_index = AsyncCommand()
    drop = AsyncCommand()
    ensure_index = AsyncCommand()
    reindex = AsyncCommand()
    rename = AsyncCommand()
    find_and_modify = AsyncCommand()
    map_reduce = AsyncCommand().wrap(pymongo.database.Collection)
    update = AsyncWrite()
    insert = AsyncWrite()
    remove = AsyncWrite()
    save = AsyncWrite()
    index_information = AsyncRead()
    count = AsyncRead()
    options = AsyncRead()
    group = AsyncRead()
    distinct = AsyncRead()
    inline_map_reduce = AsyncRead()
    find_one = AsyncRead()
    full_name = ReadOnlyProperty()

    _async_aggregate = AsyncRead(attr_name='aggregate')
    __parallel_scan = AsyncRead(attr_name='parallel_scan')

    def __init__(self, database, name):
        db_class = create_class_with_framework(
            AgnosticDatabase, self._framework, self.__module__)

        if not isinstance(database, db_class):
            raise TypeError("First argument to MotorCollection must be "
                            "MotorDatabase, not %r" % database)

        delegate = pymongo.database.Collection(database.delegate, name)
        super(self.__class__, self).__init__(delegate)
        self.database = database

    def __getattr__(self, name):
        # Dotted collection name, like "foo.bar".
        collection_class = create_class_with_framework(
            AgnosticCollection, self._framework, self.__module__)

        return collection_class(self.database, self.name + '.' + name)

    def __call__(self, *args, **kwargs):
        raise TypeError(
            "MotorCollection object is not callable. If you meant to "
            "call the '%s' method on a MotorCollection object it is "
            "failing because no such method exists." %
            self.delegate.name)

    def find(self, *args, **kwargs):
        """Create a :class:`MotorCursor`. Same parameters as for
        PyMongo's :meth:`~pymongo.collection.Collection.find`.

        Note that ``find`` does not take a `callback` parameter, nor does
        it return a Future, because ``find`` merely creates a
        :class:`MotorCursor` without performing any operations on the server.
        ``MotorCursor`` methods such as :meth:`~MotorCursor.to_list` or
        :meth:`~MotorCursor.count` perform actual operations.
        """
        if 'callback' in kwargs:
            raise pymongo.errors.InvalidOperation("Pass a callback to each, to_list, or count, not to find.")

        cursor = self.delegate.find(*args, **kwargs)
        cursor_class = create_class_with_framework(
            AgnosticCursor, self._framework, self.__module__)

        return cursor_class(cursor, self)

    def aggregate(self, pipeline, **kwargs):
        """Execute an aggregation pipeline on this collection.

        The aggregation can be run on a secondary if the client is a
        :class:`~motor.MotorReplicaSetClient` and its ``read_preference`` is not
        :attr:`PRIMARY`.

        :Parameters:
          - `pipeline`: a single command or list of aggregation commands
          - `**kwargs`: send arbitrary parameters to the aggregate command

        Returns a `MotorCommandCursor` that can be iterated like a cursor from
        `find`::

          pipeline = [{'$project': {'name': {'$toUpper': '$name'}}}]
          cursor = collection.aggregate(pipeline)
          while (yield cursor.fetch_next):
              doc = cursor.next_object()
              print(doc)

        In Python 3.5 and newer, aggregation cursors can be iterated elegantly
        in native coroutines with `async for`::

          async def f():
              async for doc in collection.aggregate(pipeline):
                  doc = cursor.next_object()
                  print(doc)

        MongoDB versions 2.4 and older do not support aggregation cursors; use
        ``yield`` and pass ``cursor=False`` for compatibility with older
        MongoDBs::

          reply = yield collection.aggregate(cursor=False)
          for doc in reply['results']:
              print(doc)

        .. versionchanged:: 0.5
           `aggregate` now returns a cursor by default, and the cursor is
           returned immediately without a ``yield``.
           See :ref:`aggregation changes in Motor 0.5 <aggregate_changes_0_5>`.

        .. versionchanged:: 0.2
           Added cursor support.

        .. _aggregate command:
            http://docs.mongodb.org/manual/applications/aggregation

        """
        if kwargs.get('cursor') is False:
            kwargs.pop('cursor')
            # One-shot aggregation, no cursor. Send command now, return Future.
            return self._async_aggregate(pipeline, **kwargs)
        else:
            if 'callback' in kwargs:
                raise pymongo.errors.InvalidOperation(
                    "Pass a callback to to_list or each, not to aggregate.")

            kwargs.setdefault('cursor', {})
            cursor_class = create_class_with_framework(
                AgnosticAggregationCursor, self._framework, self.__module__)

            # Latent cursor that will send initial command on first "async for".
            return cursor_class(self, pipeline, **kwargs)

    def parallel_scan(self, num_cursors, **kwargs):
        """Scan this entire collection in parallel.

        Returns a list of up to ``num_cursors`` cursors that can be iterated
        concurrently. As long as the collection is not modified during
        scanning, each document appears once in one of the cursors' result
        sets.

        For example, to process each document in a collection using some
        function ``process_document()``::

            @gen.coroutine
            def process_cursor(cursor):
                while (yield cursor.fetch_next):
                    process_document(cursor.next_object())

            # Get up to 4 cursors.
            cursors = yield collection.parallel_scan(4)
            yield [process_cursor(cursor) for cursor in cursors]

            # All documents have now been processed.

        If ``process_document()`` is a coroutine, do
        ``yield process_document(document)``.

        With :class:`MotorReplicaSetClient`, pass `read_preference` of
        :attr:`~pymongo.read_preference.ReadPreference.SECONDARY_PREFERRED`
        to scan a secondary.

        :Parameters:
          - `num_cursors`: the number of cursors to return

        .. note:: Requires server version **>= 2.5.5**.
        """
        io_loop = self.get_io_loop()
        future = self._framework.get_future(io_loop)

        # Return a future, or if user passed a callback chain it to the future.
        callback = kwargs.pop('callback', None)
        retval = self._framework.future_or_callback(future, callback, io_loop)

        # Once we have PyMongo Cursors, wrap in MotorCursors and resolve the
        # future with them, or pass them to the callback.
        scan_callback = functools.partial(self._scan_callback, future)
        self.__parallel_scan(num_cursors, callback=scan_callback, **kwargs)

        return retval

    def _scan_callback(self, future, command_cursors, error):
        if error:
            # TODO: exc_info.
            future.set_exception(error)
        else:
            command_cursor_class = create_class_with_framework(
                AgnosticCommandCursor, self._framework, self.__module__)

            motor_command_cursors = [
                command_cursor_class(cursor, self)
                for cursor in command_cursors]

            future.set_result(motor_command_cursors)

    def initialize_unordered_bulk_op(self):
        """Initialize an unordered batch of write operations.

        Operations will be performed on the server in arbitrary order,
        possibly in parallel. All operations will be attempted.

        Returns a :class:`~motor.MotorBulkOperationBuilder` instance.

        See :ref:`unordered_bulk` for examples.

        .. versionadded:: 0.2
        """
        bob_class = create_class_with_framework(
            AgnosticBulkOperationBuilder, self._framework, self.__module__)

        return bob_class(self, ordered=False)

    def initialize_ordered_bulk_op(self):
        """Initialize an ordered batch of write operations.

        Operations will be performed on the server serially, in the
        order provided. If an error occurs all remaining operations
        are aborted.

        Returns a :class:`~motor.MotorBulkOperationBuilder` instance.

        See :ref:`ordered_bulk` for examples.

        .. versionadded:: 0.2
        """
        bob_class = create_class_with_framework(
            AgnosticBulkOperationBuilder,
            self._framework,
            self.__module__)

        return bob_class(self, ordered=True)

    def wrap(self, obj):
        if obj.__class__ is pymongo.database.Collection:
            return self.database[obj.name]
        elif obj.__class__ is pymongo.cursor.Cursor:
            return AgnosticCursor(obj, self)
        elif obj.__class__ is pymongo.command_cursor.CommandCursor:
            command_cursor_class = create_class_with_framework(
                AgnosticCommandCursor,
                self._framework,
                self.__module__)

            return command_cursor_class(obj, self)
        else:
            return obj

    def get_io_loop(self):
        return self.database.get_io_loop()


class AgnosticBaseCursor(AgnosticBase):
    """Base class for AgnosticCursor and AgnosticCommandCursor"""
    _refresh = AsyncRead()
    cursor_id = ReadOnlyProperty()
    alive = ReadOnlyProperty()
    batch_size = CursorChainingMethod()

    def __init__(self, cursor, collection):
        """Don't construct a cursor yourself, but acquire one from methods like
        :meth:`MotorCollection.find` or :meth:`MotorCollection.aggregate`.

        .. note::
          There is no need to manually close cursors; they are closed
          by the server after being fully iterated
          with :meth:`to_list`, :meth:`each`, or :attr:`fetch_next`, or
          automatically closed by the client when the :class:`MotorCursor` is
          cleaned up by the garbage collector.
        """
        # 'cursor' is a PyMongo Cursor, CommandCursor, or a _LatentCursor.
        super(AgnosticBaseCursor, self).__init__(delegate=cursor)
        self.collection = collection
        self.started = False
        self.closed = False

    if PY35:
        exec(textwrap.dedent("""
        async def __aiter__(self):
            return self

        async def __anext__(self):
            # An optimization: skip the "await" if possible.
            if self._buffer_size() or await self.fetch_next:
                return self.next_object()
            raise StopAsyncIteration()
        """), globals(), locals())

    def _get_more(self):
        """Initial query or getMore. Returns a Future."""
        if not self.alive:
            raise pymongo.errors.InvalidOperation(
                "Can't call get_more() on a MotorCursor that has been exhausted or killed.")

        self.started = True
        return self._refresh()

    @property
    def fetch_next(self):
        """A Future used with `gen.coroutine`_ to asynchronously retrieve the
        next document in the result set, fetching a batch of documents from the
        server if necessary. Resolves to ``False`` if there are no more
        documents, otherwise :meth:`next_object` is guaranteed to return a
        document.

        While it appears that fetch_next retrieves each document from
        the server individually, the cursor actually fetches documents
        efficiently in `large batches`_.

        In Python 3.5 and newer, cursors can be iterated elegantly and very
        efficiently in native coroutines with `async for`:
        """
        if not self._buffer_size() and self.alive:
            # Return the Future, which resolves to number of docs fetched or 0.
            return self._get_more()
        elif self._buffer_size():
            future = self._framework.get_future(self.get_io_loop())
            future.set_result(True)
            return future
        else:
            # Dead
            future = self._framework.get_future(self.get_io_loop())
            future.set_result(False)
            return future

    def next_object(self):
        """Get a document from the most recently fetched batch, or ``None``.
        See :attr:`fetch_next`.
        """
        if not self._buffer_size():
            return None
        return next(self.delegate)

    def each(self, callback):
        """Iterates over all the documents for this cursor.

        `each` returns immediately, and `callback` is executed asynchronously
        for each document. `callback` is passed ``(None, None)`` when iteration
        is complete.

        Cancel iteration early by returning ``False`` from the callback. (Only
        ``False`` cancels iteration: returning ``None`` or 0 does not.)

        .. note:: Unlike other Motor methods, ``each`` requires a callback and
           does not return a Future, so it cannot be used with
           ``gen.coroutine.`` :meth:`to_list` or :attr:`fetch_next` are much
           easier to use.

        :Parameters:
         - `callback`: function taking (document, error)
        """
        if not callable(callback):
            raise CallbackTypeError()

        self._each_got_more(callback, None)

    def _each_got_more(self, callback, future):
        if future:
            try:
                future.result()
            except Exception as error:
                callback(None, error)
                return

        while self._buffer_size() > 0:
            doc = next(self.delegate)  # decrements self.buffer_size

            # Quit if callback returns exactly False (not None). Note we
            # don't close the cursor: user may want to resume iteration.
            if callback(doc, None) is False:
                return

            # The callback closed this cursor?
            if self.closed:
                return

        if self.alive and (self.cursor_id or not self.started):
            self._get_more().add_done_callback(
                functools.partial(self._each_got_more, callback))
        else:
            # Complete
            self._framework.call_soon(
                self.get_io_loop(),
                functools.partial(callback, None, None))

    def to_list(self, length, callback=None):
        """Get a list of documents.

        :Parameters:
         - `length`: maximum number of documents to return for this call, or
           None
         - `callback` (optional): function taking (documents, error)

        If a callback is passed, returns None, else returns a Future.

        .. versionchanged:: 0.2
           `callback` must be passed as a keyword argument, like
           ``to_list(10, callback=callback)``, and the
           `length` parameter is no longer optional.
        """
        if length is not None:
            if not isinstance(length, int):
                raise TypeError('length must be an int, not %r' % length)
            elif length < 0:
                raise ValueError('length must be non-negative')

        if self._query_flags() & pymongo.cursor._QUERY_OPTIONS['tailable_cursor']:
            raise pymongo.errors.InvalidOperation("Can't call to_list on tailable cursor")

        to_list_future = self._framework.get_future(self.get_io_loop())

        # Run future_or_callback's type checking before we change anything.
        retval = self._framework.future_or_callback(to_list_future, callback, self.get_io_loop())

        if not self.alive:
            to_list_future.set_result([])
        else:
            the_list = []
            self._get_more().add_done_callback(
                functools.partial(self._to_list,
                                  length,
                                  the_list,
                                  to_list_future))

        return retval

    def _to_list(self, length, the_list, to_list_future, get_more_result):
        # get_more_result is the result of self._get_more().
        # to_list_future will be the result of the user's to_list() call.
        try:
            result = get_more_result.result()
            collection = self.collection
            fix_outgoing = collection.database.delegate._fix_outgoing

            if length is None:
                n = result
            else:
                n = min(length, result)

            for _ in range(n):
                the_list.append(fix_outgoing(self._data().popleft(),
                                             collection))

            reached_length = (length is not None and len(the_list) >= length)
            if reached_length or not self.alive:
                to_list_future.set_result(the_list)
            else:
                self._get_more().add_done_callback(
                    functools.partial(self._to_list,
                                      length,
                                      the_list,
                                      to_list_future))
        except Exception as exc:
            # TODO: lost exc_info
            to_list_future.set_exception(exc)

    def get_io_loop(self):
        return self.collection.get_io_loop()

    @motor_coroutine
    def close(self):
        """Explicitly kill this cursor on the server. Call like (in Tornado)::

           yield cursor.close()

        :Parameters:
         - `callback` (optional): function taking (result, error).

        If a callback is passed, returns None, else returns a Future.
        """
        if not self.closed:
            self.closed = True
            yield self._framework.yieldable(self._close())

    def _buffer_size(self):
        return len(self._data())

    def __del__(self):
        # This MotorCursor is deleted on whatever greenlet does the last
        # decref, or (if it's referenced from a cycle) whichever is current
        # when the GC kicks in. First, do a quick check whether the cursor
        # is still alive on the server:
        if self.cursor_id and self.alive:
            client = self.collection.database.connection
            cursor_id = self.cursor_id

            # Prevent PyMongo Cursor from attempting to kill itself; it
            # doesn't know how to schedule I/O on a greenlet.
            self._clear_cursor_id()
            self._close_exhaust_cursor()
            client.kill_cursors([cursor_id])

    # Paper over some differences between PyMongo Cursor and CommandCursor.
    def _query_flags(self):
        raise NotImplementedError

    def _data(self):
        raise NotImplementedError

    def _clear_cursor_id(self):
        raise NotImplementedError

    def _close_exhaust_cursor(self):
        raise NotImplementedError

    @motor_coroutine
    def _close(self):
        raise NotImplementedError()


class AgnosticCursor(AgnosticBaseCursor):
    __motor_class_name__ = 'MotorCursor'
    __delegate_class__ = pymongo.cursor.Cursor
    count = AsyncRead()
    distinct = AsyncRead()
    explain = AsyncRead()
    add_option = CursorChainingMethod()
    remove_option = CursorChainingMethod()
    limit = CursorChainingMethod()
    skip = CursorChainingMethod()
    max_scan = CursorChainingMethod()
    sort = CursorChainingMethod()
    hint = CursorChainingMethod()
    where = CursorChainingMethod()
    max_time_ms = CursorChainingMethod()
    min = CursorChainingMethod()
    max = CursorChainingMethod()
    comment = CursorChainingMethod()

    _Cursor__die = AsyncRead()

    def rewind(self):
        """Rewind this cursor to its unevaluated state."""
        self.delegate.rewind()
        self.started = False
        return self

    def clone(self):
        """Get a clone of this cursor."""
        return self.__class__(self.delegate.clone(), self.collection)

    def __copy__(self):
        return self.__class__(self.delegate.__copy__(), self.collection)

    def __deepcopy__(self, memo):
        return self.__class__(self.delegate.__deepcopy__(memo), self.collection)

    def _query_flags(self):
        return self.delegate._Cursor__query_flags

    def _data(self):
        return self.delegate._Cursor__data

    def _clear_cursor_id(self):
        self.delegate._Cursor__id = 0

    def _close_exhaust_cursor(self):
        # If an exhaust cursor is dying without fully iterating its results,
        # it must close the socket. PyMongo's Cursor does this, but we've
        # disabled its cleanup so we must do it ourselves.
        if self.delegate._Cursor__exhaust:
            manager = self.delegate._Cursor__exhaust_mgr
            if manager.sock:
                manager.sock.close()

            manager.close()

    @motor_coroutine
    def _close(self):
        yield self._framework.yieldable(self._Cursor__die())


class AgnosticCommandCursor(AgnosticBaseCursor):
    __motor_class_name__ = 'MotorCommandCursor'
    __delegate_class__ = pymongo.command_cursor.CommandCursor

    _CommandCursor__die = AsyncRead()

    def _query_flags(self):
        return 0

    def _data(self):
        return self.delegate._CommandCursor__data

    def _clear_cursor_id(self):
        self.delegate._CommandCursor__id = 0

    def _close_exhaust_cursor(self):
        # MongoDB doesn't have exhaust command cursors yet.
        pass

    @motor_coroutine
    def _close(self):
        yield self._framework.yieldable(self._CommandCursor__die())


class _LatentCursor(object):
    """Take the place of a PyMongo CommandCursor until aggregate() begins."""
    alive = True
    _CommandCursor__data = []
    _CommandCursor__id = None
    _CommandCursor__killed = False
    cursor_id = None

    def clone(self):
        return _LatentCursor()

    def rewind(self):
        pass


class AgnosticAggregationCursor(AgnosticCommandCursor):
    __motor_class_name__ = 'MotorAggregationCursor'

    def __init__(self, collection, pipeline, **kwargs):
        # We're being constructed without yield or await, like:
        #
        #     cursor = collection.aggregate(pipeline)
        #
        # ... so we can't send the "aggregate" command to the server and get
        # a PyMongo CommandCursor back yet. Set self.delegate to a latent
        # cursor until the first yield or await triggers _get_more().
        super(self.__class__, self).__init__(_LatentCursor(), collection)
        self.pipeline = pipeline
        self.kwargs = kwargs

    def _get_more(self):
        if not self.started:
            self.started = True
            future = self._framework.get_future(self.get_io_loop())
            self.collection._async_aggregate(
                self.pipeline,
                callback=functools.partial(self._on_get_more, future),
                **self.kwargs)

            return future

        return super(self.__class__, self)._get_more()

    def _on_get_more(self, future, result, error):
        if result:
            # "result" is a CommandCursor from PyMongo's aggregate().
            self.delegate = result

            # _get_more is complete.
            future.set_result(len(result._CommandCursor__data))
        else:
            # TODO: exc_info.
            future.set_exception(error)


class AgnosticBulkOperationBuilder(AgnosticBase):
    __motor_class_name__ = 'MotorBulkOperationBuilder'
    __delegate_class__ = pymongo.bulk.BulkOperationBuilder

    find = DelegateMethod()
    insert = DelegateMethod()
    execute = AsyncCommand()

    def __init__(self, collection, ordered):
        self.io_loop = collection.get_io_loop()
        delegate = pymongo.bulk.BulkOperationBuilder(collection.delegate, ordered)
        super(self.__class__, self).__init__(delegate)

    def get_io_loop(self):
        return self.io_loop

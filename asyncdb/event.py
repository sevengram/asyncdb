# Copyright 2013-2015 MongoDB, Inc.
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

"""A version of PyMongo's thread_util for Motor."""

from __future__ import unicode_literals

try:
    from time import monotonic as _time
except ImportError:
    from time import time as _time

import greenlet


class MotorGreenletEvent(object):
    """An Event-like class for greenlets."""

    def __init__(self, io_loop, framework):
        self.io_loop = io_loop
        self._framework = framework
        self._flag = False
        self._waiters = []
        self._timeouts = set()

    def is_set(self):
        return self._flag

    def set(self):
        self._flag = True
        timeouts, self._timeouts = self._timeouts, set()
        for timeout_handle in timeouts:
            self._framework.call_later_cancel(self.io_loop, timeout_handle)

        waiters, self._waiters = self._waiters, []
        for waiter in waiters:
            # Defer execution.
            self._framework.call_soon(self.io_loop, waiter.switch)

    def clear(self):
        self._flag = False

    def wait(self, timeout_seconds=None):
        current = greenlet.getcurrent()
        parent = current.parent
        assert parent is not None, "Should be on child greenlet"
        if not self._flag:
            self._waiters.append(current)

            def on_timeout():
                # Called from event loop on main greenlet.
                self._waiters.remove(current)
                self._timeouts.discard(timeout_handle)
                current.switch()

            if timeout_seconds is not None:
                timeout_handle = self._framework.call_later(
                    self.io_loop, timeout_seconds, on_timeout)

                self._timeouts.add(timeout_handle)

            parent.switch()

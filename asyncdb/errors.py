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

"""Exceptions raised by Asyncdb."""
import socket


class AsyncdbError(Exception):
    """Base class for all Asyncdb exceptions.
    """


class ConnectionFailure(AsyncdbError):
    """Raised when a connection to the database cannot be made or is lost.
    """


class ConfigurationError(AsyncdbError):
    """Raised when something is incorrectly configured.
    """


class OperationalError(AsyncdbError):
    """Exception raised for errors that are related to the database's
    operation and not necessarily under the control of the programmer,
    e.g. an unexpected disconnect occurs, the data source name is not
    found, a transaction could not be processed, a memory allocation
    error occurred during processing, etc."""


class CallbackTypeError(AsyncdbError):
    def __init__(self):
        AsyncdbError.__init__(self, "callback must be a callable")


SocketError = socket.error

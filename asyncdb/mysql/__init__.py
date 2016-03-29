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

from . import agnostic
from ..frameworks import tornado as tornado_framework
from ..meta import create_class_with_framework


def create_mysql_class(cls):
    return create_class_with_framework(cls, tornado_framework, 'asyncdb')


AmysqlConnection = create_mysql_class(agnostic.AgnosticConnection)

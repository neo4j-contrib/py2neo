#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright 2011-2020, Nigel Small
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


from __future__ import absolute_import

from collections import deque
from hashlib import new as hashlib_new

from py2neo.internal.compat import bstr, urlsplit, string_types
from py2neo.meta import NEO4J_URI, NEO4J_AUTH, NEO4J_USER_AGENT, NEO4J_SECURE, NEO4J_VERIFIED, \
    bolt_user_agent, http_user_agent


DEFAULT_SCHEME = "bolt"
DEFAULT_SECURE = False
DEFAULT_VERIFIED = False
DEFAULT_USER = "neo4j"
DEFAULT_PASSWORD = "password"
DEFAULT_HOST = "localhost"
DEFAULT_BOLT_PORT = 7687
DEFAULT_HTTP_PORT = 7474
DEFAULT_HTTPS_PORT = 7473

DEFAULT_MAX_CONNECTIONS = 40


def coalesce(*values):
    """ Utility function to return the first non-null value from a
    sequence of values.
    """
    for value in values:
        if value is not None:
            return value
    return None


def get_connection_data(uri=None, **settings):
    """ Generate a dictionary of connection data for an optional URI plus
    additional connection settings.

    :param uri:
    :param settings:
    :return:
    """
    data = {
        "host": None,
        "password": None,
        "port": None,
        "scheme": None,
        "secure": None,
        "verified": None,
        "user": None,
        "user_agent": None,
    }
    # apply uri
    uri = coalesce(uri, NEO4J_URI)
    if uri is not None:
        parsed = urlsplit(uri)
        if parsed.scheme is not None:
            data["scheme"] = parsed.scheme
            if data["scheme"] in ["https"]:
                data["secure"] = True
            elif data["scheme"] in ["http"]:
                data["secure"] = False
        data["user"] = coalesce(parsed.username, data["user"])
        data["password"] = coalesce(parsed.password, data["password"])
        data["host"] = coalesce(parsed.hostname, data["host"])
        data["port"] = coalesce(parsed.port, data["port"])
    # apply auth (this can override `uri`)
    if "auth" in settings and settings["auth"] is not None:
        if isinstance(settings["auth"], string_types):
            data["user"], _, data["password"] = settings["auth"].partition(":")
        else:
            data["user"], data["password"] = settings["auth"]
    elif NEO4J_AUTH is not None:
        data["user"], _, data["password"] = NEO4J_AUTH.partition(":")
    # apply components (these can override `uri` and `auth`)
    data["user_agent"] = coalesce(settings.get("user_agent"), NEO4J_USER_AGENT, data["user_agent"])
    data["secure"] = coalesce(settings.get("secure"), data["secure"], NEO4J_SECURE)
    data["verified"] = coalesce(settings.get("verified"), data["verified"], NEO4J_VERIFIED)
    data["scheme"] = coalesce(settings.get("scheme"), data["scheme"])
    data["user"] = coalesce(settings.get("user"), data["user"])
    data["password"] = coalesce(settings.get("password"), data["password"])
    data["host"] = coalesce(settings.get("host"), data["host"])
    data["port"] = coalesce(settings.get("port"), data["port"])
    # apply correct scheme for security
    if data["secure"] is True and data["scheme"] == "http":
        data["scheme"] = "https"
    if data["secure"] is False and data["scheme"] == "https":
        data["scheme"] = "http"
    # apply default port for scheme
    if data["scheme"] and not data["port"]:
        if data["scheme"] == "http":
            data["port"] = DEFAULT_HTTP_PORT
        elif data["scheme"] == "https":
            data["port"] = DEFAULT_HTTPS_PORT
        elif data["scheme"] in ["bolt"]:
            data["port"] = DEFAULT_BOLT_PORT
    # apply other defaults
    if not data["user_agent"]:
        data["user_agent"] = http_user_agent() if data["scheme"] in ["http", "https"] else bolt_user_agent()
    if data["secure"] is None:
        data["secure"] = DEFAULT_SECURE
    if data["verified"] is None:
        data["verified"] = DEFAULT_VERIFIED
    if not data["scheme"]:
        data["scheme"] = DEFAULT_SCHEME
        if data["scheme"] == "http":
            data["secure"] = False
            data["verified"] = False
        if data["scheme"] == "https":
            data["secure"] = True
            data["verified"] = True
    if not data["user"]:
        data["user"] = DEFAULT_USER
    if not data["password"]:
        data["password"] = DEFAULT_PASSWORD
    if not data["host"]:
        data["host"] = DEFAULT_HOST
    if not data["port"]:
        data["port"] = DEFAULT_BOLT_PORT
    # apply composites
    data["auth"] = (data["user"], data["password"])
    data["uri"] = "%s://%s:%s" % (data["scheme"], data["host"], data["port"])
    h = hashlib_new("md5")
    for key in sorted(data):
        h.update(bstr(data[key]))
    data["hash"] = h.hexdigest()
    return data


class Task(object):

    def done(self):
        pass

    def failure(self):
        pass

    def ignored(self):
        pass


class ItemizedTask(Task):

    def __init__(self):
        Task.__init__(self)
        self._items = deque()
        self._complete = False

    def items(self):
        return iter(self._items)

    def append(self, item, final=False):
        self._items.append(item)
        if final:
            self._complete = True

    def complete(self):
        return self._complete

    def latest(self):
        try:
            return self._items[-1]
        except IndexError:
            return None

    def done(self):
        return self.complete() and self.latest().done()

    def failure(self):
        for item in self._items:
            if item.failure():
                return item.failure()
        return None

    def ignored(self):
        return self._items and self._items[0].ignored()


class Connection(object):
    """

    :ivar cx_data: connection data
    """

    scheme = None

    user_agent = bolt_user_agent()

    server_agent = None

    @classmethod
    def open(cls, uri=None, **settings):
        pass

    def __init__(self, cx_data):
        self.cx_data = cx_data

    def close(self):
        pass

    def hello(self, auth):
        pass

    def goodbye(self):
        pass

    def reset(self):
        pass

    def auto_run(self, cypher, parameters=None,
                 db=None, readonly=False, bookmarks=None, metadata=None, timeout=None):
        pass

    def begin(self, db=None, readonly=False, bookmarks=None, metadata=None, timeout=None):
        """ Begin a transaction

        :param db:
        :param readonly:
        :param bookmarks:
        :param metadata:
        :param timeout:
        :return: new :class:`.Transaction` object
        :raise :exc:`.TransactionError` if a new transaction cannot be created
        """

    def commit(self, tx):
        pass

    def rollback(self, tx):
        pass

    def check(self, tx):
        pass

    def run(self, tx, cypher, parameters=None):
        pass

    def pull(self, query, n=-1):
        pass

    def discard(self, query, n=-1):
        pass

    def send(self, query):
        pass

    def wait(self, query):
        pass


class Transaction(ItemizedTask):

    def __init__(self, db=None, readonly=False, bookmarks=None, metadata=None, timeout=None):
        super(Transaction, self).__init__()
        self.db = db
        self.readonly = readonly
        self.bookmarks = bookmarks
        self.metadata = metadata
        self.timeout = timeout

    @property
    def extra(self):
        extra = {}
        if self.db:
            extra["db"] = self.db
        if self.readonly:
            extra["mode"] = "r"
        # TODO: other extras
        return extra


class Query(object):

    def __init__(self):
        super(Query, self).__init__()

    def record_type(self):
        return tuple

    def records(self):
        raise NotImplementedError


class TransactionError(Exception):

    pass

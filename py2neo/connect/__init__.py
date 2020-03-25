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


from collections import deque
from logging import getLogger
from threading import Event
from uuid import uuid4

from py2neo.internal.compat import urlsplit, string_types, perf_counter
from py2neo.meta import (
    NEO4J_URI,
    NEO4J_AUTH,
    NEO4J_SECURE,
    NEO4J_VERIFY,
)
from py2neo.connect.addressing import Address


DEFAULT_SCHEME = "bolt"
DEFAULT_SECURE = False
DEFAULT_VERIFY = True
DEFAULT_USER = "neo4j"
DEFAULT_PASSWORD = "password"
DEFAULT_HOST = "localhost"
DEFAULT_BOLT_PORT = 7687
DEFAULT_HTTP_PORT = 7474
DEFAULT_HTTPS_PORT = 7473

DEFAULT_MAX_CONNECTIONS = 40


log = getLogger(__name__)


class ConnectionProfile(object):
    """ Connection details for a Neo4j service.
    """

    secure = None
    verify = None
    scheme = None
    user = None
    password = None
    address = None

    def __init__(self, uri=None, **settings):
        # TODO: recognise IPv6 addresses explicitly
        uri = self._coalesce(uri, NEO4J_URI)
        if uri is not None:
            parsed = urlsplit(uri)
            if parsed.scheme is not None:
                self.scheme = parsed.scheme
                if self.scheme in ["bolt+s", "bolt+ssc", "https", "http+s", "http+ssc"]:
                    self.secure = True
                elif self.scheme in ["bolt", "http"]:
                    self.secure = False
                if self.scheme in ["bolt+s", "https", "http+s"]:
                    self.verify = True
                elif self.scheme in ["bolt+ssc", "http+ssc"]:
                    self.verify = False
            self.user = self._coalesce(parsed.username, self.user)
            self.password = self._coalesce(parsed.password, self.password)
            self.address = Address.parse(parsed.netloc)
        else:
            self.address = Address.parse("")
        self._apply_auth(**settings)
        self._apply_components(**settings)
        self._apply_correct_scheme_for_security()
        self._apply_other_defaults()

    def _apply_auth(self, **settings):
        if "auth" in settings and settings["auth"] is not None:
            if isinstance(settings["auth"], string_types):
                self.user, _, self.password = settings["auth"].partition(":")
            else:
                self.user, self.password = settings["auth"]
        elif NEO4J_AUTH is not None:
            self.user, _, self.password = NEO4J_AUTH.partition(":")

    def _apply_components(self, **settings):
        self.secure = self._coalesce(settings.get("secure"), self.secure, NEO4J_SECURE)
        self.verify = self._coalesce(settings.get("verify"), self.verify, NEO4J_VERIFY)
        self.scheme = self._coalesce(settings.get("scheme"), self.scheme)
        self.user = self._coalesce(settings.get("user"), self.user)
        self.password = self._coalesce(settings.get("password"), self.password)
        if "address" in settings:
            self.address = Address.parse(settings.get("address"))
        if "host" in settings and "port" in settings:
            self.address = Address.parse("%s:%s" % (settings.get("host"), settings.get("port")))
        elif "host" in settings:
            self.address = Address.parse("%s:%s" % (settings.get("host"), self.port))
        elif "port" in settings:
            self.address = Address.parse("%s:%s" % (self.host, settings.get("port")))

    def _apply_correct_scheme_for_security(self):
        if self.secure is True and self.scheme == "http":
            self.scheme = "https"
        if self.secure is False and self.scheme in ("https", "http+s", "http+ssc"):
            self.scheme = "http"

    def _apply_other_defaults(self):
        if self.secure is None:
            self.secure = DEFAULT_SECURE
        if self.verify is None:
            self.verify = DEFAULT_VERIFY
        if not self.scheme:
            self.scheme = DEFAULT_SCHEME
            if self.scheme in ("bolt", "http"):
                self.secure = False
                self.verify = False
            if self.scheme in ("bolt+s", "https", "http+s"):
                self.secure = True
                self.verify = True
            if self.scheme in ("bolt+ssc", "http+ssc"):
                self.secure = True
                self.verify = False
        if not self.user:
            self.user = DEFAULT_USER
        if not self.password:
            self.password = DEFAULT_PASSWORD
        if not self.address.port:
            bits = list(self.address)
            if self.scheme == "http":
                bits[1] = DEFAULT_HTTP_PORT
            elif self.scheme in ("https", "http+s", "http+ssc"):
                bits[1] = DEFAULT_HTTPS_PORT
            else:
                bits[1] = DEFAULT_BOLT_PORT
            self.address = Address(bits)

    @property
    def auth(self):
        return self.user, self.password

    @property
    def host(self):
        return self.address.host

    @property
    def port(self):
        return self.address.port

    @property
    def port_number(self):
        return self.address.port_number

    @property
    def protocol(self):
        if self.scheme in ("neo4j", "bolt", "bolt+routing", "bolt+s",
                           "bolt+ssc", "neo4j+s", "neo4j+ssc"):
            return "bolt"
        elif self.scheme in ("http", "https", "http+s", "http+ssc"):
            return "http"
        else:
            return None

    @property
    def uri(self):
        return "%s://%s:%s" % (self.scheme, self.host, self.port)

    __hash_keys = ("secure", "verify", "scheme", "user", "password", "address")

    def __hash__(self):
        values = tuple(getattr(self, key) for key in self.__hash_keys)
        return hash(values)

    def __eq__(self, other):
        self_values = tuple(getattr(self, key) for key in self.__hash_keys)
        other_values = tuple(getattr(other, key) for key in self.__hash_keys)
        return self_values == other_values

    @staticmethod
    def _coalesce(*values):
        """ Utility function to return the first non-null value from a
        sequence of values.
        """
        for value in values:
            if value is not None:
                return value
        return None

    def to_dict(self):
        keys = ["secure", "verify", "scheme", "user", "password", "address",
                "auth", "host", "port", "port_number", "uri"]
        d = {}
        for key in keys:
            d[key] = getattr(self, key)
        return d


class Connection(object):
    """ A single point-to-point connection between a client and a
    server.

    :ivar profile: connection profile
    :ivar user_agent:
    """

    protocol_version = None

    server_agent = None

    connection_id = None

    pool = None

    # TODO: ping method

    @classmethod
    def open(cls, profile, user_agent=None):
        if profile.protocol == "bolt":
            from py2neo.connect.bolt import Bolt
            return Bolt.open(profile, user_agent=user_agent)
        elif profile.protocol == "http":
            from py2neo.connect.http import HTTP
            return HTTP.open(profile, user_agent=user_agent)
        else:
            raise ValueError("Unsupported scheme %r" % profile.scheme)

    def __init__(self, profile, user_agent):
        self.profile = profile
        self.user_agent = user_agent
        self.__t_opened = perf_counter()

    def __del__(self):
        if self.pool is not None:
            return
        try:
            self.close()
        except OSError:
            pass

    def close(self):
        pass

    @property
    def closed(self):
        raise NotImplementedError

    @property
    def broken(self):
        raise NotImplementedError

    @property
    def local_port(self):
        raise NotImplementedError

    @property
    def age(self):
        """ The age of this connection in seconds.
        """
        return perf_counter() - self.__t_opened

    def _hello(self):
        pass

    def _goodbye(self):
        pass

    def reset(self, force=False):
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

    def run_in_tx(self, tx, cypher, parameters=None):
        pass

    def pull(self, result, n=-1):
        pass

    def discard(self, result, n=-1):
        pass

    def sync(self, result):
        """ Perform network synchronisation required to make available
        a given result.
        """

    def fetch(self, result):
        pass

    @classmethod
    def default_hydrant(cls, profile, graph):
        if profile.protocol == "bolt":
            from py2neo.connect.bolt import Bolt
            return Bolt.default_hydrant(profile, graph)
        elif profile.protocol == "http":
            from py2neo.connect.http import HTTP
            return HTTP.default_hydrant(profile, graph)
        else:
            raise ValueError("Unsupported scheme %r" % profile.scheme)

    def release(self):
        if self.pool is not None:
            self.pool.release(self)


class Connector(object):
    """ A connection pool for a remote Neo4j service.

    :param profile: a :class:`.ConnectionProfile` describing how to
        connect to the remote service for which this pool operates
    :param max_size: the maximum permitted number of simultaneous
        connections that may be owned by this pool, both in-use and
        free
    :param max_age: the maximum permitted age, in seconds, for
        connections to be retained in this pool
    """

    default_init_size = 1

    default_max_size = 100

    default_max_age = 3600

    @classmethod
    def open(cls, profile=None, user_agent=None, init_size=None, max_size=None, max_age=None):
        """ Create a new connection pool, with an option to seed one
        or more initial connections.
        """
        pool = cls(profile, user_agent, max_size, max_age)
        seeds = [pool.acquire() for _ in range(init_size or cls.default_init_size)]
        for seed in seeds:
            pool.release(seed)
        return pool

    def __init__(self, profile, user_agent, max_size, max_age):
        self._profile = profile
        self._user_agent = user_agent
        self._max_size = max_size or self.default_max_size
        self._max_age = max_age or self.default_max_age
        self._in_use_list = deque()
        self._quarantine = deque()
        self._free_list = deque()
        self._waiting_list = WaitingList()
        self._transactions = {}

    def __del__(self):
        try:
            self.close()
        except OSError:
            pass

    def __repr__(self):
        return "<{} profile={!r} [{}{}{}]>".format(
            self.__class__.__name__,
            self.profile,
            "|" * len(self._in_use_list),
            "." * len(self._free_list),
            " " * (self.max_size - self.size),
        )

    def __hash__(self):
        return hash(self._profile)

    @property
    def profile(self):
        """ The connection profile for which this pool operates.
        """
        return self._profile

    @property
    def server_agent(self):
        cx = self.acquire()
        try:
            return cx.server_agent
        finally:
            self.release(cx)

    @property
    def user_agent(self):
        """ The user agent for connections in this pool.
        """
        return self._user_agent

    @property
    def max_size(self):
        """ The maximum permitted number of simultaneous connections
        that may be owned by this pool, both in-use and free.
        """
        return self._max_size

    @max_size.setter
    def max_size(self, value):
        old_value = self._max_size
        self._max_size = value
        if value > old_value:
            # The maximum size has grown, so new slots have become
            # available. Notify any waiting acquirers of this extra
            # capacity.
            self._waiting_list.notify()

    @property
    def max_age(self):
        """ The maximum permitted age, in seconds, for connections to
        be retained in this pool.
        """
        return self._max_age

    @property
    def in_use(self):
        """ The number of connections in this pool that are currently
        in use.
        """
        return len(self._in_use_list)

    @property
    def size(self):
        """ The total number of connections (both in-use and free)
        currently owned by this connection pool.
        """
        return len(self._in_use_list) + len(self._free_list)

    def _sanitize(self, cx, force_reset=False):
        """ Attempt to clean up a connection, such that it can be
        reused.

        If the connection is broken or closed, it can be discarded.
        Otherwise, the age of the connection is checked against the
        maximum age permitted by this pool, consequently closing it
        on expiry.

        Should the connection be neither broken, closed nor expired,
        it will be reset (optionally forcibly so) and the connection
        object will be returned, indicating success.
        """
        if cx.broken or cx.closed:
            return None
        expired = self.max_age is not None and cx.age > self.max_age
        if expired:
            cx.close()
            return None
        self._quarantine.append(cx)
        cx.reset(force=force_reset)
        self._quarantine.remove(cx)
        return cx

    def acquire(self, force_reset=False):
        """ Acquire a connection from the pool.

        In the simplest case, this will return an existing open
        connection, if one is free. If not, and the pool is not full,
        a new connection will be created. If the pool is full and no
        free connections are available, this will block until a
        connection is released, or until the acquire call is cancelled.

        This method will return :py:`None` if and only if the maximum
        size of the pool is set to zero. In this special case, no
        amount of waiting would result in the acquisition of a
        connection. This will be the case if the pool has been closed.

        :param force_reset: if true, the connection will be forcibly
            reset before being returned; if false, this will only occur
            if the connection is not already in a clean state
        :return: a Bolt connection object
        """
        log.debug("Acquiring connection from pool %r", self)
        cx = None
        while cx is None or cx.broken or cx.closed:
            if self.max_size == 0:
                return None
            try:
                # Plan A: select a free connection from the pool
                cx = self._free_list.popleft()
            except IndexError:
                if self.size < self.max_size:
                    # Plan B: if the pool isn't full, open
                    # a new connection
                    cx = Connection.open(self.profile, user_agent=self.user_agent)
                    cx.pool = self
                else:
                    # Plan C: wait for more capacity to become
                    # available, then try again
                    log.debug("Joining waiting list")
                    if not self._waiting_list.wait():
                        raise RuntimeError("Unable to acquire connection")
            else:
                cx = self._sanitize(cx, force_reset=force_reset)
        self._in_use_list.append(cx)
        return cx

    def release(self, cx, force_reset=False):
        """ Release a Bolt connection, putting it back into the pool
        if the connection is healthy and the pool is not already at
        capacity.

        :param cx: the connection to release
        :param force_reset: if true, the connection will be forcibly
            reset before being released back into the pool; if false,
            this will only occur if the connection is not already in a
            clean state
        :raise ValueError: if the connection is not currently in use,
            or if it does not belong to this pool
        """
        log.debug("Releasing connection %r", cx)
        if cx in self._in_use_list:
            self._in_use_list.remove(cx)
            if self.size < self.max_size:
                # If there is spare capacity in the pool, attempt to
                # sanitize the connection and return it to the pool.
                cx = self._sanitize(cx, force_reset=force_reset)
                if cx:
                    # Carry on only if sanitation succeeded.
                    if self.size < self.max_size:
                        # Check again if there is still capacity.
                        self._free_list.append(cx)
                        self._waiting_list.notify()
                    else:
                        # Otherwise, close the connection.
                        cx.close()
            else:
                # If the pool is full, simply close the connection.
                cx.close()
        elif cx in self._free_list:
            raise ValueError("Connection is not in use")
        elif cx in self._quarantine:
            pass
        else:
            raise ValueError("Connection %r does not belong to this pool" % cx)

    def prune(self):
        """ Close all free connections.
        """
        self.__close(self._free_list)

    def close(self):
        """ Close all connections immediately.

        This does not permanently disable the connection pool. Instead,
        it sets the maximum pool size to zero before shutting down all
        open connections, including those in use.

        To reuse the pool, the maximum size will need to be set to a
        a value greater than zero before connections can once again be
        acquired.

        To close gracefully, allowing work in progress to continue
        until connections are released, use the following sequence
        instead:

            pool.max_size = 0
            pool.prune()

        This will force all future connection acquisitions to be
        rejected, and released connections will be closed instead
        of being returned to the pool.
        """
        self.max_size = 0
        self.prune()
        self.__close(self._in_use_list)
        self._waiting_list.notify()

    @classmethod
    def __close(cls, connections):
        """ Close all connections in the given list.
        """
        closers = deque()
        while True:
            try:
                cx = connections.popleft()
            except IndexError:
                break
            else:
                closers.append(cx.close)
        for closer in closers:
            closer()

    def reacquire(self, tx):
        """ Lookup and return the connection bound to this
        transaction. If this transaction is not bound, acquire
        a connection via the regular acquire method.
        """
        try:
            return self._transactions[tx]
        except KeyError:
            return self.acquire()

    def is_bound(self, tx):
        """ Return true if the given transaction is bound
        within this pool.
        """
        return tx in self._transactions

    def bind(self, tx, cx):
        """ Bind a transaction to a connection.
        """
        self._transactions[tx] = cx

    def unbind(self, tx):
        """ Unbind a transaction from a connection.
        """
        try:
            del self._transactions[tx]
        except KeyError:
            raise TransactionError("Transaction not bound")

    def begin(self):
        return self.acquire().begin()

    def commit(self, tx):
        self.reacquire(tx).commit(tx)

    def rollback(self, tx):
        self.reacquire(tx).rollback(tx)

    def run(self, cypher, parameters=None, tx=None, hydrant=None):
        cx = self.reacquire(tx)
        if hydrant:
            parameters = hydrant.dehydrate(parameters, version=cx.protocol_version)
        if tx is None:
            result = cx.auto_run(cypher, parameters)
        else:
            result = cx.run_in_tx(tx, cypher, parameters)
        cx.pull(result)
        cx.sync(result)
        return result


class WaitingList:

    def __init__(self):
        self._wait_list = deque()

    def wait(self):
        event = Event()
        self._wait_list.append(event)
        return event.wait(3)  # TODO: make this configurable

    def notify(self):
        try:
            event = self._wait_list.popleft()
        except IndexError:
            pass
        else:
            event.set()


class Transaction(object):

    def __init__(self, db, txid=None):
        self.db = db
        self.txid = txid or uuid4()

    def __hash__(self):
        return hash((self.db, self.txid))

    def __eq__(self, other):
        if isinstance(other, Transaction):
            return self.db == other.db and self.txid == other.txid
        else:
            return False


class Result(object):

    def __init__(self):
        super(Result, self).__init__()

    @property
    def protocol_version(self):
        return None

    def buffer(self):
        raise NotImplementedError

    def fields(self):
        raise NotImplementedError

    def summary(self):
        raise NotImplementedError

    def fetch(self):
        raise NotImplementedError

    def has_records(self):
        raise NotImplementedError

    def take_record(self):
        raise NotImplementedError


class TransactionError(Exception):

    pass


class Failure(Exception):

    def __init__(self, message, code):
        super(Failure, self).__init__(message)
        self.code = code
        _, self.classification, self.category, self.title = self.code.split(".")

    def __str__(self):
        return "[%s] %s" % (self.code, super(Failure, self).__str__())

    @property
    def message(self):
        return self.args[0]


class Hydrant(object):

    def hydrate(self, keys, values, entities=None, version=None):
        raise NotImplementedError

    def dehydrate(self, data, version=None):
        raise NotImplementedError

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
from hashlib import new as hashlib_new
from logging import getLogger
from threading import Event

from py2neo.internal.compat import bstr, urlsplit, string_types, perf_counter
from py2neo.meta import (
    NEO4J_URI,
    NEO4J_AUTH,
    NEO4J_USER_AGENT,
    NEO4J_SECURE,
    NEO4J_VERIFIED,
    bolt_user_agent,
    http_user_agent,
)
from py2neo.net.addressing import Address


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


log = getLogger(__name__)


class Service(object):
    """ Neo4j service descriptor.
    """

    secure = None
    verified = None
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
                if self.scheme in ["https"]:
                    self.secure = True
                elif self.scheme in ["http"]:
                    self.secure = False
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
        self.verified = self._coalesce(settings.get("verified"), self.verified, NEO4J_VERIFIED)
        self.scheme = self._coalesce(settings.get("scheme"), self.scheme)
        self.user = self._coalesce(settings.get("user"), self.user)
        self.password = self._coalesce(settings.get("password"), self.password)
        if "address" in settings:
            self.address = Address.parse(settings.get("address"))

    def _apply_correct_scheme_for_security(self):
        if self.secure is True and self.scheme == "http":
            self.scheme = "https"
        if self.secure is False and self.scheme == "https":
            self.scheme = "http"

    def _apply_other_defaults(self):
        if self.secure is None:
            self.secure = DEFAULT_SECURE
        if self.verified is None:
            self.verified = DEFAULT_VERIFIED
        if not self.scheme:
            self.scheme = DEFAULT_SCHEME
            if self.scheme == "http":
                self.secure = False
                self.verified = False
            if self.scheme == "https":
                self.secure = True
                self.verified = True
        if not self.user:
            self.user = DEFAULT_USER
        if not self.password:
            self.password = DEFAULT_PASSWORD
        if not self.address.port:
            bits = list(self.address)
            if self.scheme == "http":
                bits[1] = DEFAULT_HTTP_PORT
            elif self.scheme == "https":
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
    def uri(self):
        return "%s://%s:%s" % (self.scheme, self.host, self.port)

    def __hash__(self):
        keys = ["secure", "verified", "scheme", "user", "password", "address"]
        h = hashlib_new("md5")
        for key in keys:
            h.update(bstr(getattr(self, key)))
        return h.hexdigest()

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
        keys = ["secure", "verified", "scheme", "user", "password", "address",
                "auth", "host", "port", "port_number", "uri"]
        d = {}
        for key in keys:
            d[key] = getattr(self, key)
        return d


class Connection(object):
    """ A single point-to-point connection between a client and a
    server.

    :ivar service: service descriptor
    :ivar user_agent:
    """

    scheme = None

    protocol_version = ()

    server_agent = None

    connection_id = None

    pool = None

    __subclasses = None

    @classmethod
    def _walk_subclasses(cls):
        for subclass in cls.__subclasses__():
            assert issubclass(subclass, cls)  # for the benefit of the IDE
            yield subclass
            for k in subclass._walk_subclasses():
                yield k

    @classmethod
    def _get_subclass(cls, scheme, protocol_version):
        key = (scheme, protocol_version)
        if cls.__subclasses is None:
            cls.__subclasses = {}
            for subclass in cls._walk_subclasses():
                subclass_key = (subclass.scheme, subclass.protocol_version)
                cls.__subclasses[subclass_key] = subclass
        return cls.__subclasses.get(key)

    @classmethod
    def open(cls, service, user_agent=None):
        # TODO: automatically via subclass sniffing
        if service.scheme == "bolt":
            from py2neo.net.bolt import Bolt
            return Bolt.open(service, user_agent=user_agent)
        elif service.scheme == "http":
            from py2neo.net.http import HTTP
            return HTTP.open(service, user_agent=user_agent)
        elif service.scheme == "https":
            from py2neo.net.http import HTTPS
            return HTTPS.open(service, user_agent=user_agent)
        else:
            raise ValueError("Unsupported scheme %r" % service.scheme)

    def __init__(self, service, user_agent):
        self.service = service
        self.user_agent = user_agent
        self.__t_opened = perf_counter()

    def __del__(self):
        if self.pool:
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

    def hello(self):
        pass

    def goodbye(self):
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


# TODO: fix the docstring
class ConnectionPool(object):
    """ A pool of connections to a single address.

    :param opener: a function to which an address can be passed that
        returns an open and ready Bolt connection
    :param address: the remote address for which this pool operates
    :param max_size: the maximum permitted number of simultaneous
        connections that may be owned by this pool, both in-use and
        free
    :param max_age: the maximum permitted age, in seconds, for
        connections to be retained in this pool
    """

    @classmethod
    def open(cls, service=None, init_size=1, max_size=100, max_age=3600):
        """ Create a new connection pool, with an option to seed one
        or more initial connections.
        """
        pool = cls(service, max_size, max_age)
        seeds = [pool.acquire() for _ in range(init_size)]
        for seed in seeds:
            pool.release(seed)
        return pool

    def __init__(self, service, max_size, max_age):
        self._service = service
        self._max_size = max_size
        self._max_age = max_age
        self._in_use_list = deque()
        self._quarantine = deque()
        self._free_list = deque()
        self._waiting_list = WaitingList()

    def __del__(self):
        try:
            self.close()
        except OSError:
            pass

    def __repr__(self):
        return "<{} service={!r} [{}{}{}]>".format(
            self.__class__.__name__,
            self.service,
            "|" * len(self._in_use_list),
            "." * len(self._free_list),
            " " * (self.max_size - self.size),
        )

    def __contains__(self, cx):
        return cx in self._in_use_list or cx in self._free_list

    def __len__(self):
        return self.size

    @property
    def service(self):
        """ The remote service for which this pool operates.
        """
        return self._service

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
                    cx = Connection.open(self.service)
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


class WaitingList:

    def __init__(self):
        self._wait_list = deque()

    def wait(self):
        event = Event()
        self._wait_list.append(event)
        return event.wait(3)

    def notify(self):
        try:
            event = self._wait_list.popleft()
        except IndexError:
            pass
        else:
            event.set()

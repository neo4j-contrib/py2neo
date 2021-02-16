#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright 2011-2021, Nigel Small
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


from collections import deque, namedtuple, OrderedDict
from datetime import timedelta
from logging import getLogger
from uuid import UUID, uuid4

from monotonic import monotonic
from packaging.version import Version

from py2neo import ClientError
from py2neo.client.config import ConnectionProfile
from py2neo.compat import string_types
from py2neo.timing import repeater
from py2neo.wiring import Address


DEFAULT_MAX_CONNECTIONS = 40


log = getLogger(__name__)


ConnectionRecord = namedtuple("ConnectionRecord",
                              ["cxid", "since", "client_address",
                               "server_profile", "user_agent"])
TransactionRecord = namedtuple("TransactionRecord",
                               ["txid", "since", "cxid", "client_address",
                                "server_profile", "metadata", "database",
                                "current_qid", "current_query", "status",
                                "stats"])
QueryRecord = namedtuple("QueryRecord",
                         ["qid", "since", "cxid", "client_address",
                          "server_profile", "metadata", "database",
                          "query", "parameters", "planner", "runtime",
                          "indexes", "status", "stats"])


def _millis_to_timedelta(t):
    if t is None:
        return None
    else:
        return timedelta(milliseconds=t)


class Bookmark(object):

    def __init__(self, *values):
        value_list = []

        def add_values(v):
            for value in v:
                if not value:
                    continue
                elif isinstance(value, Bookmark):
                    value_list.extend(value.__values)
                elif isinstance(value, tuple):
                    add_values(value)
                elif isinstance(value, string_types):
                    value_list.append(value)
                else:
                    raise TypeError("Unusable bookmark value {!r}".format(value))

        add_values(values)
        self.__values = frozenset(value_list)

    def __hash__(self):
        return hash(self.__values)

    def __eq__(self, other):
        if isinstance(other, Bookmark):
            return self.__values == other.__values
        else:
            return False

    def __iter__(self):
        return iter(self.__values)

    def __repr__(self):
        return "<Bookmark %s>" % " ".join(map(repr, self.__values))


class Connection(object):
    """ A single point-to-point connection between a client and a
    server.

    This base class is extended by both :class:`.Bolt` and
    :class:`.HTTP` implementations and contains interfaces for the
    basic operations provided by both.

    :ivar Connection.profile: connection profile
    :ivar Connection.user_agent:
    """

    protocol_version = None

    server_agent = None

    connection_id = None

    # TODO: ping method

    @classmethod
    def open(cls, profile=None, user_agent=None, on_bind=None, on_unbind=None,
             on_release=None, on_broken=None):
        """ Open a connection to a server.

        :param profile: :class:`.ConnectionProfile` detailing how and
            where to connect
        :param user_agent:
        :param on_bind:
        :param on_unbind:
        :param on_release:
        :param on_broken:
        :returns: :class:`.Bolt` connection object
        :raises: :class:`.ConnectionUnavailable` if a connection cannot
            be opened
        :raises: ValueError if the profile references an unsupported
            scheme
        """
        if profile is None:
            profile = ConnectionProfile()  # default connection profile
        if profile.protocol == "bolt":
            from py2neo.client.bolt import Bolt
            return Bolt.open(profile, user_agent=user_agent,
                             on_bind=on_bind, on_unbind=on_unbind,
                             on_release=on_release, on_broken=on_broken)
        elif profile.protocol == "http":
            from py2neo.client.http import HTTP
            return HTTP.open(profile, user_agent=user_agent,
                             on_bind=on_bind, on_unbind=on_unbind,
                             on_release=on_release, on_broken=on_broken)
        else:
            raise ValueError("Unknown scheme %r" % profile.scheme)

    def __init__(self, profile, user_agent, on_bind=None, on_unbind=None, on_release=None):
        self.profile = profile
        self.user_agent = user_agent
        self._neo4j_version = None
        self._neo4j_edition = None
        self._on_bind = on_bind
        self._on_unbind = on_unbind
        self._on_release = on_release
        self.__t_opened = monotonic()

    def close(self):
        pass

    @property
    def closed(self):
        """ True if the connection has been closed by the client.
        """
        raise NotImplementedError

    @property
    def broken(self):
        """ True if the connection has been broken by the server or
        network.
        """
        raise NotImplementedError

    @property
    def local_port(self):
        raise NotImplementedError

    @property
    def bytes_sent(self):
        raise NotImplementedError

    @property
    def bytes_received(self):
        raise NotImplementedError

    @property
    def age(self):
        """ The age of this connection in seconds.
        """
        from monotonic import monotonic
        return monotonic() - self.__t_opened

    @property
    def neo4j_version(self):
        """ The version of Neo4j to which this connection is
        established.
        """
        if self._neo4j_version is None:
            self._get_version_and_edition()
        return self._neo4j_version

    @property
    def neo4j_edition(self):
        """ The edition of Neo4j to which this connection is
        established.
        """
        if self._neo4j_edition is None:
            self._get_version_and_edition()
        return self._neo4j_edition

    def _system_call(self, tx, procedure):
        cypher = "CALL " + procedure
        try:
            if tx is None:
                result = self.auto_run(None, cypher)
            else:
                result = self.run_in_tx(tx, cypher)
            self.pull(result)
            result.buffer()
        except ClientError as error:
            if error.title == "ProcedureNotFound":
                raise TypeError("This Neo4j installation does not support the "
                                "procedure %r" % procedure)
            else:
                raise
        else:
            fields = result.fields()
            for record in result.records():
                yield OrderedDict(zip(fields, record))

    def _get_version_and_edition(self):
        for record in self._system_call(None, "dbms.components"):
            if record["name"] == "Neo4j Kernel":
                self._neo4j_version = Version(record["versions"][0])
                self._neo4j_edition = record["edition"]

    def _hello(self):
        pass

    def _goodbye(self):
        pass

    def reset(self, force=False):
        pass

    def auto_run(self, graph_name, cypher, parameters=None, readonly=False,
                 # after=None, metadata=None, timeout=None
                 ):
        """ Run a single query within an auto-commit transaction. This
        method may invoke network activity

        :param graph_name:
        :param cypher:
        :param parameters:
        :param readonly:
        :returns:
        """

    def begin(self, graph_name, readonly=False,
              # after=None, metadata=None, timeout=None
              ):
        """ Begin a transaction. This method may invoke network
        activity.

        :param graph_name:
        :param readonly:
        :returns: new :class:`.Transaction` object
        :raises TransactionError: if a new transaction cannot be created
        """

    def commit(self, tx):
        """ Commit a transaction. This method will always invoke
        network activity.

        :param tx: the transaction to commit
        :returns: bookmark
        :raises ValueError: if the supplied :class:`.Transaction`
            object is not valid for committing
        :raises BrokenTransactionError: if the transaction cannot be
            committed
        """

    def rollback(self, tx):
        """ Rollback a transaction. This method will always invoke
        network activity.

        :param tx: the transaction to rollback
        :returns: bookmark
        :raises ValueError: if the supplied :class:`.Transaction`
            object is not valid for rolling back
        :raises BrokenTransactionError: if the transaction cannot be
            rolled back
        """

    def run_in_tx(self, tx, cypher, parameters=None):
        pass  # may have network activity

    def pull(self, result, n=-1):
        pass

    def discard(self, result, n=-1):
        pass

    def route(self, graph_name=None, context=None):
        """ Fetch the routing table for a given database.

        :param graph_name: the name of the graph database for which to
            retrieve a routing table; `None` references the default
            database
        :param context: an optional dictionary of routing context
            information
        :returns: 4-tuple of router, reader, writer connection
            profiles, plus ttl
        :raises TypeError: if routing is not supported
        """
        raise TypeError("Routing not supported "
                        "for {} connections".format(self.__class__.__name__))

    def sync(self, result):
        """ Perform network synchronisation required to make available
        a given result.
        """

    def fetch(self, result):
        pass

    @classmethod
    def default_hydrant(cls, profile, graph):
        if profile.protocol == "bolt":
            from py2neo.client.bolt import Bolt
            return Bolt.default_hydrant(profile, graph)
        elif profile.protocol == "http":
            from py2neo.client.http import HTTP
            return HTTP.default_hydrant(profile, graph)
        else:
            raise ValueError("Unknown scheme %r" % profile.scheme)

    def release(self):
        """ Signal that this connection is no longer in use.
        """
        if callable(self._on_release):
            self._on_release(self)

    def supports_multi(self):
        """ Detect whether or not this connection supports
        multi-database.
        """

    def get_cluster_overview(self, tx=None):
        """ Fetch an overview of the cluster of which the server is a
        member. If the server is not part of a cluster, a
        :exc:`TypeError` is raised.

        :param tx: transaction in which this call should be carried
            out, if any
        :returns: dictionary of cluster membership, keyed by unique
            server ID
        :raises: :exc:`TypeError` if the server is not a member of a
            cluster
        """
        overview = {}
        for record in self._system_call(tx, "dbms.cluster.overview"):
            addresses = {}
            for address in record["addresses"]:
                profile = ConnectionProfile(address)
                addresses[profile.scheme] = profile.address
            overview[UUID(record["id"])] = {
                "addresses": addresses,
                "databases": record["databases"],
                "groups": record["groups"],
            }
        return overview

    def get_config(self, tx=None):
        """ Fetch a dictionary of configuration for the server to which
        this connection is established.

        :param tx: transaction in which this call should be carried
            out, if any
        :returns: dictionary of name-value pairs for each setting
        """
        return {record["name"]: record["value"]
                for record in self._system_call(tx, "dbms.listConfig")}

    def get_connections(self, tx=None):
        """ Fetch a list of connections to the server to which this
        connection is established.

        This method calls the dbms.listConnections procedure, which is
        available with the following versions of Neo4j:

        - Community Edition - version 4.2 and above
        - Enterprise Edition - all versions

        :returns: list of :class:`.ConnectionRecord` objects
        :raises TypeError: if the dbms.listConnections procedure is not
            supported by the underlying Neo4j installation
        """
        from neotime import DateTime
        records = []
        for record in self._system_call(tx, "dbms.listConnections"):
            server_profile = ConnectionProfile(scheme=record["connector"],
                                               address=record["serverAddress"],
                                               user=record["username"])
            connect_time = DateTime.from_iso_format(record["connectTime"].
                                                    replace("Z", "+00:00")).to_native()
            records.append(ConnectionRecord(cxid=record["connectionId"],
                                            since=connect_time,
                                            client_address=Address.parse(record["clientAddress"]),
                                            server_profile=server_profile,
                                            user_agent=record["userAgent"]))
        return records

    def get_transactions(self, tx=None):
        """ Fetch a list of transactions currently active on the server
        to which this connection is established.

        This method calls the dbms.listTransactions procedure, which is
        available with the following versions of Neo4j:

        - Community Edition - version 4.2 and above
        - Enterprise Edition - all versions

        :returns: list of :class:`.TransactionRecord` objects
        :raises TypeError: if the dbms.listTransactions procedure is not
            supported by the underlying Neo4j installation
        """
        from neotime import DateTime
        records = []
        for record in self._system_call(tx, "dbms.listTransactions"):
            server_profile = ConnectionProfile(scheme=record["protocol"],
                                               address=record["requestUri"],
                                               user=record["username"])
            start_time = DateTime.from_iso_format(record["startTime"].
                                                  replace("Z", "+00:00")).to_native()
            records.append(TransactionRecord(txid=record["transactionId"],
                                             since=start_time,
                                             cxid=record.get("connectionId"),
                                             client_address=Address.parse(record["clientAddress"]),
                                             server_profile=server_profile,
                                             metadata=record["metaData"],
                                             database=record.get("database"),
                                             current_qid=record["currentQueryId"],
                                             current_query=record["currentQuery"],
                                             status=record["status"],
                                             stats={
                                                 "active_lock_count": record["activeLockCount"],
                                                 "elapsed_time": _millis_to_timedelta(record["elapsedTimeMillis"]),
                                                 "cpu_time": _millis_to_timedelta(record["cpuTimeMillis"]),
                                                 "wait_time": _millis_to_timedelta(record["waitTimeMillis"]),
                                                 "idle_time": _millis_to_timedelta(record["idleTimeMillis"]),
                                                 "page_hits": record["pageHits"],
                                                 "page_faults": record["pageFaults"],
                                                 "allocated_bytes": record["allocatedBytes"],
                                                 "allocated_direct_bytes": record["allocatedDirectBytes"],
                                                 "estimated_used_heap_memory": record.get("estimatedUsedHeapMemory"),
                                             }))
        return records

    def get_queries(self, tx=None):
        """ Fetch a list of queries currently active on the server
        to which this connection is established.

        This method calls the dbms.listQueries procedure, which is
        available with the following versions of Neo4j:

        - Community Edition - version 4.2 and above
        - Enterprise Edition - all versions

        :returns: list of :class:`.QueryRecord` objects
        :raises TypeError: if the dbms.listQueries procedure is not
            supported by the underlying Neo4j installation
        """
        from neotime import DateTime
        records = []
        for record in self._system_call(tx, "dbms.listQueries"):
            server_profile = ConnectionProfile(scheme=record["protocol"],
                                               address=record["requestUri"],
                                               user=record["username"])
            start_time = DateTime.from_iso_format(record["startTime"].
                                                  replace("Z", "+00:00")).to_native()
            records.append(QueryRecord(qid=record["queryId"],
                                       since=start_time,
                                       cxid=record.get("connectionId"),
                                       client_address=Address.parse(record["clientAddress"]),
                                       server_profile=server_profile,
                                       metadata=record["metaData"],
                                       database=record.get("database"),
                                       query=record["query"],
                                       parameters=record["parameters"],
                                       planner=record["planner"],
                                       runtime=record["runtime"],
                                       indexes=record["indexes"],
                                       status=record["status"],
                                       stats={
                                           "active_lock_count": record["activeLockCount"],
                                           "elapsed_time": _millis_to_timedelta(record["elapsedTimeMillis"]),
                                           "cpu_time": _millis_to_timedelta(record["cpuTimeMillis"]),
                                           "wait_time": _millis_to_timedelta(record["waitTimeMillis"]),
                                           "idle_time": _millis_to_timedelta(record["idleTimeMillis"]),
                                           "page_hits": record["pageHits"],
                                           "page_faults": record["pageFaults"],
                                           "allocated_bytes": record["allocatedBytes"],
                                       }))
        return records

    def get_management_data(self, tx=None):
        """ Fetch a dictionary of management data for the server to
        which this connection is established. This method calls the
        dbms.queryJmx procedure.

        The structure and format of the data returned may vary
        depending on the underlying version and edition of Neo4j.
        """
        data = {}
        for record in self._system_call(tx, "dbms.queryJmx('*:*') YIELD name, attributes"):
            name = record["name"]
            attributes = record["attributes"]
            data[name] = {key: value["value"]
                          for key, value in attributes.items()
                          if key not in ("ObjectName",)}
        return data


class ConnectionPool(object):
    """ A pool of connections targeting a single Neo4j server.
    """

    default_init_size = 1

    default_max_size = 100

    default_max_age = 3600

    @classmethod
    def open(cls, profile=None, user_agent=None, init_size=None, max_size=None, max_age=None,
             on_bind=None, on_unbind=None, on_broken=None):
        """ Create a new connection pool, with an option to seed one
        or more initial connections.

        :param profile: a :class:`.ConnectionProfile` describing how to
            connect to the remote service for which this pool operates
        :param user_agent: a user agent string identifying the client
            software
        :param init_size: the number of seed connections to open
        :param max_size: the maximum permitted number of simultaneous
            connections that may be owned by this pool, both in-use and
            free
        :param max_age: the maximum permitted age, in seconds, for
            connections to be retained in this pool
        :param on_bind: callback to execute when binding a transaction
            to a connection; this must accept two arguments
            representing the transaction and the connection
        :param on_unbind: callback to execute when unbinding a
            transaction from a connection; this must accept an argument
            representing the transaction
        :param on_broken: callback to execute when a connection in the
            pool is broken; this must accept an argument representing
            the connection profile and a second with an error message
        :raises: :class:`.ConnectionUnavailable` if connections cannot
            be successfully made to seed the pool
        :raises: ValueError if the profile references an unsupported
            scheme
        """
        pool = cls(profile, user_agent, max_size, max_age, on_bind, on_unbind, on_broken)
        seeds = [pool.acquire() for _ in range(init_size or cls.default_init_size)]
        for seed in seeds:
            seed.release()
        return pool

    def __init__(self, profile, user_agent=None, max_size=None, max_age=None,
                 on_bind=None, on_unbind=None, on_broken=None):
        self._profile = profile or ConnectionProfile()
        self._user_agent = user_agent
        self._max_size = max_size or self.default_max_size
        self._max_age = max_age or self.default_max_age
        self._on_bind = on_bind
        self._on_unbind = on_unbind
        self._on_broken = on_broken
        self._in_use_list = deque()
        self._quarantine = deque()
        self._free_list = deque()
        self._supports_multi = False
        # stats
        self._opened_list = deque()
        self._bytes_sent = 0
        self._bytes_received = 0

    def __repr__(self):
        return "<{} profile={!r} in_use={!r} free={!r} spare={!r}>".format(
            self.__class__.__name__,
            self.profile,
            len(self._in_use_list),
            len(self._free_list),
            (self.max_size - self.size),
        )

    def __hash__(self):
        return hash(self._profile)

    @property
    def profile(self):
        """ The connection profile for which this pool operates.
        """
        return self._profile

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
            pass  # Removed waiting list mechanism (11 Nov 2020)

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

    @property
    def bytes_sent(self):
        b = self._bytes_sent
        for cx in list(self._opened_list):
            b += cx.bytes_sent
        return b

    @property
    def bytes_received(self):
        b = self._bytes_received
        for cx in list(self._opened_list):
            b += cx.bytes_received
        return b

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

    def connect(self):
        """ Open a new connection, adding it to a list of opened
        connections in order to collect statistics. This method also
        collects stats from closed or broken connections into a
        pool-wide running total.
        """
        for _ in range(len(self._opened_list)):
            cx = self._opened_list.popleft()
            if cx.closed or cx.broken:
                self._bytes_sent += cx.bytes_sent
                self._bytes_received += cx.bytes_received
            else:
                self._opened_list.append(cx)
        cx = Connection.open(self.profile, user_agent=self.user_agent,
                             on_bind=self._on_bind, on_unbind=self._on_unbind,
                             on_release=lambda c: self.release(c),
                             on_broken=lambda msg: self.__on_broken(msg))
        self._opened_list.append(cx)
        return cx

    def acquire(self, force_reset=False):
        """ Acquire a connection from the pool.

        In the simplest case, this will return an existing open
        connection, if one is free. If not, and the pool is not full,
        a new connection will be created. If the pool is full and no
        free connections are available, this will block until a
        connection is released, or until the acquire call is cancelled.

        This method will raise :exc:`.ConnectionUnavailable` if the
        maximum size of the pool is set to zero, if the pool is full
        and all connections are in use, or if a new connection attempt
        is made which fails.

        :param force_reset: if true, the connection will be forcibly
            reset before being returned; if false, this will only occur
            if the connection is not already in a clean state
        :returns: a Bolt connection object
        :raises: :class:`.ConnectionUnavailable` if no connection can
            be acquired
        """
        log.debug("Trying to acquire connection from pool %r", self)
        cx = None
        while cx is None or cx.broken or cx.closed:
            if self.max_size == 0:
                log.debug("Pool %r is set to zero size", self)
                raise ConnectionUnavailable("Pool is set to zero size")
            try:
                # Plan A: select a free connection from the pool
                cx = self._free_list.popleft()
            except IndexError:
                if self.size < self.max_size:
                    # Plan B: if the pool isn't full, open
                    # a new connection. This may raise a
                    # ConnectionUnavailable exception, which
                    # should bubble up to the caller.
                    cx = self.connect()
                    if cx.supports_multi():
                        self._supports_multi = True
                else:
                    # Plan C: the pool is full and all connections
                    # are in use. Return immediately to allow the
                    # caller to make an alternative choice.
                    log.debug("Pool %r is full with all connections "
                              "in use", self)
                    raise ConnectionUnavailable("Pool is full")
            else:
                cx = self._sanitize(cx, force_reset=force_reset)
        log.debug("Acquired connection %r", cx)
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
        :raise ValueError: if the connection does not belong to this
            pool
        """
        if cx in self._free_list or cx in self._quarantine:
            return
        log.debug("Releasing connection %r", cx)
        if cx not in self._in_use_list:
            # Connection does not belong to this pool
            log.debug("Connection %r does not belong to pool %r", cx, self)
            return
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
                    pass  # Removed waiting list mechanism (11 Nov 2020)
                else:
                    # Otherwise, close the connection.
                    cx.close()
        else:
            # If the pool is full, simply close the connection.
            cx.close()

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

    def supports_multi(self):
        return self._supports_multi

    def __on_broken(self, message):
        if callable(self._on_broken):
            self._on_broken(self._profile, message)


class Connector(object):
    """ A connection pool abstraction that uses an appropriate
    connection pool implementation and is coupled with a transaction
    manager.

    :param profile: a :class:`.ConnectionProfile` describing how to
        connect to the remote graph database service
    :param user_agent: a user agent string identifying the client
        software
    :param init_size: the number of seed connections to open in the
        initial pool
    :param max_size: the maximum permitted number of simultaneous
        connections that may be owned by pools held by this
        connector, both in-use and free
    :param max_age: the maximum permitted age, in seconds, for
        connections to be retained within pools held by this
        connector
    :param routing: flag to switch on client-side routing across a
        cluster
    """

    def __init__(self, profile, user_agent=None, init_size=None, max_size=None, max_age=None,
                 routing=False):
        self._profile = ConnectionProfile(profile)
        self._user_agent = user_agent
        self._init_size = init_size
        self._max_size = max_size
        self._max_age = max_age
        self._transactions = {}
        self._pools = {}
        if routing:
            self._routers = []
            self._routing_tables = {}
        else:
            self._routers = None
            self._routing_tables = None
        self.add_pools(self._profile)
        if routing:
            self._refresh_routing_table(None)

    def __repr__(self):
        return "<{} to {!r}>".format(self.__class__.__name__, self.profile)

    def __hash__(self):
        return hash(self.profile)

    def add_pools(self, *profiles):
        """ Adds connection pools for one or more connection profiles.
        Pools that already exist will be skipped.
        """
        for profile in profiles:
            if profile in self._pools:
                # This profile already has a pool,
                # no need to add it again
                continue
            log.debug("Adding connection pool for profile %r", profile)
            pool = ConnectionPool.open(
                profile,
                user_agent=self._user_agent,
                init_size=self._init_size,
                max_size=self._max_size,
                max_age=self._max_age,
                on_bind=self._on_bind,
                on_unbind=self._on_unbind,
                on_broken=self._on_broken)
            self._pools[profile] = pool

    @classmethod
    def _repr_graph_name(cls, graph_name):
        # helper for logging
        if graph_name is None:
            return "default database"
        else:
            return repr(graph_name)

    def get_pools(self, graph_name=None, readonly=False):
        """ Obtain a list of connection pools for a particular
        graph database and read/write mode.

        If, for any reason, the routing table is not valid, a
        :exc:`.RoutingTable.Invalid` exception will be raised.
        Possible reasons are:
        - No routing table exists for the given graph database
        - The routing table has expired
        - No appropriate readonly or read-write servers are listed
        """
        if self._routers is None:
            # If routing isn't enabled, just return a
            # simple list of pools.
            return list(self._pools.values())
        try:
            rt = self._routing_tables[graph_name]
        except KeyError:
            log.debug("No routing table available for %s", self._repr_graph_name(graph_name))
            raise RoutingTable.Invalid()
        else:
            if rt.expired():
                log.debug("Routing table for %s expired at %r", self._repr_graph_name(graph_name),
                          rt.expiry_time)
                raise RoutingTable.Invalid()
            profiles = rt.runners(readonly=readonly)
            if not profiles:
                log.debug("No server profiles for %r "
                          "and readonly=%r", self._repr_graph_name(graph_name), readonly)
                raise RoutingTable.Invalid()
            return [pool for profile, pool in self._pools.items()
                    if profile in profiles]

    def _refresh_routing_table(self, graph_name=None):
        log.debug("Attempting to refresh routing table for %s",
                  "default database" if graph_name is None else repr(graph_name))
        assert self._routers is not None
        for router in self._routers + [self.profile]:
            # TODO: don't open a new connection every time, use the pool if free
            try:
                cx = self._pools[router].connect()
            except ConnectionUnavailable:
                continue  # try the next router instead
            else:
                try:
                    routers, ro_runners, rw_runners, ttl = cx.route(graph_name)
                except TransactionError as error:
                    log.warning(error.args[0])
                    continue
                else:
                    # TODO: comment this algorithm
                    self.add_pools(*ro_runners)
                    self.add_pools(*rw_runners)
                    routing_table = RoutingTable(ro_runners, rw_runners, monotonic() + ttl)
                    # TODO: housekeep old pools (maybe in connection pool after connection failure)
                    old = set(profile for profile in self._routers if profile not in routers)
                    if graph_name in self._routing_tables:
                        rt = self._routing_tables[graph_name]
                        old.update(profile for profile in rt if profile not in routing_table)

                    self._routers[:] = routers
                    self._routing_tables[graph_name] = routing_table
                    for profile in old:
                        self._pools[profile].prune()
                    return
                finally:
                    cx.close()
        raise ConnectionUnavailable("No suitable routers found")  # TODO: better exception

    @property
    def profile(self):
        """ The initial connection profile for this connector.
        """
        return self._profile

    @property
    def user_agent(self):
        """ The user agent for connections attached to this connector.
        """
        return self._user_agent

    @property
    def in_use(self):
        """ A dictionary mapping each profile to the number of
        connections currently pooled for that profile that are
        currently in use.
        """
        return {profile: pool.in_use
                for profile, pool in self._pools.items()}

    @property
    def bytes_sent(self):
        b = 0
        for pool in list(self._pools.values()):
            b += pool.bytes_sent
        return b

    @property
    def bytes_received(self):
        b = 0
        for pool in list(self._pools.values()):
            b += pool.bytes_received
        return b

    def acquire(self, graph_name=None, readonly=False, timeout=None, force_reset=False):
        """ Acquire a connection from a pool owned by this connector.

        In the simplest case, this will return an existing open
        connection, if one is free. If not, and the pool is not full,
        a new connection will be created. If the pool is full and no
        free connections are available, this will block until a
        connection is released, or until the acquire call is cancelled.

        This method will return :const:`None` if and only if the
        maximum size of the pool is set to zero. In this special case,
        no amount of waiting would result in the acquisition of a
        connection. This will be the case if the pool has been closed.

        :param graph_name: the graph database name for which a
            connection must be acquired
        :param readonly: if true, a readonly server will be selected,
            if available; if no such servers are available, a regular
            server will be used instead
        :param timeout: for how long (in seconds) to continue to
            attempt to acquire a connection
        :param force_reset: if true, the connection will be forcibly
            reset before being returned; if false, this will only occur
            if the connection is not already in a clean state
        :return: a :class:`.Connection` object
        :raises: :class:`.ConnectionUnavailable` if a connection
            could not be acquired within the time limit
        """
        # TODO: improve logging for this method
        for n in repeater(at_least=3, timeout=timeout):
            log.debug("Attempting to acquire connection to %s", self._repr_graph_name(graph_name))
            try:
                pools = self.get_pools(graph_name, readonly=readonly)
            except RoutingTable.Invalid:
                self._refresh_routing_table(graph_name)
            else:
                for pool in sorted(pools, key=lambda p: p.in_use):
                    log.debug("Using connection pool %r", pool)
                    try:
                        cx = pool.acquire(force_reset=force_reset)
                    except ConnectionUnavailable as error:
                        log.debug("Connection unavailable; %r", error.args[0])
                        continue
                    else:
                        if cx is not None:
                            return cx
        else:
            raise ConnectionUnavailable("Timed out trying to acquire connection")

    def release(self, cx, force_reset=False):
        """ Release a connection back into the pool.
        """
        for pool in self._pools:
            try:
                pool.release(cx, force_reset=force_reset)
            except ValueError:
                continue
            else:
                break

    def prune(self, profile):
        """ Close all free connections for the given profile.
        """
        try:
            pool = self._pools[profile]
        except KeyError:
            pass
        else:
            pool.prune()

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
        for pool in self._pools.values():
            pool.close()

    def _get_connection(self, tx):
        """ Lookup and return the connection bound to this
        transaction, if any, otherwise acquire a new connection.

        :param tx: a bound transaction
        :raise TypeError: if the given transaction is invalid or not bound
        """
        try:
            return self._transactions[tx]
        except KeyError:
            return self.acquire(tx.graph_name, tx.readonly)

    def _on_bind(self, tx, cx):
        """ Bind a transaction to a connection.

        :param tx: an unbound transaction
        :param tx: an connection to which to bind the transaction
        :raise TypeError: if the given transaction is already bound
        """
        try:
            cx0 = self._transactions[tx]
        except KeyError:
            self._transactions[tx] = cx
        else:
            raise TypeError("Transaction {!r} already bound to connection {!r}".format(tx, cx0))

    def _on_unbind(self, tx):
        """ Unbind a transaction from a connection.

        :param tx: a bound transaction
        :raise TypeError: if the given transaction is invalid or not bound
        """
        try:
            del self._transactions[tx]
        except KeyError:
            raise TypeError("Invalid or unbound transaction {!r}".format(tx))

    def _on_broken(self, profile, message):
        """ Handle a broken connection.
        """
        log.warning("Connection to %r broken\n%s", profile, message)
        if self._routers is not None:
            log.debug("Removing profile %r from router list", profile)
            try:
                self._routers.remove(profile)
            except ValueError:
                pass  # ignore
            for graph_name, routing_table in self._routing_tables.items():
                log.debug("Removing profile %r from routing table for %s", profile,
                          "default database" if graph_name is None else repr(graph_name))
                routing_table.remove(profile)
        try:
            pool = self._pools[profile]
        except KeyError:
            pass  # no such pool
        else:
            log.debug("Pruning idle connections to %r", profile)
            pool.prune()

    def begin(self, graph_name, readonly=False,
              # after=None, metadata=None, timeout=None
              ):
        """ Begin a new explicit transaction.
        """
        cx = self.acquire(graph_name, readonly=readonly)
        try:
            return cx.begin(graph_name, readonly=readonly,
                            # after=after, metadata=metadata, timeout=timeout
                            )
        except TransactionError:
            # TODO: retry on failure (TransactionError)
            self.prune(cx.profile)
            raise

    def commit(self, tx):
        """ Commit a transaction.

        :param tx: the transaction to commit
        :returns: dictionary of transaction summary information
        :raises ValueError: if the transaction is not valid to be committed
        :raises BrokenTransactionError: if the transaction fails to commit
        """
        cx = self._get_connection(tx)
        try:
            bookmark = cx.commit(tx)
        except TransactionError:
            self.prune(cx.profile)
            raise
        else:
            return {"bookmark": bookmark,
                    "profile": cx.profile,
                    "time": tx.age}

    def rollback(self, tx):
        """ Roll back a transaction.

        :param tx: the transaction to rollback
        :returns: dictionary of transaction summary information
        :raises ValueError: if the transaction is not valid to be rolled back
        :raises BrokenTransactionError: if the transaction fails to rollback
        """
        cx = self._get_connection(tx)
        try:
            bookmark = cx.rollback(tx)
        except TransactionError:
            self.prune(cx.profile)
            raise
        else:
            return {"bookmark": bookmark,
                    "profile": cx.profile,
                    "time": tx.age}

    def auto_run(self, graph_name, cypher, parameters=None, readonly=False,
                 # after=None, metadata=None, timeout=None
                 ):
        """ Run a Cypher query within a new auto-commit transaction.
        """
        cx = self.acquire(graph_name, readonly)
        result = cx.auto_run(graph_name, cypher, parameters, readonly=readonly)
        cx.pull(result)
        try:
            cx.sync(result)
        except TransactionError:
            # TODO: retry on failure (TransactionError)
            self.prune(cx.profile)
            raise
        else:
            return result

    def run_in_tx(self, tx, cypher, parameters=None):
        """ Run a Cypher query within an open explicit transaction.
        """
        cx = self._get_connection(tx)
        result = cx.run_in_tx(tx, cypher, parameters)
        cx.pull(result)
        try:
            cx.sync(result)
        except TransactionError:
            self.prune(cx.profile)
            raise
        else:
            return result

    def supports_multi(self):
        assert self._pools  # this will break if no pools exist
        return all(pool.supports_multi()
                   for pool in self._pools.values())

    def supports_readonly_transactions(self):
        return self.profile.protocol == "bolt"

    def _show_databases(self):
        if self.supports_multi():
            cx = self.acquire("system", readonly=True)
            try:
                result = cx.auto_run("system", "SHOW DATABASES")
                cx.pull(result)
                cx.sync(result)
                return result
            finally:
                cx.release()
        else:
            raise TypeError("Multi-database not supported")

    def graph_names(self):
        """ Fetch a list of available graph database names.
        """

        try:
            result = self._show_databases()
        except TypeError:
            return []
        else:
            value = set()
            while result.has_records():
                (name, address, role, requested_status,
                 current_status, error, default) = result.fetch()
                value.add(name)
            return sorted(value)

    def default_graph_name(self):
        """ Fetch the default graph database name for the service.
        """
        try:
            result = self._show_databases()
        except TypeError:
            return None
        else:
            while result.has_records():
                (name, address, role, requested_status,
                 current_status, error, default) = result.fetch()
                if default:
                    return name
            return None


class RoutingTable(object):

    class Invalid(Exception):
        pass

    def __init__(self, ro_runners, rw_runners, expiry_time):
        self._ro_runners = list(ro_runners)
        self._rw_runners = list(rw_runners)
        self.expiry_time = expiry_time

    def __repr__(self):
        return "%s(%r, %r, %r)" % (self.__class__.__name__,
                                   self._ro_runners, self._rw_runners, self.expiry_time)

    def expired(self):
        return monotonic() >= self.expiry_time

    def runners(self, readonly=False):
        return list(self._ro_runners if readonly else self._rw_runners)

    def remove(self, profile):
        try:
            self._ro_runners.remove(profile)
        except ValueError:
            pass  # ignore, not present
        try:
            self._rw_runners.remove(profile)
        except ValueError:
            pass  # ignore, not present


class Transaction(object):

    def __init__(self, graph_name, txid=None, readonly=False):
        self.graph_name = graph_name
        self.txid = txid or uuid4()
        self.readonly = readonly
        self.__broken = False
        self.__time_created = monotonic()

    def __hash__(self):
        return hash((self.graph_name, self.txid))

    def __eq__(self, other):
        if isinstance(other, Transaction):
            return self.graph_name == other.graph_name and self.txid == other.txid
        else:
            return False

    @property
    def broken(self):
        """ Flag indicating whether this transaction has been broken
        due to disconnection or remote failure.
        """
        return self.__broken

    def mark_broken(self):
        self.__broken = True

    @property
    def age(self):
        return monotonic() - self.__time_created


class Result(object):
    """ Abstract base class representing the result of a Cypher query.
    """

    def __init__(self, graph_name):
        super(Result, self).__init__()
        self._graph_name = graph_name

    @property
    def graph_name(self):
        """ Return the name of the database from which this result
        originates.

        :returns: database name
        """
        return self._graph_name

    @property
    def protocol_version(self):
        """ Return the underlying protocol version used to transfer
        this result, or :const:`None` if not applicable.

        :returns: protocol version
        """
        return None

    def query_id(self):
        """ Return the ID of the query behind this result. This method
        may carry out network activity.

        :returns: query ID or :const:`None`
        :raises: :class:`.BrokenTransactionError` if the transaction is
            broken by an unexpected network event.
        """
        return None

    def buffer(self):
        """ Fetch the remainder of the result into memory. This method
        may carry out network activity.

        :raises: :class:`.BrokenTransactionError` if the transaction is
            broken by an unexpected network event.
        """
        raise NotImplementedError

    def fields(self):
        """ Return the list of field names for records in this result.
        This method may carry out network activity.

        :returns: list of field names
        :raises: :class:`.BrokenTransactionError` if the transaction is
            broken by an unexpected network event.
        """
        raise NotImplementedError

    def records(self):
        """ Iterate through the remaining records, yielding each one in
        turn. This method may carry out network activity.

        :returns: record iterator
        :raises: :class:`.BrokenTransactionError` if the transaction is
            broken by an unexpected network event.
        """
        self.buffer()
        while True:
            record = self.fetch()
            if record is None:
                break
            yield record

    def summary(self):
        """ Gather and return summary information as relates to the
        current progress of query execution and result retrieval. This
        method does not carry out any network activity.

        :returns: summary information
        """
        raise NotImplementedError

    def fetch(self):
        """ Fetch and return the next record in this result, or
        :const:`None` if at the end of the result. This method may carry
        out network activity.

        :returns: the next available record, or :const:`None`
        :raises: :class:`.BrokenTransactionError` if the transaction is
            broken by an unexpected network event.
        """
        raise NotImplementedError

    def has_records(self):
        """ Return :const:`True` if this result contains buffered
        records, :const:`False` otherwise. This method does not carry
        out any network activity.

        :returns: boolean indicator
        """
        raise NotImplementedError

    def take_record(self):
        """ Return the next record from the buffer if one is available,
        :const:`None` otherwise. This method does not carry out any
        network activity.

        :returns: record or :class:`None`
        """
        raise NotImplementedError

    def peek_records(self, limit):
        """ Return up to `limit` records from the buffer if available.
        This method does not carry out any network activity.

        :returns: list of records
        """
        raise NotImplementedError


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


class TransactionError(Exception):
    """ Raised when an error occurs in relation to a transaction."""


class BrokenTransactionError(TransactionError):
    """ Raised when a transaction is broken by the network or remote peer.
    """


class ConnectionUnavailable(Exception):
    """ Raised when a connection cannot be established.
    """


class Hydrant(object):

    def hydrate(self, keys, values, entities=None, version=None):
        raise NotImplementedError

    def dehydrate(self, data, version=None):
        raise NotImplementedError

#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright 2011-2018, Nigel Small
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


from os import listdir
from os.path import isdir, join as path_join
from shutil import rmtree
from threading import Thread


class LocalCluster(object):

    @classmethod
    def install(cls, warehouse, name, version, **databases):
        """ Install a new Causal Cluster or Multicluster.

        :param warehouse: warehouse in which to install
        :param name: cluster or multicluster name
        :param version: Neo4j version (Enterprise edition is required)
        :param databases: pairs of db_name=core_size or db_name=(core_size, rr_size)
        :return: :class:`.LocalCluster` instance
        """
        core_databases = []
        read_replica_databases = []
        for database, size in databases.items():
            if isinstance(size, tuple):
                core_databases.extend([database] * size[0])
                read_replica_databases.extend([database] * size[1])
            else:
                core_databases.extend([database] * size)
        initial_discovery_members = ",".join("localhost:%d" % (18100 + i) for i, database in enumerate(core_databases))
        for i, database in enumerate(core_databases):
            install = warehouse.install(name, "enterprise", version, database=database, role="core", member=i)
            install.set_config("dbms.mode", "CORE")
            install.set_config("dbms.backup.enabled", False)
            install.set_config("dbms.connector.bolt.listen_address", ":%d" % (17100 + i))
            install.set_config("dbms.connector.http.listen_address", ":%d" % (17200 + i))
            install.set_config("dbms.connector.https.listen_address", ":%d" % (17300 + i))
            install.set_config("causal_clustering.database", database)
            install.set_config("causal_clustering.discovery_listen_address", ":%d" % (18100 + i))
            install.set_config("causal_clustering.expected_core_cluster_size", 3)
            install.set_config("causal_clustering.initial_discovery_members", initial_discovery_members)
            install.set_config("causal_clustering.raft_listen_address", ":%d" % (18200 + i))
            install.set_config("causal_clustering.transaction_listen_address", ":%d" % (18300 + i))
        for i, database in enumerate(read_replica_databases):
            install = warehouse.install(name, "enterprise", version, database=database, role="rr", member=i)
            install.set_config("dbms.mode", "READ_REPLICA")
            install.set_config("dbms.backup.enabled", False)
            install.set_config("dbms.connector.bolt.listen_address", ":%d" % (27100 + i))
            install.set_config("dbms.connector.http.listen_address", ":%d" % (27200 + i))
            install.set_config("dbms.connector.https.listen_address", ":%d" % (27300 + i))
            install.set_config("causal_clustering.database", database)
            install.set_config("causal_clustering.discovery_listen_address", ":%d" % (28100 + i))
            install.set_config("causal_clustering.expected_core_cluster_size", 3)
            install.set_config("causal_clustering.initial_discovery_members", initial_discovery_members)
            install.set_config("causal_clustering.raft_listen_address", ":%d" % (28200 + i))
            install.set_config("causal_clustering.transaction_listen_address", ":%d" % (28300 + i))
        return cls(warehouse, name)

    def __init__(self, warehouse, name):
        self.warehouse = warehouse
        self.name = name
        self.installations = {}
        for database, role, member in self._walk_installations():
            self.installations.setdefault(database, {}).setdefault(role, {})[member] = warehouse.get(name, database, role, member)

    def iter_databases(self):
        for database in listdir(path_join(self.warehouse.cc, self.name)):
            yield database

    def databases(self):
        return list(self.iter_databases())

    def iter_members(self, database):
        core_path = path_join(self.warehouse.cc, self.name, database, "core")
        if isdir(core_path):
            for member in listdir(core_path):
                yield "core", member
        else:
            raise RuntimeError("Database %s has no cores" % database)
        rr_path = path_join(self.warehouse.cc, self.name, database, "rr")
        if isdir(rr_path):
            for member in listdir(rr_path):
                yield "rr", member

    def members(self, database):
        return list(self.iter_members(database))

    def _walk_installations(self):
        """ For each installation in the cluster, yield a 3-tuple
        of (database, role, member).
        """
        for database in listdir(path_join(self.warehouse.cc, self.name)):
            core_path = path_join(self.warehouse.cc, self.name, database, "core")
            if isdir(core_path):
                for member in listdir(core_path):
                    yield database, "core", member
            else:
                raise RuntimeError("Database %s has no cores" % database)
            rr_path = path_join(self.warehouse.cc, self.name, database, "rr")
            if isdir(rr_path):
                for member in listdir(rr_path):
                    yield database, "rr", member

    def __repr__(self):
        return "<%s ?>" % (self.__class__.__name__,)

    def for_each(self, database, role, f):
        if not callable(f):
            raise TypeError("Callback is not callable")

        threads = []

        def call_f(i):
            t = Thread(target=f, args=[i])
            t.start()
            threads.append(t)

        for r, member in self.members(database):
            if r == role or (isinstance(role, (tuple, set)) and r in role):
                install = self.warehouse.get(self.name, database, r, member)
                call_f(install)

        return threads

    @staticmethod
    def join_all(threads):
        while threads:
            for thread in list(threads):
                if thread.is_alive():
                    thread.join(timeout=0.1)
                else:
                    threads.remove(thread)

    def start(self):
        threads = []
        for database in self.databases():
            threads.extend(self.for_each(database, {"core", "rr"},
                                         lambda install: install.server.start()))
        self.join_all(threads)

    def stop(self):
        threads = []
        for database in self.databases():
            threads.extend(self.for_each(database, {"core", "rr"},
                                         lambda install: install.server.stop()))
        self.join_all(threads)

    def running(self):
        running = []
        threads = []
        for database in self.databases():
            threads.extend(self.for_each(database, {"core", "rr"},
                                         lambda install: running.append(install.server.running())))
        self.join_all(threads)
        return all(running)

    def uninstall(self):
        self.stop()
        self.installations.clear()
        rmtree(self.warehouse.cc, self.name)

    def update_auth(self, user, password):
        """ Update the auth file for each member with the given credentials.
        """
        threads = []
        for database in self.databases():
            threads.extend(self.for_each(database, {"core", "rr"}, lambda install: install.auth.update(user, password)))
        self.join_all(threads)

    @property
    def http_uris(self):
        uris = set()
        threads = []
        for database in self.databases():
            threads.extend(self.for_each(database, {"core", "rr"}, lambda install: uris.add(install.http_uri)))
        self.join_all(threads)
        return uris

    @property
    def https_uris(self):
        uris = set()
        threads = []
        for database in self.databases():
            threads.extend(self.for_each(database, {"core", "rr"}, lambda install: uris.add(install.https_uri)))
        self.join_all(threads)
        return uris

    @property
    def bolt_uris(self):
        uris = set()
        threads = []
        for database in self.databases():
            threads.extend(self.for_each(database, {"core", "rr"}, lambda install: uris.add(install.bolt_uri)))
        self.join_all(threads)
        return uris

    @property
    def bolt_routing_uris(self):
        uris = set()
        threads = []
        for database in self.databases():
            threads.extend(self.for_each(database, "core", lambda install: uris.add(install.bolt_routing_uri)))
        self.join_all(threads)
        return uris

#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright 2011-2019, Nigel Small
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


from os import getenv
from uuid import uuid4

from pytest import fixture

from py2neo import Graph
from py2neo.admin.dist import Distribution, minor_versions
from py2neo.admin.install import Warehouse
from py2neo.database import Database
from py2neo.experimental.clustering import LocalCluster
from py2neo.internal.connectors import Connector


NEO4J_EDITION = "enterprise"
NEO4J_VERSION = getenv("NEO4J_VERSION", minor_versions[-1])
NEO4J_PROTOCOLS = ["bolt+routing"]
NEO4J_HOST = "localhost"
NEO4J_PORTS = {
    "bolt+routing": 10087,
}
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "password"
NEO4J_DEBUG = getenv("NEO4J_DEBUG", "")
NEO4J_PROCESS = {}


@fixture(scope="session")
def neo4j_version():
    return Distribution(version=NEO4J_VERSION).version


@fixture(scope="session")
def neo4j_minor_version(neo4j_version):
    return ".".join(neo4j_version.split(".")[:2])


@fixture(scope="session", params=NEO4J_PROTOCOLS)
def uri(request):
    protocol = request.param
    uri = "{}://{}:{}".format(protocol, NEO4J_HOST, NEO4J_PORTS[protocol])
    warehouse = Warehouse()
    name = uuid4().hex
    cluster = LocalCluster.install(warehouse, name, NEO4J_VERSION, alpha=(3, 1))
    print("Installed Neo4j %s %s cluster (%s)" % (NEO4J_EDITION.title(), NEO4J_VERSION, ", ".join(
        "%s/%s" % (role, member) for role, member in sorted(cluster.iter_members("alpha"))
    )))
    cluster.update_auth(NEO4J_USER, NEO4J_PASSWORD)
    cluster.start()
    print("Started Neo4j cluster")
    NEO4J_PROCESS.update({
        "warehouse": warehouse,
        "name": name,
        "cluster": cluster,
    })
    if NEO4J_DEBUG:
        from neobolt.diagnostics import watch
        watch("neobolt")
    return uri


@fixture(scope="session")
def connector(uri):
    return Connector(uri)


@fixture(scope="session")
def database(uri):
    return Database(uri)


@fixture(scope="session")
def graph(uri):
    return Graph(uri)


@fixture()
def make_unique_id():
    return lambda: "_" + uuid4().hex


@fixture(scope="function", autouse=True)
def check_bolt_connections_released(connector):
    """ Called after each individual test to ensure all connections in the
    pool have been correctly released.
    """
    yield
    if "bolt" in connector.scheme:
        address = connector.connection_data["host"], connector.connection_data["port"]
        # print(connector.pool.in_use_connection_count(address), "/", len(connector.pool.connections.get(address, [])))
        assert connector.pool.in_use_connection_count(address) == 0


def pytest_sessionfinish(session, exitstatus):
    """ Called after the entire session to ensure Neo4j is shut down.
    """
    if NEO4J_PROCESS:
        print("Stopping Neo4j cluster")
        NEO4J_PROCESS["cluster"].stop()
        NEO4J_PROCESS["cluster"].uninstall()
        NEO4J_PROCESS.clear()
        Database.forget_all()

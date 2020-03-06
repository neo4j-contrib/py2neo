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


from os import getenv
from os.path import dirname, join as path_join
from socket import create_connection
from uuid import uuid4

from pytest import fixture

from py2neo import Graph
from py2neo.admin.dist import Distribution, minor_versions
from py2neo.admin.install import Warehouse
from py2neo.database import GraphService
from py2neo.internal.compat import SocketError
from py2neo.internal.connectors import Connector
from py2neo.net import ConnectionProfile


NEO4J_EDITION = "enterprise"
NEO4J_VERSION = getenv("NEO4J_VERSION", minor_versions[-1])
NEO4J_PROTOCOLS = ["bolt", "http"]  # TODO: https/cert
NEO4J_HOST = "localhost"
NEO4J_PORTS = {
    "bolt": 7687,
    "http": 7474,
    "https": 7473,
}
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "password"
NEO4J_DEBUG = getenv("NEO4J_DEBUG", "")
NEO4J_PROCESS = {}


def is_server_running():
    host = NEO4J_HOST
    port = NEO4J_PORTS["bolt"]
    try:
        s = create_connection((host, port))
    except SocketError:
        return False
    else:
        # TODO: ensure version is correct
        s.close()
        return True


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
    if not is_server_running():
        warehouse = Warehouse()
        name = uuid4().hex
        installation = warehouse.install(name, NEO4J_EDITION, NEO4J_VERSION)
        print("Installed Neo4j %s %s to %s" % (NEO4J_EDITION, NEO4J_VERSION, installation.home))
        installation.auth.update(NEO4J_USER, NEO4J_PASSWORD)
        pid = installation.server.start()
        print("Started Neo4j server with PID %d" % pid)
        NEO4J_PROCESS.update({
            "warehouse": warehouse,
            "name": name,
            "installation": installation,
            "pid": pid,
        })
    if NEO4J_DEBUG:
        from py2neo.diagnostics import watch
        watch("py2neo")
    return uri


@fixture(scope="session")
def connection_profile(uri):
    return ConnectionProfile(uri)


@fixture(scope="session")
def connector(connection_profile):
    return Connector(connection_profile)


@fixture(scope="session")
def graph_service(uri):
    return GraphService(uri)


@fixture(scope="session")
def graph(uri):
    return Graph(uri)


@fixture(scope="function")
def movie_graph(graph):
    graph.delete_all()
    with open(path_join(dirname(__file__), "..", "resources", "movies.cypher")) as f:
        cypher = f.read()
    graph.run(cypher)
    yield graph
    graph.delete_all()


@fixture()
def make_unique_id():
    return lambda: "_" + uuid4().hex


@fixture(scope="function", autouse=True)
def check_bolt_connections_released(connector):
    """ Called after each individual test to ensure all connections in the
    pool have been correctly released.
    """
    yield
    if connector.profile.protocol == "bolt":
        try:
            assert connector.pool.in_use == 0
        except AttributeError:
            address = connector.connection_data["host"], connector.connection_data["port"]
            # print(connector.pool.in_use_connection_count(address), "/", len(connector.pool.connections.get(address, [])))
            assert connector.pool.in_use_connection_count(address) == 0


def pytest_sessionfinish(session, exitstatus):
    """ Called after the entire session to ensure Neo4j is shut down.
    """
    if NEO4J_PROCESS:
        print("Stopping Neo4j server with PID %d" % NEO4J_PROCESS["pid"])
        NEO4J_PROCESS["installation"].server.stop()
        NEO4J_PROCESS["warehouse"].uninstall(NEO4J_PROCESS["name"])
        NEO4J_PROCESS.clear()
        GraphService.forget_all()

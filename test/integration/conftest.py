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
from py2neo.admin.dist import Distribution
from py2neo.admin.install import Warehouse
from py2neo.connect import Connector, ConnectionProfile
from py2neo.database import GraphService
from py2neo.internal.compat import SocketError


NEO4J_HOST = "localhost"
NEO4J_PORTS = {
    "bolt": 7687,
    "bolt+s": 7687,
    "bolt+ssc": 7687,
    "http": 7474,
    "https": 7473,
    "http+s": 7473,
    "http+ssc": 7473,
}
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "password"
NEO4J_DEBUG = getenv("NEO4J_DEBUG", "")
NEO4J_PROCESS = {}


class ServiceProfile(object):

    def __init__(self, release=None, topology=None, cert=None, schemes=None):
        self.release = release
        self.topology = topology   # "CE|EE-SI|EE-C3|EE-C3-R2"
        self.cert = cert
        self.schemes = schemes

    def __str__(self):
        server = "%s.%s %s" % (self.release[0], self.release[1], self.topology)
        if self.cert:
            server += " %s" % (self.cert,)
        schemes = " ".join(self.schemes)
        return "[%s]-[%s]" % (server, schemes)


class ServiceConnectionProfile(object):

    def __init__(self, release=None, topology=None, cert=None, scheme=None):
        self.release = release
        self.topology = topology   # "CE|EE-SI|EE-C3|EE-C3-R2"
        self.cert = cert
        self.scheme = scheme

    def __str__(self):
        extra = "%s" % (self.topology,)
        if self.cert:
            extra += "; %s" % (self.cert,)
        bits = [
            "Neo4j/%s.%s (%s)" % (self.release[0], self.release[1], extra),
            "over",
            "'%s'" % self.scheme,
        ]
        return " ".join(bits)

    @property
    def edition(self):
        if self.topology.startswith("CE"):
            return "community"
        elif self.topology.startswith("EE"):
            return "enterprise"
        else:
            return None

    @property
    def release_str(self):
        return ".".join(map(str, self.release))


# TODO: test with full certificates
UNSECURED_SCHEMES = ["bolt", "http"]
ALL_SCHEMES = ["bolt", "bolt+s", "bolt+ssc", "http", "https", "http+s", "http+ssc"]
SSC_SCHEMES = ["bolt", "bolt+ssc", "http", "http+ssc"]
SERVICE_PROFILES = {
    11: [
        ServiceProfile(release=(4, 0), topology="CE", schemes=UNSECURED_SCHEMES),
        ServiceProfile(release=(4, 0), topology="CE", cert="ssc", schemes=SSC_SCHEMES),
        # # ServiceProfile(release=(4, 0), topology="CE", cert="full", schemes=ALL_SCHEMES),
        ServiceProfile(release=(3, 5), topology="CE", schemes=SSC_SCHEMES),
        # # ServiceProfile(release=(3, 5), topology="CE", cert="full", schemes=ALL_SCHEMES),
    ],
    8: [
        ServiceProfile(release=(3, 4), topology="CE", schemes=SSC_SCHEMES),
        # # ServiceProfile(release=(3, 4), topology="CE", cert="full", schemes=ALL_SCHEMES),
        ServiceProfile(release=(3, 3), topology="CE", schemes=SSC_SCHEMES),
        # # ServiceProfile(release=(3, 3), topology="CE", cert="full", schemes=ALL_SCHEMES),
        ServiceProfile(release=(3, 2), topology="CE", schemes=SSC_SCHEMES),
        # # ServiceProfile(release=(3, 2), topology="CE", cert="full", schemes=ALL_SCHEMES),
    ],
}
SERVICE_CONNECTION_PROFILES = [
    ServiceConnectionProfile(release=sp.release, topology=sp.topology, cert=sp.cert, scheme=s)
    for sp in SERVICE_PROFILES[int(getenv("JAVA_VERSION", max(SERVICE_PROFILES.keys())))]
    for s in sp.schemes
]


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


@fixture(scope="session", params=SERVICE_CONNECTION_PROFILES,
         ids=list(map(str, SERVICE_CONNECTION_PROFILES)))
def service_connection_profile(request):
    sc_profile = request.param
    yield sc_profile


@fixture(scope="session")
def neo4j_version(service_connection_profile):
    return Distribution(version=service_connection_profile.release_str).version


@fixture(scope="session")
def neo4j_minor_version(neo4j_version):
    return ".".join(neo4j_version.split(".")[:2])


@fixture(scope="session")
def uri(service_connection_profile):
    scp = service_connection_profile
    port = NEO4J_PORTS[scp.scheme]
    uri = "{}://{}:{}".format(scp.scheme, NEO4J_HOST, port)
    if not is_server_running():
        warehouse = Warehouse()
        name = uuid4().hex
        installation = warehouse.install(name, scp.edition, scp.release_str)
        print("Installed Neo4j %s %s to %s" % (scp.edition, scp.release_str, installation.home))
        if scp.cert == "full":
            assert False
        elif scp.cert == "ssc":
            print("Installing self-signed certificate")
            installation.install_self_signed_certificate()
            installation.set_config("dbms.ssl.policy.bolt.enabled", True)
            installation.set_config("dbms.ssl.policy.https.enabled", True)
            installation.set_config("dbms.connector.bolt.tls_level", "OPTIONAL")
            installation.set_config("dbms.connector.https.enabled", True)
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
    yield uri
    stop_neo4j()


@fixture(scope="session")
def connection_profile(uri):
    return ConnectionProfile(uri)


@fixture(scope="session")
def connector(connection_profile):
    return Connector.open(connection_profile)


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
            assert connector.in_use == 0
        except AttributeError:
            # print(connector.pool.in_use_connection_count(address), "/", len(connector.pool.connections.get(address, [])))
            assert connector.in_use_connection_count(connector.profile.address) == 0


def pytest_sessionfinish(session, exitstatus):
    """ Called after the entire session to ensure Neo4j is shut down.
    """
    stop_neo4j()


def stop_neo4j():
    if NEO4J_PROCESS:
        print("Stopping Neo4j server with PID %d" % NEO4J_PROCESS["pid"])
        NEO4J_PROCESS["installation"].server.stop()
        NEO4J_PROCESS["warehouse"].uninstall(NEO4J_PROCESS["name"])
        NEO4J_PROCESS.clear()
        GraphService.forget_all()

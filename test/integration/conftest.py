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
from uuid import uuid4

from pytest import fixture

from py2neo import Graph
from py2neo.client import Connector, ConnectionProfile
from py2neo.database import GraphService
from py2neo.server import Neo4jService
from py2neo.security import make_self_signed_certificate


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


UNSECURED_SCHEMES = ["bolt", "http"]
ALL_SCHEMES = ["bolt", "bolt+s", "bolt+ssc", "http", "https", "http+s", "http+ssc"]
SSC_SCHEMES = ["bolt", "bolt+ssc", "http", "http+ssc"]


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


class TestProfile(object):

    def __init__(self, service_profile=None, scheme=None):
        self.service_profile = service_profile
        self.scheme = scheme
        assert self.topology == "CE"

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
    def release(self):
        return self.service_profile.release

    @property
    def topology(self):
        return self.service_profile.topology

    @property
    def cert(self):
        return self.service_profile.cert

    @property
    def release_str(self):
        return ".".join(map(str, self.release))

    def generate_uri(self, service_name=None):
        if self.cert == "full":
            raise NotImplementedError("Full certificates are not yet supported")
        elif self.cert == "ssc":
            cert_key_pair = make_self_signed_certificate()
        else:
            cert_key_pair = None, None
        service = Neo4jService.single_instance(name=service_name,
                                               image_tag=self.release_str,
                                               auth=("neo4j", "password"),
                                               cert_key_pair=cert_key_pair)
        service.start()
        try:
            addresses = [instance.addresses[self.scheme]
                         for instance in service.instances]
            uris = ["{}://{}:{}".format(self.scheme, address.host, address.port)
                    for address in addresses]
            yield uris[0]
        finally:
            service.stop()


# TODO: test with full certificates
neo4j_service_profiles = [
    ServiceProfile(release=(4, 0), topology="CE", schemes=UNSECURED_SCHEMES),
    ServiceProfile(release=(4, 0), topology="CE", cert="ssc", schemes=SSC_SCHEMES),
    # ServiceProfile(release=(4, 0), topology="CE", cert="full", schemes=ALL_SCHEMES),
    ServiceProfile(release=(3, 5), topology="CE", cert="ssc", schemes=SSC_SCHEMES),
    # ServiceProfile(release=(3, 5), topology="CE", cert="full", schemes=ALL_SCHEMES),
    ServiceProfile(release=(3, 4), topology="CE", cert="ssc", schemes=SSC_SCHEMES),
    # ServiceProfile(release=(3, 4), topology="CE", cert="full", schemes=ALL_SCHEMES),
]
neo4j_test_profiles = [TestProfile(sp, scheme=s)
                       for sp in neo4j_service_profiles
                       for s in sp.schemes]


@fixture(scope="session",
         params=neo4j_test_profiles,
         ids=list(map(str, neo4j_test_profiles)))
def test_profile(request):
    GraphService.forget_all()
    test_profile = request.param
    yield test_profile


@fixture(scope="session")
def uri(test_profile):
    for uri in test_profile.generate_uri("py2neo"):
        yield uri
    return


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

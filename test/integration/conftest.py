#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright (c) "Neo4j"
# Neo4j Sweden AB [https://neo4j.com]
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


from os import getenv, path
from uuid import uuid4

from grolt import Neo4jService, Neo4jDirectorySpec
from grolt.security import install_self_signed_certificate
from pytest import fixture

from py2neo import ServiceProfile, GraphService, Graph
from py2neo.client import Connector
from py2neo.ogm import Repository


NEO4J_PROCESS = {}
NEO4J_VERSION = getenv("NEO4J_VERSION", "")


UNSECURED_SCHEMES = ["neo4j", "bolt", "http"]
ALL_SCHEMES = ["neo4j", "neo4j+s", "neo4j+ssc",
               "bolt", "bolt+s", "bolt+ssc",
               "http", "https", "http+s", "http+ssc"]
SSC_SCHEMES = ["neo4j", "neo4j+ssc", "bolt", "bolt+ssc", "http", "http+ssc"]

UNSECURED_LEGACY_SCHEMES = ["bolt", "http"]
ALL_LEGACY_SCHEMES = ["bolt", "bolt+s", "bolt+ssc", "http", "https", "http+s", "http+ssc"]
SSC_LEGACY_SCHEMES = ["bolt", "bolt+ssc", "http", "http+ssc"]


class DeploymentProfile(object):

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

    def __init__(self, deployment_profile=None, scheme=None):
        self.deployment_profile = deployment_profile
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
        return self.deployment_profile.release

    @property
    def topology(self):
        return self.deployment_profile.topology

    @property
    def cert(self):
        return self.deployment_profile.cert

    @property
    def release_str(self):
        return ".".join(map(str, self.release))

    def generate_uri(self, service_name=None):
        if self.cert == "full":
            raise NotImplementedError("Full certificates are not yet supported")
        elif self.cert == "ssc":
            certificates_dir = install_self_signed_certificate(self.release_str)
            dir_spec = Neo4jDirectorySpec(certificates_dir=certificates_dir)
        else:
            dir_spec = None
        with Neo4jService(name=service_name, image=self.release_str,
                          auth=("neo4j", "password"), dir_spec=dir_spec) as service:
            uris = [router.uri(self.scheme) for router in service.routers()]
            yield service, uris[0]


# TODO: test with full certificates
neo4j_deployment_profiles = [
    DeploymentProfile(release=(4, 4), topology="CE", schemes=UNSECURED_SCHEMES),
    DeploymentProfile(release=(4, 4), topology="CE", cert="ssc", schemes=SSC_SCHEMES),
    # ServiceProfile(release=(4, 4), topology="CE", cert="full", schemes=ALL_SCHEMES),
    DeploymentProfile(release=(4, 3), topology="CE", schemes=UNSECURED_SCHEMES),
    DeploymentProfile(release=(4, 3), topology="CE", cert="ssc", schemes=SSC_SCHEMES),
    # ServiceProfile(release=(4, 3), topology="CE", cert="full", schemes=ALL_SCHEMES),
    DeploymentProfile(release=(4, 2), topology="CE", schemes=UNSECURED_SCHEMES),
    DeploymentProfile(release=(4, 2), topology="CE", cert="ssc", schemes=SSC_SCHEMES),
    # ServiceProfile(release=(4, 2), topology="CE", cert="full", schemes=ALL_SCHEMES),
    DeploymentProfile(release=(4, 1), topology="CE", schemes=UNSECURED_SCHEMES),
    DeploymentProfile(release=(4, 1), topology="CE", cert="ssc", schemes=SSC_SCHEMES),
    # ServiceProfile(release=(4, 1), topology="CE", cert="full", schemes=ALL_SCHEMES),
    DeploymentProfile(release=(4, 0), topology="CE", schemes=UNSECURED_SCHEMES),
    DeploymentProfile(release=(4, 0), topology="CE", cert="ssc", schemes=SSC_SCHEMES),
    # ServiceProfile(release=(4, 0), topology="CE", cert="full", schemes=ALL_SCHEMES),
    DeploymentProfile(release=(3, 5), topology="CE", schemes=UNSECURED_LEGACY_SCHEMES),
    DeploymentProfile(release=(3, 5), topology="CE", cert="ssc", schemes=SSC_LEGACY_SCHEMES),
    # ServiceProfile(release=(3, 5), topology="CE", cert="full", schemes=ALL_LEGACY_SCHEMES),
    DeploymentProfile(release=(3, 4), topology="CE", schemes=UNSECURED_LEGACY_SCHEMES),
    DeploymentProfile(release=(3, 4), topology="CE", cert="ssc", schemes=SSC_LEGACY_SCHEMES),
    # ServiceProfile(release=(3, 4), topology="CE", cert="full", schemes=ALL_LEGACY_SCHEMES),
]

if NEO4J_VERSION == "LATEST":
    neo4j_deployment_profiles = neo4j_deployment_profiles[:1]
elif NEO4J_VERSION == "4.x":
    neo4j_deployment_profiles = [profile for profile in neo4j_deployment_profiles
                                 if profile.release[0] == 4]
elif NEO4J_VERSION == "4.4":
    neo4j_deployment_profiles = [profile for profile in neo4j_deployment_profiles
                                 if profile.release == (4, 4)]
elif NEO4J_VERSION == "4.3":
    neo4j_deployment_profiles = [profile for profile in neo4j_deployment_profiles
                                 if profile.release == (4, 3)]
elif NEO4J_VERSION == "4.2":
    neo4j_deployment_profiles = [profile for profile in neo4j_deployment_profiles
                                 if profile.release == (4, 2)]
elif NEO4J_VERSION == "4.1":
    neo4j_deployment_profiles = [profile for profile in neo4j_deployment_profiles
                                 if profile.release == (4, 1)]
elif NEO4J_VERSION == "4.0":
    neo4j_deployment_profiles = [profile for profile in neo4j_deployment_profiles
                                 if profile.release == (4, 0)]
elif NEO4J_VERSION == "3.x":
    neo4j_deployment_profiles = [profile for profile in neo4j_deployment_profiles
                                 if profile.release[0] == 3]
elif NEO4J_VERSION == "3.5":
    neo4j_deployment_profiles = [profile for profile in neo4j_deployment_profiles
                                 if profile.release == (3, 5)]
elif NEO4J_VERSION == "3.4":
    neo4j_deployment_profiles = [profile for profile in neo4j_deployment_profiles
                                 if profile.release == (3, 4)]

neo4j_test_profiles = [TestProfile(deployment_profile, scheme=scheme)
                       for deployment_profile in neo4j_deployment_profiles
                       for scheme in deployment_profile.schemes]


@fixture(scope="session",
         params=neo4j_test_profiles,
         ids=list(map(str, neo4j_test_profiles)))
def test_profile(request):
    test_profile = request.param
    yield test_profile


@fixture(scope="session")
def neo4j_service_and_uri(test_profile):
    for service, uri in test_profile.generate_uri("py2neo"):
        yield service, uri
    return


@fixture(scope="session")
def neo4j_service(neo4j_service_and_uri):
    neo4j_service, _ = neo4j_service_and_uri
    return neo4j_service


@fixture(scope="session")
def uri(neo4j_service_and_uri):
    _, uri = neo4j_service_and_uri
    return uri


@fixture(scope="session")
def service_profile(uri):
    return ServiceProfile(uri)


@fixture(scope="session")
def connector(service_profile):
    return Connector(service_profile)


@fixture(scope="session")
def graph_service(uri):
    return GraphService(uri)


@fixture(scope="session")
def graph(uri):
    return Graph(uri)


@fixture(scope="function")
def repo(graph):
    graph.delete_all()
    yield Repository.wrap(graph)
    graph.delete_all()


@fixture(scope="function")
def movie_graph(graph):
    graph.delete_all()
    with open(path.join(path.dirname(__file__), "..", "resources", "movies.cypher")) as f:
        cypher = f.read()
    graph.run(cypher)
    yield graph
    graph.delete_all()


@fixture(scope="function")
def movie_repo(movie_graph):
    return Repository.wrap(movie_graph)


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
            assert sum(connector.in_use.values()) == 0
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

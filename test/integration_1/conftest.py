

from os import getenv
from socket import create_connection
from uuid import uuid4

from pytest import fixture

from py2neo import Graph
from py2neo.admin.dist import Distribution, minor_versions
from py2neo.admin.install import Warehouse
from py2neo.database import Database
from py2neo.internal.connectors import Connector


NEO4J_EDITION = "community"
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
NEO4J_RUNNING = {}


def is_server_running():
    host = NEO4J_HOST
    port = NEO4J_PORTS["bolt"]
    try:
        s = create_connection((host, port))
    except OSError:
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
    if is_server_running():
        yield uri
    else:
        warehouse = Warehouse()
        name = uuid4().hex
        installation = warehouse.install(name, NEO4J_EDITION, NEO4J_VERSION)
        print("Installed Neo4j %s %s to %s" % (NEO4J_EDITION, NEO4J_VERSION, installation.home))
        installation.auth.update(NEO4J_USER, NEO4J_PASSWORD)
        pid = installation.server.start()
        print("Started Neo4j server with PID %d" % pid)
        NEO4J_RUNNING.update({
            "warehouse": warehouse,
            "name": name,
            "installation": installation,
            "pid": pid,
        })
        yield uri
    if protocol == NEO4J_PROTOCOLS[-1] and NEO4J_RUNNING:
        NEO4J_RUNNING["installation"].server.stop()
        NEO4J_RUNNING["warehouse"].uninstall(NEO4J_RUNNING["name"])
        NEO4J_RUNNING.clear()
        Database.forget_all()


@fixture(scope="session")
def connector(uri):
    return Connector(uri)


@fixture(scope="session")
def graph(uri):
    return Graph(uri)


@fixture()
def make_unique_id():
    return lambda: "_" + uuid4().hex

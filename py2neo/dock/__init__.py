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


from collections import namedtuple
from logging import getLogger
from random import choice
from threading import Thread
from time import sleep
from uuid import uuid4

from py2neo.connect.addressing import Address
from py2neo.connect.wire import Wire
from py2neo.dock.console import Neo4jConsole
from py2neo.internal.compat import perf_counter


log = getLogger(__name__)


Auth = namedtuple("Auth", ["user", "password"])


def make_auth(value=None, default_user=None, default_password=None):
    try:
        user, _, password = str(value or "").partition(":")
    except AttributeError:
        raise ValueError("Invalid auth string {!r}".format(value))
    else:
        return Auth(user or default_user or "neo4j",
                    password or default_password or uuid4().hex)


def resolve_image(image):
    """ Resolve an informal image tag into a full Docker image tag. Any tag
    available on Docker Hub for Neo4j can be used, and if no 'neo4j:' prefix
    exists, this will be added automatically. The default edition is
    Community, unless a cluster is being created in which case Enterprise
    edition is selected instead. Explicit selection of Enterprise edition can
    be made by adding an '-enterprise' suffix to the image tag.

    If a 'file:' URI is passed in here instead of an image tag, the Docker
    image will be loaded from that file instead.

    Examples of valid tags:
    - 3.4.6
    - neo4j:3.4.6
    - latest
    - file:/home/me/image.tar

    """
    resolved = image
    if resolved.startswith("file:"):
        return load_image_from_file(resolved[5:])
    if ":" not in resolved:
        resolved = "neo4j:" + image
    return resolved


def load_image_from_file(name):
    from docker import DockerClient
    docker = DockerClient.from_env(version="auto")
    with open(name, "rb") as f:
        images = docker.images.load(f.read())
        image = images[0]
        return image.tags[0]


class Neo4jInstanceProfile(object):
    # Base config for all machines. This can be overridden by
    # individual instances.
    config = {
        "dbms.backup.enabled": "false",
        "dbms.memory.heap.initial_size": "300m",
        "dbms.memory.heap.max_size": "500m",
        "dbms.memory.pagecache.size": "50m",
        "dbms.transaction.bookmark_ready_timeout": "5s",
    }

    def __init__(
            self,
            name,
            service_name,
            bolt_port,
            http_port,
            https_port,
            config,
            env
    ):
        self.name = name
        self.service_name = service_name
        self.bolt_port = bolt_port
        self.http_port = http_port
        self.https_port = https_port
        self.env = dict(env or {})
        self.config = dict()
        self.config["dbms.connector.bolt.advertised_address"] = \
            "localhost:{}".format(self.bolt_port)
        if config:
            self.config.update(**config)

    def __hash__(self):
        return hash(self.fq_name)

    @property
    def fq_name(self):
        return "{}.{}".format(self.name, self.service_name)

    @property
    def http_uri(self):
        return "http://localhost:{}".format(self.http_port)

    @property
    def bolt_address(self):
        return Address(("localhost", self.bolt_port))

    @property
    def addresses(self):
        # TODO: include only those that are configured
        return {
            "bolt": Address(("localhost", self.bolt_port)),
            "http": Address(("localhost", self.http_port)),
            "https": Address(("localhost", self.https_port)),
        }

    def connection_profile(self, protocol):
        pass


class Neo4jInstance(object):
    """ A single Neo4j server instance, potentially part of a cluster.
    """

    container = None

    ip_address = None

    ready = 0

    def __init__(self, spec, image, auth):
        from docker import DockerClient
        from docker.errors import ImageNotFound
        self.spec = spec
        self.image = image
        self.address = self.addresses["bolt"]
        self.auth = auth
        self.docker = DockerClient.from_env(version="auto")
        environment = {}
        if self.auth:
            environment["NEO4J_AUTH"] = "/".join(self.auth)
        environment["NEO4J_ACCEPT_LICENSE_AGREEMENT"] = "yes"
        for key, value in self.spec.config.items():
            fixed_key = "NEO4J_" + key.replace("_", "__").replace(".", "_")
            environment[fixed_key] = value
        for key, value in self.spec.env.items():
            environment[key] = value
        ports = {
                    "7474/tcp": self.spec.http_port,
                    "7473/tcp": self.spec.https_port,
                    "7687/tcp": self.spec.bolt_port,
                }

        def create_container(img):
            return self.docker.containers.create(
                img,
                detach=True,
                environment=environment,
                hostname=self.spec.fq_name,
                name=self.spec.fq_name,
                network=self.spec.service_name,
                ports=ports,
            )

        try:
            self.container = create_container(self.image)
        except ImageNotFound:
            log.info("Downloading Docker image %r", self.image)
            self.docker.images.pull(self.image)
            self.container = create_container(self.image)

    def __hash__(self):
        return hash(self.container)

    def __repr__(self):
        return "%s(fq_name={!r}, image={!r}, address={!r})".format(
            self.__class__.__name__, self.spec.fq_name,
            self.image, self.address)

    @property
    def addresses(self):
        return self.spec.addresses

    def start(self):
        from docker.errors import APIError
        log.info("Starting machine %r at "
                 "«%s»", self.spec.fq_name, self.address)
        try:
            self.container.start()
            self.container.reload()
            self.ip_address = (self.container.attrs["NetworkSettings"]
                               ["Networks"][self.spec.service_name]["IPAddress"])
        except APIError as e:
            log.info(e)

        log.debug("Machine %r has internal IP address "
                  "«%s»", self.spec.fq_name, self.ip_address)

    def _poll_bolt_address(self, count=240, interval=0.5):
        address = self.addresses["bolt"]
        t0 = perf_counter()
        for _ in range(count):
            wire = None
            try:
                wire = Wire.open(address, keep_alive=True)
                wire.write(b"\x60\x60\xB0\x17"
                           b"\x00\x00\x00\x04"
                           b"\x00\x00\x00\x03"
                           b"\x00\x00\x00\x02"
                           b"\x00\x00\x00\x01")
                wire.send()
                data = wire.read(4)
            except OSError:
                sleep(interval)
            else:
                t1 = perf_counter() - t0
                log.info("Machine {!r} available "
                         "for Bolt traffic "
                         "after {:.02f}s".format(self.spec.fq_name, t1))
                return True
            finally:
                if wire:
                    wire.close()
        return False

    def _poll_http_address(self, count=240, interval=0.5):
        address = self.addresses["http"]
        t0 = perf_counter()
        for _ in range(count):
            wire = None
            try:
                wire = Wire.open(address, keep_alive=True)
                wire.write("GET / HTTP/1.1\r\n"
                           "Host: {}\r\n\r\n".format(address.host).encode("ASCII"))
                wire.send()
                data = wire.read(4)
            except OSError:
                sleep(interval)
            else:
                t1 = perf_counter() - t0
                log.info("Machine {!r} available "
                         "for HTTP traffic "
                         "after {:.02f}s".format(self.spec.fq_name, t1))
                return True
            finally:
                if wire:
                    wire.close()
        return False

    def await_started(self):
        sleep(1)
        self.container.reload()
        if self.container.status == "running":

            result = {}

            def poll_bolt_address():
                result["bolt"] = self._poll_bolt_address()

            def poll_http_address():
                result["http"] = self._poll_http_address()

            threads = [
                Thread(target=poll_bolt_address),
                Thread(target=poll_http_address),
            ]
            for t in threads:
                t.start()
            for t in threads:
                t.join()
            if any(value is False for value in result.values()):
                self.container.reload()
                state = self.container.attrs["State"]
                if state["Status"] == "exited":
                    self.ready = -1
                    log.error("Machine %r exited with code %r",
                              self.spec.fq_name, state["ExitCode"])
                    for line in self.container.logs().splitlines():
                        log.error("> %s" % line.decode("utf-8"))
                else:
                    log.error("Machine %r did not "
                              "become available", self.spec.fq_name)
            else:
                self.ready = 1
        else:
            log.error("Machine %r is not running (status=%r)",
                      self.spec.fq_name, self.container.status)
            for line in self.container.logs().splitlines():
                log.error("> %s" % line.decode("utf-8"))

    def stop(self):
        log.info("Stopping machine %r", self.spec.fq_name)
        self.container.stop()
        self.container.remove(force=True)


class Neo4jService:
    """ A Neo4j database management service.
    """

    default_image = NotImplemented

    default_bolt_port = 7687
    default_http_port = 7474
    default_https_port = 7473

    @classmethod
    def single_instance(cls, name, image, auth, config, env):
        service = cls(name, image, auth)
        spec = Neo4jInstanceProfile(
            name="a",
            service_name=service.name,
            bolt_port=7687,
            http_port=7474,
            https_port=7473,
            config=config,
            env=env,
        )
        service.machines[spec] = Neo4jInstance(
            spec,
            service.image,
            auth=service.auth,
        )
        return service

    @classmethod
    def _random_name(cls):
        return "".join(choice("bcdfghjklmnpqrstvwxz") for _ in range(7))

    # noinspection PyUnusedLocal
    def __init__(self, name, image, auth):
        from docker import DockerClient
        self.name = name or self._random_name()
        self.image = resolve_image(image or self.default_image)
        self.auth = Auth(*auth) if auth else make_auth()
        if self.auth.user != "neo4j":
            raise ValueError("Auth user must be 'neo4j' or empty")
        self.docker = DockerClient.from_env(version="auto")
        self.machines = {}
        self.network = None
        self.console = None

    def __enter__(self):
        try:
            self.start()
        except KeyboardInterrupt:
            self.stop()
            raise
        else:
            return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()

    def _get_machine_by_address(self, address):
        address = Address((address.host, address.port_number))
        for spec, machine in self.machines.items():
            if spec.bolt_address == address:
                return machine

    def _for_each_machine(self, f):
        threads = []
        for spec, machine in self.machines.items():
            thread = Thread(target=f(machine))
            thread.daemon = True
            thread.start()
            threads.append(thread)
        for thread in threads:
            thread.join()

    def start(self):
        log.info("Starting service %r with image %r", self.name, self.image)
        self.network = self.docker.networks.create(self.name)
        self._for_each_machine(lambda machine: machine.start)
        self.await_started()

    def await_started(self):

        def wait(machine):
            machine.await_started()

        self._for_each_machine(wait)
        if all(machine.ready == 1 for spec, machine in self.machines.items()):
            log.info("Service %r available", self.name)
        else:
            raise RuntimeError("Service %r unavailable - "
                               "some machines failed", self.name)

    def stop(self):
        log.info("Stopping service %r", self.name)

        def _stop(machine):
            machine.stop()

        self._for_each_machine(_stop)
        if self.network:
            self.network.remove()

    @classmethod
    def find_and_stop(cls, service_name):
        from docker import DockerClient
        docker = DockerClient.from_env(version="auto")
        for container in docker.containers.list(all=True):
            if container.name.endswith(".{}".format(service_name)):
                container.stop()
                container.remove(force=True)
        docker.networks.get(service_name).remove()

    def run_console(self):
        self.console = Neo4jConsole(self)
        self.console.invoke("env")
        self.console.run()

    def env(self):
        addresses = [machine.address for spec, machine in self.machines.items()]
        auth = "{}:{}".format(self.auth.user, self.auth.password)
        return {
            "BOLT_SERVER_ADDR": " ".join(map(str, addresses)),
            "NEO4J_AUTH": auth,
        }


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
        assert topology == "CE"
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

    def yield_uri(self):
        assert self.cert is None
        from py2neo.diagnostics import watch
        from logging import DEBUG
        watch("py2neo.dock", DEBUG)
        service = Neo4jService.single_instance(None, self.release_str, ("neo4j", "password"), None, None)
        service.start()
        try:
            addresses = [machine.addresses[self.scheme]
                         for spec, machine in service.machines.items()]
            uris = ["{}://{}:{}".format(self.scheme, address.host, address.port)
                    for address in addresses]
            yield uris[0]
        finally:
            service.stop()

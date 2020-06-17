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
from logging import getLogger, DEBUG
from os import chmod, path
from random import choice
from shutil import rmtree
from tempfile import mkdtemp
from threading import Thread
from time import sleep
from uuid import uuid4

from docker import DockerClient
from docker.errors import APIError, ImageNotFound

from py2neo.connect.addressing import Address
from py2neo.connect.wire import Wire
from py2neo.dock.console import Neo4jConsole
from py2neo.internal.compat import perf_counter
from py2neo.internal.versioning import Version
from py2neo.security import install_certificate, install_private_key


docker = DockerClient.from_env(version="auto")

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


def random_name(size):
    return "".join(choice("bcdfghjklmnpqrstvwxz") for _ in range(size))


class InstanceProfile(object):
    # Base config for all instances. This can be overridden by
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
            service,
            name,
            bolt_port=None,
            http_port=None,
            https_port=None,
            config=None,
            env=None
    ):
        self.service = service
        self.name = name
        self.bolt_port = bolt_port or 7687
        self.http_port = http_port or 7474
        self.https_port = https_port or 7473
        advertised_address = "localhost:{}".format(self.bolt_port)
        self.config = {
            "dbms.connector.bolt.advertised_address": advertised_address,
        }
        if config:
            self.config.update(**config)
        self.env = dict(env or {})

    def __hash__(self):
        return hash(self.fq_name)

    @property
    def fq_name(self):
        return "{}.{}".format(self.name, self.service.name)

    @property
    def addresses(self):
        addresses = {
            "bolt": Address(("localhost", self.bolt_port)),
            "http": Address(("localhost", self.http_port)),
        }
        if self.service.secured:
            addresses["bolt+s"] = Address(("localhost", self.bolt_port))
            addresses["bolt+ssc"] = Address(("localhost", self.bolt_port))
            addresses["https"] = Address(("localhost", self.https_port))
            addresses["http+s"] = Address(("localhost", self.https_port))
            addresses["http+ssc"] = Address(("localhost", self.https_port))
        return addresses


class Instance(object):
    """ A single Neo4j server instance, potentially part of a cluster.
    """

    container = None

    ip_address = None

    ready = 0

    def __init__(self, profile):
        self.profile = profile
        self.address = self.addresses["bolt"]
        self.cert_volume_dir = None
        self.env = self._create_env(self.profile)
        ports = {"7687/tcp": self.profile.bolt_port,
                 "7474/tcp": self.profile.http_port}
        volumes = {}
        if self.profile.service.secured:
            cert, key = self.profile.service.cert_key_pair
            ports["7473/tcp"] = self.profile.https_port
            self.cert_volume_dir = mkdtemp()
            chmod(self.cert_volume_dir, 0o755)
            log.info("Using directory %r as shared certificate volume", self.cert_volume_dir)
            if self.profile.service.image.version >= Version.parse("4.0"):
                subdirectories = [path.join(self.cert_volume_dir, subdir)
                                  for subdir in ["bolt", "https"]]
                install_certificate(cert, "public.crt", *subdirectories)
                install_private_key(key, "private.key", *subdirectories)
            else:
                install_certificate(cert, "neo4j.cert", self.cert_volume_dir)
                install_private_key(key, "neo4j.key", self.cert_volume_dir)
            volumes[self.cert_volume_dir] = {
                "bind": "/var/lib/neo4j/certificates",
                "mode": "ro",
            }
        self.container = docker.containers.create(
            self.image.id,
            detach=True,
            environment=self.env,
            hostname=self.profile.fq_name,
            name=self.profile.fq_name,
            network=self.profile.service.name,
            ports=ports,
            volumes=volumes,
        )

    def __hash__(self):
        return hash(self.container)

    def __repr__(self):
        return "%s(fq_name={!r}, image={!r}, address={!r})".format(
            self.__class__.__name__, self.profile.fq_name,
            self.image, self.address)

    @classmethod
    def _create_env(cls, profile):
        env = {}

        # Enterprise edition requires license agreement
        # TODO: make this externally explicit, somehow
        if profile.service.image.edition == "enterprise":
            env["NEO4J_ACCEPT_LICENSE_AGREEMENT"] = "yes"

        # Add initial auth details
        if profile.service.auth:
            env["NEO4J_AUTH"] = "/".join(profile.service.auth)

        # General configuration
        config = dict(profile.config)
        if profile.service.secured:
            if profile.service.image.version >= Version.parse("4.0"):
                config.update({
                    "dbms.ssl.policy.bolt.enabled": True,
                    "dbms.ssl.policy.https.enabled": True,
                    "dbms.connector.bolt.tls_level": "OPTIONAL",
                    "dbms.connector.https.enabled": True,
                })
            else:
                pass
        for key, value in config.items():
            fixed_key = "NEO4J_" + key.replace("_", "__").replace(".", "_")
            env[fixed_key] = value

        # Other environment variables
        for key, value in profile.env.items():
            env[key] = value

        return env

    @property
    def image(self):
        return self.profile.service.image

    @property
    def addresses(self):
        return self.profile.addresses

    def start(self):
        log.info("Starting instance %r with image %r",
                 self.profile.fq_name, self.profile.service.image)
        for scheme, address in self.addresses.items():
            log.info("  at <%s://%s>", scheme, address)
        try:
            self.container.start()
            self.container.reload()
            self.ip_address = (self.container.attrs["NetworkSettings"]
                               ["Networks"][self.profile.service.name]["IPAddress"])
        except APIError as e:
            log.info(e)

        log.info("Machine %r is bound to internal IP address %s",
                 self.profile.fq_name, self.ip_address)

    def _poll_bolt_address(self, count=240, interval=0.5, is_running=None):
        address = self.addresses["bolt"]
        t0 = perf_counter()
        for _ in range(count):
            if callable(is_running) and not is_running():
                break
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
                         "after {:.02f}s".format(self.profile.fq_name, t1))
                return True
            finally:
                if wire:
                    wire.close()
        return False

    def _poll_http_address(self, count=240, interval=0.5, is_running=None):
        address = self.addresses["http"]
        t0 = perf_counter()
        for _ in range(count):
            if callable(is_running) and not is_running():
                break
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
                         "after {:.02f}s".format(self.profile.fq_name, t1))
                return True
            finally:
                if wire:
                    wire.close()
        return False

    def await_started(self):
        sleep(1)

        def is_running():
            self.container.reload()
            return self.container.status == "running"

        if is_running():

            result = {}

            def poll_bolt_address():
                result["bolt"] = self._poll_bolt_address(is_running=is_running)

            def poll_http_address():
                result["http"] = self._poll_http_address(is_running=is_running)

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
                              self.profile.fq_name, state["ExitCode"])
                    for line in self.container.logs().splitlines():
                        log.error("> %s" % line.decode("utf-8"))
                else:
                    log.error("Machine %r did not "
                              "become available", self.profile.fq_name)
            else:
                self.ready = 1
        else:
            log.error("Machine %r is not running (status=%r)",
                      self.profile.fq_name, self.container.status)
            for line in self.container.logs().splitlines():
                log.error("> %s" % line.decode("utf-8"))

    def stop(self):
        log.info("Stopping instance %r", self.profile.fq_name)
        self.container.stop()
        self.container.remove(force=True)
        if self.cert_volume_dir:
            log.info("Removing directory %r", self.cert_volume_dir)
            rmtree(self.cert_volume_dir)


class Service(object):
    """ A Neo4j database management service.
    """

    default_bolt_port = 7687
    default_http_port = 7474
    default_https_port = 7473

    @classmethod
    def single_instance(cls, name, image_tag, auth, cert_key_pair=None):
        service = cls(name, image_tag, auth, cert_key_pair)
        profile = InstanceProfile(
            service=service,
            name="a",
            bolt_port=7687,
            http_port=7474,
            https_port=7473,
        )
        service.instances[profile] = Instance(profile)
        return service

    def __init__(self, name, image_tag, auth, cert_key_pair):
        self.name = name or random_name(7)
        self.image = Neo4jImage(image_tag)
        self.auth = Auth(*auth) if auth else make_auth()
        if self.auth.user != "neo4j":
            raise ValueError("Auth user must be 'neo4j' or empty")
        self.cert_key_pair = cert_key_pair
        self.instances = {}
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

    def _get_instance_by_address(self, address):
        address = Address((address.host, address.port_number))
        for profile, instance in self.instances.items():
            if profile.addresses["bolt"] == address:
                return instance

    def _for_each_instance(self, f):
        threads = []
        for profile, instance in self.instances.items():
            thread = Thread(target=f(instance))
            thread.daemon = True
            thread.start()
            threads.append(thread)
        for thread in threads:
            thread.join()

    @property
    def secured(self):
        if self.cert_key_pair is None or self.cert_key_pair == (None, None):
            return False
        else:
            return True

    def start(self):
        # TODO: detect verbosity level
        from py2neo.diagnostics import watch
        watch("py2neo.dock", DEBUG)
        self.network = docker.networks.create(self.name)
        self._for_each_instance(lambda instance: instance.start)
        self.await_started()

    def await_started(self):

        def wait(instance):
            instance.await_started()

        self._for_each_instance(wait)
        if all(instance.ready == 1 for profile, instance in self.instances.items()):
            log.info("Neo4j %s %s service %r available",
                     self.image.edition,
                     ".".join(map(str, self.image.version.major_minor_patch)),
                     self.name)
        else:
            raise RuntimeError("Service %r unavailable - "
                               "some instances failed" % self.name)

    def stop(self):
        log.info("Stopping service %r", self.name)

        def _stop(instance):
            instance.stop()

        self._for_each_instance(_stop)
        if self.network:
            self.network.remove()

    @classmethod
    def find_and_stop(cls, service_name):
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
        addresses = [instance.address for profile, instance in self.instances.items()]
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


class Neo4jImage(object):

    def __init__(self, tag="latest"):
        self._image_tag = self._resolve_image_tag(tag)
        try:
            self._image = docker.images.get(self._image_tag)
        except ImageNotFound:
            log.info("Downloading Docker image %r", self._image_tag)
            self._image = docker.images.pull(self._image_tag)

    def __repr__(self):
        return "Neo4jImage(tag=%r)" % self._image_tag

    @property
    def id(self):
        return self._image.id

    @property
    def tarball(self):
        """ Name of the Neo4j tarball used to build the Docker image
        used by this service.
        """
        for item in self._image.attrs["Config"]["Env"]:
            name, _, value = item.partition("=")
            if name == "NEO4J_TARBALL":
                return value

    @property
    def edition(self):
        """ Edition of Neo4j used to build the Docker image used by
        this service.
        """
        _, edition, _, _ = self.tarball.split("-")
        return edition

    @property
    def version(self):
        """ Version of Neo4j used to build the Docker image used by
        this service.
        """
        _, _, version, _ = self.tarball.split("-")
        return Version.parse(version)

    @classmethod
    def _resolve_image_tag(cls, tag):
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
        resolved = tag
        if resolved.startswith("file:"):
            return cls._load_image_from_file(resolved[5:])
        if ":" not in resolved:
            resolved = "neo4j:" + tag
        return resolved

    @classmethod
    def _load_image_from_file(cls, name):
        with open(name, "rb") as f:
            images = docker.images.load(f.read())
            image = images[0]
            return image.tags[0]

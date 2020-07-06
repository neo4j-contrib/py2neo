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


from argparse import ArgumentParser
from inspect import getdoc
from logging import getLogger
from os import chmod, path
from random import choice
from shlex import split as shlex_split
from shutil import rmtree
from tempfile import mkdtemp
from textwrap import wrap
from threading import Thread
from time import sleep
from webbrowser import open as open_browser

from docker import DockerClient
from docker.errors import APIError, ImageNotFound
from packaging.version import Version
from pansi.console import Console

from py2neo.compat import perf_counter
from py2neo.security import Auth, make_auth, install_certificate, install_private_key
from py2neo.wiring import Address, Wire


docker = DockerClient.from_env(version="auto")

log = getLogger(__name__)


def random_name(size):
    return "".join(choice("bcdfghjklmnpqrstvwxz") for _ in range(size))


class Neo4jInstance(object):
    """ A single Neo4j server instance, potentially part of a cluster.
    """

    container = None

    ip_address = None

    ready = 0

    def __init__(self, service, name, bolt_port=None, http_port=None, https_port=None):
        self.service = service
        self.name = name
        self.bolt_port = bolt_port
        self.http_port = http_port
        self.https_port = https_port
        self.address = self.addresses["bolt"]
        self.cert_volume_dir = None
        self.config = self._create_config(self.service)
        self.env = self._create_env(self.service)
        ports = {"7687/tcp": self.bolt_port,
                 "7474/tcp": self.http_port}
        volumes = {}
        if self.service.secured:
            cert, key = self.service.cert_key_pair
            ports["7473/tcp"] = self.https_port
            self.cert_volume_dir = mkdtemp()
            chmod(self.cert_volume_dir, 0o755)
            log.debug("Using directory %r as shared certificate volume", self.cert_volume_dir)
            if self.service.image.version >= Version("4.0"):
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
            hostname=self.fq_name,
            name=self.fq_name,
            network=self.service.name,
            ports=ports,
            volumes=volumes,
        )

    def __hash__(self):
        return hash(self.container)

    def __repr__(self):
        return "%s(fq_name={!r}, image={!r}, address={!r})".format(
            self.__class__.__name__, self.fq_name,
            self.image, self.address)

    def _create_config(self, service):
        config = {
            "dbms.backup.enabled": "false",
            "dbms.connector.bolt.advertised_address": "localhost:{}".format(self.bolt_port),
            "dbms.memory.heap.initial_size": "300m",
            "dbms.memory.heap.max_size": "500m",
            "dbms.memory.pagecache.size": "50m",
            "dbms.transaction.bookmark_ready_timeout": "5s",
        }

        # Security configuration
        if service.secured:
            if service.image.version >= Version("4.0"):
                config.update({
                    "dbms.ssl.policy.bolt.enabled": True,
                    "dbms.ssl.policy.https.enabled": True,
                    "dbms.connector.bolt.tls_level": "OPTIONAL",
                    "dbms.connector.https.enabled": True,
                })
            else:
                pass

        return config

    def _create_env(self, service):
        env = {}

        # Enterprise edition requires license agreement
        # TODO: make this externally explicit, somehow
        if service.image.edition == "enterprise":
            env["NEO4J_ACCEPT_LICENSE_AGREEMENT"] = "yes"

        # Add initial auth details
        if service.auth:
            env["NEO4J_AUTH"] = "/".join(service.auth)

        # Add config
        for key, value in self.config.items():
            fixed_key = "NEO4J_" + key.replace("_", "__").replace(".", "_")
            env[fixed_key] = value

        return env

    @property
    def fq_name(self):
        return "{}.{}".format(self.name, self.service.name)

    @property
    def image(self):
        return self.service.image

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

    def start(self):
        log.info("Starting instance %r with image %r",
                 self.fq_name, self.service.image)
        for scheme, address in self.addresses.items():
            log.info("  at <%s://%s>", scheme, address)
        try:
            self.container.start()
            self.container.reload()
            self.ip_address = (self.container.attrs["NetworkSettings"]
                               ["Networks"][self.service.name]["IPAddress"])
        except APIError as e:
            log.exception(e)

        log.debug("Machine %r is bound to internal IP address %s",
                  self.fq_name, self.ip_address)

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
                           b"\x00\x00\x01\x04"
                           b"\x00\x00\x00\x04"
                           b"\x00\x00\x00\x03"
                           b"\x00\x00\x00\x02")
                wire.send()
                data = wire.read(4)
            except OSError:
                sleep(interval)
            else:
                t1 = perf_counter() - t0
                log.info("Machine {!r} available "
                         "for Bolt traffic "
                         "after {:.02f}s".format(self.fq_name, t1))
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
                         "after {:.02f}s".format(self.fq_name, t1))
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
                              self.fq_name, state["ExitCode"])
                    for line in self.container.logs().splitlines():
                        log.error("> %s" % line.decode("utf-8"))
                else:
                    log.error("Machine %r did not "
                              "become available", self.fq_name)
            else:
                self.ready = 1
        else:
            log.error("Machine %r is not running (status=%r)",
                      self.fq_name, self.container.status)
            for line in self.container.logs().splitlines():
                log.error("> %s" % line.decode("utf-8"))

    def stop(self):
        log.info("Stopping instance %r", self.fq_name)
        self.container.stop()
        self.container.remove(force=True)
        if self.cert_volume_dir:
            log.debug("Removing directory %r", self.cert_volume_dir)
            rmtree(self.cert_volume_dir)


class Neo4jService(object):
    """ A Neo4j database management service.
    """

    default_bolt_port = 7687
    default_http_port = 7474
    default_https_port = 7473

    @classmethod
    def single_instance(cls, name, image_tag, auth, cert_key_pair=None):
        service = cls(name, image_tag, auth, cert_key_pair)
        ports = {
            "bolt_port": 7687,
            "http_port": 7474,
        }
        if service.secured:
            ports["https_port"] = 7473
        service.instances.append(Neo4jInstance(service, "a", **ports))
        return service

    def __init__(self, name, image_tag, auth, cert_key_pair):
        self.name = name or random_name(7)
        self.image = Neo4jImage(image_tag)
        self.auth = Auth(*auth) if auth else make_auth()
        if self.auth.user != "neo4j":
            raise ValueError("Auth user must be 'neo4j' or empty")
        self.cert_key_pair = cert_key_pair
        self.instances = []
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
        for instance in self.instances:
            if instance.addresses["bolt"] == address:
                return instance

    def _for_each_instance(self, f):
        threads = []
        for instance in self.instances:
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
        self.network = docker.networks.create(self.name)
        self._for_each_instance(lambda instance: instance.start)
        self.await_started()

    def await_started(self):

        def wait(instance):
            instance.await_started()

        self._for_each_instance(wait)
        if all(instance.ready == 1 for instance in self.instances):
            log.info("Neo4j %s %s service %r available",
                     self.image.edition.title(), self.image.version, self.name)
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

    def env(self):
        addresses = [instance.address for instance in self.instances]
        auth = "{}:{}".format(self.auth.user, self.auth.password)
        return {
            "BOLT_SERVER_ADDR": " ".join(map(str, addresses)),
            "NEO4J_AUTH": auth,
        }


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
        return Version(version)

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


class _ConsoleArgumentParser(ArgumentParser):

    def __init__(self, prog=None, **kwargs):
        kwargs["add_help"] = False
        super(_ConsoleArgumentParser, self).__init__(prog, **kwargs)

    def exit(self, status=0, message=None):
        pass    # TODO

    def error(self, message):
        raise _ConsoleCommandError(message)


class _ConsoleCommandError(Exception):

    pass


class _CommandConsole(Console):

    def __init__(self, name, verbosity=None, history=None, time_format=None):
        super(_CommandConsole, self).__init__(name, verbosity=verbosity,
                                              history=history, time_format=time_format)
        self.__parser = _ConsoleArgumentParser(self.name)
        self.__command_parsers = self.__parser.add_subparsers()
        self.__commands = {}
        self.__usages = {}
        self.add_command("help", self.help)
        self.add_command("exit", self.exit)

    def add_command(self, name, f):
        parser = self.__command_parsers.add_parser(name, add_help=False)
        try:
            from inspect import getfullargspec
        except ImportError:
            # Python 2
            from inspect import getargspec
            spec = getargspec(f)
        else:
            # Python 3
            spec = getfullargspec(f)
        args, default_values = spec[0], spec[3]
        if default_values:
            n_defaults = len(default_values)
            defaults = dict(zip(args[-n_defaults:], default_values))
        else:
            defaults = {}
        usage = []
        for i, arg in enumerate(args):
            if i == 0 and arg == "self":
                continue
            if arg in defaults:
                parser.add_argument(arg, nargs="?", default=defaults[arg])
                usage.append("[%s]" % arg)
            else:
                parser.add_argument(arg)
                usage.append(arg)
        parser.set_defaults(f=f)
        self.__commands[name] = parser
        self.__usages[name] = usage

    def process(self, line):
        """ Handle input.
        """

        # Lex
        try:
            tokens = shlex_split(line)
        except ValueError as error:
            self.error("Syntax error (%s)", error.args[0])
            return 2

        # Parse
        try:
            args = self.__parser.parse_args(tokens)
        except _ConsoleCommandError as error:
            if tokens[0] in self.__commands:
                # misused
                self.error(error)
                return 1
            else:
                # unknown
                self.error(error)
                return 127

        # Dispatch
        kwargs = vars(args)
        f = kwargs.pop("f")
        return f(**kwargs) or 0

    def help(self, command=None):
        """ Show general or command-specific help.
        """
        if command:
            try:
                parser = self.__commands[command]
            except KeyError:
                self.error("No such command %r", command)
                raise RuntimeError('No such command "%s".' % command)
            else:
                parts = ["usage:", command] + self.__usages[command]
                self.write(" ".join(parts))
                self.write()
                f = parser.get_default("f")
                doc = getdoc(f)
                self.write(doc.rstrip())
                self.write()
        else:
            self.write("Commands:")
            command_width = max(map(len, self.__commands))
            template = "  {:<%d}   {}" % command_width
            for name in sorted(self.__commands):
                parser = self.__commands[name]
                f = parser.get_default("f")
                doc = getdoc(f)
                lines = wrap(first_sentence(doc), 73 - command_width)
                for i, line in enumerate(lines):
                    if i == 0:
                        self.write(template.format(name, line))
                    else:
                        self.write(template.format("", line))
            self.write()

    def exit(self):
        """ Exit the console.
        """
        super(_CommandConsole, self).exit()


class Neo4jConsole(_CommandConsole):

    args = None

    service = None

    def __init__(self):
        super(Neo4jConsole, self).__init__(__name__)  # TODO: history file
        self.add_command("browser", self.browser)
        self.add_command("env", self.env)
        self.add_command("ls", self.ls)
        self.add_command("logs", self.logs)

    def _iter_instances(self, name):
        if not name:
            name = "a"
        for instance in self.service.instances:
            if name in (instance.name, instance.fq_name):
                yield instance

    def _for_each_instance(self, name, f):
        found = 0
        for instance_obj in self._iter_instances(name):
            f(instance_obj)
            found += 1
        return found

    def browser(self, instance="a"):
        """ Start the Neo4j browser.

        A machine name may optionally be passed, which denotes the server to
        which the browser should be tied. If no machine name is given, 'a' is
        assumed.
        """

        def f(i):
            try:
                uri = "https://{}".format(i.addresses["https"])
            except KeyError:
                uri = "http://{}".format(i.addresses["http"])
            log.info("Opening web browser for machine %r at %r", i.fq_name, uri)
            open_browser(uri)

        if not self._for_each_instance(instance, f):
            raise RuntimeError("Machine {!r} not found".format(instance))

    def env(self):
        """ Show available environment variables.

        Each service exposes several environment variables which contain
        information relevant to that service. These are:

          BOLT_SERVER_ADDR   space-separated string of router addresses
          NEO4J_AUTH         colon-separated user and password

        """
        for key, value in sorted(self.service.env().items()):
            log.info("%s=%r", key, value)

    def ls(self):
        """ Show server details.
        """
        self.write("CONTAINER   NAME        "
                   "BOLT PORT   HTTP PORT   HTTPS PORT   MODE")
        for instance in self.service.instances:
            if instance is None:
                continue
            self.write("{:<12}{:<12}{:<12}{:<12}{:<13}{:<15}".format(
                instance.container.short_id,
                instance.fq_name,
                instance.bolt_port,
                instance.http_port,
                instance.https_port or 0,
                instance.config.get("dbms.mode", "SINGLE"),
            ))

    def logs(self, instance="a"):
        """ Display server logs.

        If no server name is provided, 'a' is used as a default.
        """

        def f(m):
            self.write(m.container.logs().decode("utf-8"))

        if not self._for_each_instance(instance, f):
            self.error("Machine %r not found", instance)
            return 1


def first_sentence(text):
    """ Extract the first sentence of a text.
    """
    if not text:
        return ""

    from re import match
    lines = text.splitlines(keepends=False)
    one_line = " ".join(lines)
    matched = match(r"^(.*?(?<!\b\w)[.?!])\s+[A-Z0-9]", one_line)
    if matched:
        return matched.group(1)
    else:
        return lines[0]

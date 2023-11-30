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


from __future__ import division

from collections import namedtuple
from inspect import getmembers
from logging import getLogger
from math import ceil
from os import makedirs
from os.path import isdir, join as path_join
from random import choice
from shlex import split as shlex_split
from threading import Thread
from time import sleep
from xml.etree import ElementTree
from webbrowser import open as open_browser

import click
from click import BadParameter, ClickException
from docker import DockerClient
from docker.errors import APIError, ImageNotFound
from monotonic import monotonic
from py2neo import ServiceProfile, ConnectionProfile, ConnectionUnavailable
from py2neo.addressing import Address
from py2neo.client import Connector, Connection
from packaging.version import InvalidVersion

from six.moves import input

from grolt.images import is_legacy_image, resolve_image
from grolt.security import Auth, make_auth


docker = DockerClient.from_env(version="auto")

log = getLogger(__name__)

debug_opts_type = namedtuple("debug_opts_type", ["suspend", "port"])


def port_range(base_port, count):
    if base_port:
        return list(range(base_port, base_port + count))
    else:
        return [0] * count


class Neo4jDirectorySpec(object):

    def __init__(self,
                 certificates_dir=None,
                 import_dir=None,
                 logs_dir=None,
                 plugins_dir=None,
                 shared_dirs=None,
                 neo4j_source_dir=None,
                 ):
        self.certificates_dir = certificates_dir
        self.import_dir = import_dir
        self.logs_dir = logs_dir
        self.plugins_dir = plugins_dir
        self.shared_dirs = shared_dirs
        self.neo4j_source_dir = neo4j_source_dir

    def volumes(self, name):
        volumes = {}
        if self.certificates_dir:
            # The certificate directory needs to be shared as rw to
            # allow Neo4j to perform 'chown'.
            log.debug("Sharing directory %r for certificates (rw)", self.certificates_dir)
            volumes[self.certificates_dir] = {
                "bind": "/var/lib/neo4j/certificates",
                "mode": "rw",
            }
        if self.import_dir:
            log.debug("Sharing directory %r for imports (ro)", self.import_dir)
            volumes[self.import_dir] = {
                "bind": "/var/lib/neo4j/import",
                "mode": "ro",
            }
        if self.logs_dir:
            log.debug("Sharing directory %r for logs (rw)", self.logs_dir)
            volumes[path_join(self.logs_dir, name)] = {
                "bind": "/var/lib/neo4j/logs",
                "mode": "rw",
            }
        if self.plugins_dir:
            log.debug("Sharing directory %r for plugins (ro)", self.plugins_dir)
            volumes[self.plugins_dir] = {
                "bind": "/plugins",
                "mode": "ro",
            }
        if self.shared_dirs:
            for shared_dir in self.shared_dirs:
                log.debug("Sharing directory %r as %r", shared_dir.source, shared_dir.destination)
                volumes[shared_dir.source] = {
                    "bind": shared_dir.destination,
                    "mode": "rw",
                }
        if self.neo4j_source_dir:
            pom = ElementTree.parse(self.neo4j_source_dir + "/pom.xml").getroot()
            xml_tag_prefix = pom.tag.split("project")[0]
            neo4j_version = pom.find(xml_tag_prefix+"version").text
            lib_dir = ("{}/private/packaging/standalone/target/"
                       "neo4j-enterprise-{}-unix/neo4j-enterprise-{}/"
                       "lib".format(self.neo4j_source_dir, neo4j_version, neo4j_version))
            bin_dir = ("{}/private/packaging/standalone/target/"
                       "neo4j-enterprise-{}-unix/neo4j-enterprise-{}/"
                       "bin".format(self.neo4j_source_dir, neo4j_version, neo4j_version))
            if not isdir(lib_dir):
                raise Exception("Could not find packaged neo4j source at {}\n"
                                "Perhaps you need to run `mvn package`?".format(lib_dir))

            volumes[lib_dir] = {
                "bind": "/var/lib/neo4j/lib/",
                "mode": "ro",
            }
            volumes[bin_dir] = {
                "bind": "/var/lib/neo4j/bin/",
                "mode": "ro",
            }

        return volumes


class Neo4jMachineSpec(object):
    # Base config for all machines. This can be overridden by
    # individual instances.
    config = {
        "dbms.backup.enabled": "false",
        "dbms.transaction.bookmark_ready_timeout": "5s",
    }

    discovery_port = 5000
    transaction_port = 6000
    raft_port = 7000
    debug_port = 5100
    bolt_internal_port = 7688

    def __init__(self, name, service_name, image,
                 bolt_port, http_port, https_port, debug_opts,
                 dir_spec, config, env):
        self.name = name
        self.service_name = service_name
        self.image = image
        self.bolt_port = bolt_port
        self.http_port = http_port
        self.https_port = https_port
        self.dir_spec = dir_spec
        self.debug_opts = debug_opts
        self.env = dict(env or {})
        self.config = dict(self.config or {})
        if debug_opts.port:
            self._add_debug_opts(debug_opts)
        self.config["dbms.connector.bolt.advertised_address"] = \
            "localhost:{}".format(self.bolt_port)
        self.config["dbms.connector.http.advertised_address"] = \
            "localhost:{}".format(self.http_port)
        self.config["dbms.connector.https.advertised_address"] = \
            "localhost:{}".format(self.https_port)
        self.config["dbms.routing.advertised_address"] = \
            self.bolt_internal_address
        if self.dir_spec and self.dir_spec.certificates_dir and not is_legacy_image(self.image):
            self.config.update({
                "dbms.ssl.policy.bolt.enabled": True,
                "dbms.ssl.policy.https.enabled": True,
                "dbms.connector.bolt.tls_level": "OPTIONAL",
                "dbms.connector.https.enabled": True,
            })
        if config:
            self.config.update(**config)

    def __hash__(self):
        return hash((self.name, self.service_name))

    @property
    def dbms_mode(self):
        return self.config.get("dbms.mode")

    @property
    def fq_name(self):
        return "{}.{}".format(self.name, self.service_name)

    @property
    def discovery_address(self):
        return "{}:{}".format(self.fq_name, self.discovery_port)

    @property
    def bolt_internal_address(self):
        return "{}:{}".format(self.fq_name, self.bolt_internal_port)

    def _add_debug_opts(self, debug_opts):
        if debug_opts.port is not None:
            suspend = "y" if debug_opts.suspend else "n"
            self.env["JAVA_TOOL_OPTIONS"] = (
                "-agentlib:jdwp=transport=dt_socket,server=y,"
                "suspend={},address=*:{}".format(suspend, self.debug_port)
            )


class Neo4jMachine(object):
    """ A single Neo4j server instance, potentially part of a cluster.
    """

    container = None

    ip_address = None

    ready = 0

    def __init__(self, spec, image, auth, user):
        self.spec = spec
        self.image = image
        self.address = Address(("localhost", self.spec.bolt_port))
        self.auth = auth
        self.profiles = {
            "bolt": ConnectionProfile(scheme="bolt", port=self.spec.bolt_port, auth=self.auth),
            "http": ConnectionProfile(scheme="http", port=self.spec.http_port, auth=self.auth),
            "https": ConnectionProfile(scheme="https", port=self.spec.https_port, auth=self.auth),
        }
        environment = {}
        if self.auth:
            environment["NEO4J_AUTH"] = "/".join(self.auth)
        environment["NEO4J_ACCEPT_LICENSE_AGREEMENT"] = "yes"
        for key, value in self.spec.config.items():
            fixed_key = "NEO4J_" + key.replace("_", "__").replace(".", "_")
            environment[fixed_key] = value
        for key, value in self.spec.env.items():
            environment[key] = value
        ports = {"7474/tcp": self.spec.http_port,
                 "7473/tcp": self.spec.https_port,
                 "7687/tcp": self.spec.bolt_port}
        if self.spec.debug_opts.port is not None:
            ports["5100/tcp"] = self.spec.debug_opts.port
        if self.spec.dir_spec:
            volumes = self.spec.dir_spec.volumes(self.spec.name)
            for path in volumes:
                try:
                    makedirs(path)
                except OSError:
                    pass
        else:
            volumes = None
        try:
            user = int(user)
        except TypeError:
            user = None
        except ValueError:
            # Note: this will only work on Unix.
            from pwd import getpwnam
            user = getpwnam(user).pw_uid

        def create_container(img):
            return docker.containers.create(
                img,
                detach=True,
                environment=environment,
                hostname=self.spec.fq_name,
                name=self.spec.fq_name,
                network=self.spec.service_name,
                ports=ports,
                user=user,
                volumes=volumes,
            )

        try:
            self.container = create_container(self.image)
        except ImageNotFound:
            log.info("Downloading Docker image %r", self.image)
            docker.images.pull(self.image)
            self.container = create_container(self.image)

    def __hash__(self):
        return hash(self.container)

    def __repr__(self):
        return "%s(fq_name={!r}, image={!r}, address={!r})".format(
            self.__class__.__name__, self.spec.fq_name,
            self.image, self.address)

    def start(self):
        log.info("Starting machine %r at "
                 "«%s»", self.spec.fq_name, self.address)
        try:
            self.container.start()
            self.container.reload()
            self.ip_address = (self.container.attrs["NetworkSettings"]
                               ["Networks"][self.spec.service_name]["IPAddress"])
        except APIError as error:
            log.info(error)

        log.debug(u"Machine %r has internal IP address "
                  u"«%s»", self.spec.fq_name, self.ip_address)

    def restart(self):
        log.info("Restarting machine %r at "
                 "«%s»", self.spec.fq_name, self.address)
        try:
            self.container.restart()
            self.container.reload()
            self.ip_address = (self.container.attrs["NetworkSettings"]
                               ["Networks"][self.spec.service_name]["IPAddress"])
        except APIError as error:
            log.info(error)

        log.debug("Machine %r has internal IP address "
                  "«%s»", self.spec.fq_name, self.ip_address)

    def _poll_connection(self, port_name, timeout=0):
        """ Repeatedly attempt to open a connection to a Bolt server.
        """
        t0 = monotonic()
        profile = self.profiles[port_name]
        log.debug("Trying to open connection to %s", profile)
        errors = set()
        again = True
        wait = 0.1
        while again:
            try:
                cx = Connection.open(profile)
            except InvalidVersion as e:
                log.info("Encountered invalid Neo4j version '%s'. Continuing anyway (this is a dev tool)", e)
                return None
            except ConnectionUnavailable as e:
                errors.add(" ".join(map(str, e.args)))
            else:
                if cx:
                    return cx
            again = monotonic() - t0 < (timeout or 0)
            if again:
                sleep(wait)
                wait *= 2
        log.error("Could not open connection to %s (%r)", profile, errors)
        raise ConnectionUnavailable("Could not open connection")

    def ping(self, timeout):
        try:
            cx = self._poll_connection("bolt", timeout=timeout)
            if cx is not None:
                cx.close()
            cx = self._poll_connection("http", timeout=timeout)
            if cx is not None:
                cx.close()
            log.info("Machine {!r} available".format(self.spec.fq_name))
        except ConnectionUnavailable:
            log.info("Machine {!r} unavailable".format(self.spec.fq_name))

    def await_started(self, timeout):
        sleep(1)
        self.container.reload()
        if self.container.status == "running":
            try:
                self.ping(timeout)
            except OSError:
                self.container.reload()
                state = self.container.attrs["State"]
                if state["Status"] == "exited":
                    self.ready = -1
                    log.error("Machine %r exited with code %r",
                              self.spec.fq_name, state["ExitCode"])
                    for line in self.container.logs().splitlines():
                        log.error("> %s" % line.decode("utf-8"))
                else:
                    log.error("Machine %r did not become available "
                              "within %rs", self.spec.fq_name, timeout)
            else:
                self.ready = 1
        else:
            log.error("Machine %r is not running (status=%r)",
                      self.spec.fq_name, self.container.status)
            for line in self.container.logs().splitlines():
                log.error("> %s" % line.decode("utf-8"))

    def stop(self, timeout=None):
        log.info("Stopping machine %r", self.spec.fq_name)
        self.container.stop(timeout=timeout)
        self.container.remove(force=True)

    def uri(self, scheme):
        """ Return a URI targeting this machine for a given URI scheme.
        """
        if scheme in ("neo4j", "neo4j+s", "neo4j+ssc", "bolt", "bolt+s", "bolt+ssc"):
            port = self.spec.bolt_port
        elif scheme == "http":
            port = self.spec.http_port
        elif scheme in ("https", "http+s", "http+ssc"):
            port = self.spec.https_port
        else:
            raise ValueError("Unsupported URI scheme %r", scheme)
        return "{}://localhost:{}".format(scheme, port)


class Neo4jService(object):
    """ A Neo4j database management service.
    """

    default_image = NotImplemented

    default_bolt_port = 7687
    default_http_port = 7474
    default_https_port = 7473
    default_debug_port = 5005

    def __new__(cls, name=None, image=None, auth=None, user=None,
                n_cores=None, n_replicas=None,
                bolt_port=None, http_port=None, https_port=None,
                debug_port=None, debug_suspend=None,
                dir_spec=None, config=None, env=None):
        if n_cores:
            return object.__new__(Neo4jClusterService)
        else:
            return object.__new__(Neo4jStandaloneService)

    @classmethod
    def _random_name(cls):
        return "".join(choice("bcdfghjklmnpqrstvwxz") for _ in range(7))

    # noinspection PyUnusedLocal
    def __init__(self, name=None, image=None, auth=None, user=None,
                 n_cores=None, n_replicas=None,
                 bolt_port=None, http_port=None, https_port=None,
                 debug_port=None, debug_suspend=None, dir_spec=None,
                 config=None, env=None):
        self.name = name or self._random_name()
        self.image = resolve_image(image or self.default_image)
        self.auth = Auth(*auth) if auth else make_auth()
        if self.auth.user != "neo4j":
            raise ValueError("Auth user must be 'neo4j' or empty")
        self.user = user
        self.machines = {}
        self.network = None
        self.console = None

    def __enter__(self):
        try:
            self.start(timeout=300)
        except KeyboardInterrupt:
            self.stop(timeout=300)
            raise
        else:
            return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()

    def boot(self):
        for spec, machine in self.machines.items():
            if machine is None:
                self.machines[spec] = Neo4jMachine(spec, self.image, self.auth, self.user)

    def routers(self):
        return list(self.machines.values())

    def _for_each_machine(self, f):
        threads = []
        for spec, machine in self.machines.items():
            thread = Thread(target=f(machine))
            thread.daemon = True
            thread.start()
            threads.append(thread)
        for thread in threads:
            thread.join()

    def start(self, timeout=None):
        log.info("Starting service %r with image %r", self.name, self.image)
        self.network = docker.networks.create(self.name)
        self._for_each_machine(lambda machine: machine.start)
        if timeout is not None:
            self.await_started(timeout)

    def await_started(self, timeout):

        def wait(machine):
            machine.await_started(timeout=timeout)

        self._for_each_machine(wait)
        if all(machine.ready == 1 for spec, machine in self.machines.items()):
            log.info("Service %r available", self.name)
        else:
            raise RuntimeError(("Service %r unavailable - "
                                "some machines failed") % self.name)

    def stop(self, timeout=None):
        log.info("Stopping service %r", self.name)

        def _stop(machine):
            machine.stop(timeout)

        self._for_each_machine(_stop)
        if self.network:
            self.network.remove()

    def run_console(self):
        self.console = Neo4jConsole(self)
        self.console.invoke("env")
        self.console.run()

    def env(self):
        auth = "{}:{}".format(self.auth.user, self.auth.password)
        return {
            "BOLT_SERVER_ADDR": " ".join(str(router.address) for router in self.routers()),
            "NEO4J_AUTH": auth,
        }


class Neo4jStandaloneService(Neo4jService):
    default_image = "neo4j:latest"

    def __init__(self, name=None, image=None, auth=None, user=None,
                 n_cores=None, n_replicas=None,
                 bolt_port=None, http_port=None, https_port=None, debug_port=None,
                 debug_suspend=None, dir_spec=None, config=None, env=None):
        super(Neo4jStandaloneService, self).__init__(name, image, auth, user,  n_cores, n_replicas,
                                                     bolt_port, http_port, https_port, dir_spec,
                                                     config, env)
        spec = Neo4jMachineSpec(
            name="a",
            service_name=self.name,
            image=self.image,
            bolt_port=bolt_port or self.default_bolt_port,
            http_port=http_port or self.default_http_port,
            https_port=https_port or self.default_https_port,
            debug_opts=debug_opts_type(debug_suspend, debug_port),
            dir_spec=dir_spec,
            config=config,
            env=env,
        )
        self.machines[spec] = None
        self.boot()


class Neo4jClusterService(Neo4jService):
    default_image = "neo4j:enterprise"

    # The minimum and maximum number of cores permitted
    min_cores = 3
    max_cores = 7

    # The minimum and maximum number of read replicas permitted
    min_replicas = 0
    max_replicas = 9

    default_bolt_port = 17601
    default_http_port = 17401
    default_https_port = 17301
    default_debug_port = 15001

    def __init__(self, name=None, image=None, auth=None, user=None,
                 n_cores=None, n_replicas=None,
                 bolt_port=None, http_port=None, https_port=None, debug_port=None,
                 debug_suspend=None, dir_spec=None, config=None, env=None):
        super(Neo4jClusterService, self).__init__(name, image, auth, user, n_cores, n_replicas,
                                                  bolt_port, http_port, https_port, debug_port,
                                                  debug_suspend, dir_spec, config, env)
        n_cores = n_cores or self.min_cores
        n_replicas = n_replicas or self.min_replicas
        if not self.min_cores <= n_cores <= self.max_cores:
            raise ValueError("A cluster must have been {} and {} "
                             "cores".format(self.min_cores, self.max_cores))
        if not self.min_replicas <= n_replicas <= self.max_replicas:
            raise ValueError("A cluster must have been {} and {} "
                             "read replicas".format(self.min_replicas,
                                                    self.max_replicas))

        core_bolt_port_range = port_range(
            bolt_port or self.default_bolt_port, self.max_cores)
        core_http_port_range = port_range(
            http_port or self.default_http_port, self.max_cores)
        core_https_port_range = port_range(
            https_port or self.default_https_port, self.max_cores)
        core_debug_port_range = port_range(debug_port, self.max_cores)
        self.free_core_machine_specs = [
            Neo4jMachineSpec(
                name=chr(97 + i),
                service_name=self.name,
                image=self.image,
                bolt_port=core_bolt_port_range[i],
                http_port=core_http_port_range[i],
                https_port=core_https_port_range[i],
                # Only suspend first core in cluster, otherwise cluster won't form until debuggers
                # connect to all of them.
                debug_opts=debug_opts_type(debug_suspend if i == 0 else False,
                                           core_debug_port_range[i]),
                dir_spec=dir_spec,
                config=dict(config or {}, **{
                    "dbms.mode": "CORE",
                    "causal_clustering.minimum_core_cluster_size_at_formation":
                        n_cores or self.min_cores,
                    "causal_clustering.minimum_core_cluster_size_at_runtime":
                        self.min_cores,
                }),
                env=env,
            )
            for i in range(self.max_cores)
        ]
        replica_bolt_port_range = port_range(
            ceil(core_bolt_port_range[-1] / 10) * 10 + 1, self.max_replicas)
        replica_http_port_range = port_range(
            ceil(core_http_port_range[-1] / 10) * 10 + 1, self.max_replicas)
        replica_https_port_range = port_range(
            ceil(core_https_port_range[-1] / 10) * 10 + 1, self.max_replicas)
        if debug_port:
            replica_debug_port_range = port_range(
                ceil(core_debug_port_range[-1] / 10) * 10 + 1, self.max_replicas)
        else:
            replica_debug_port_range = port_range(None, self.max_replicas)
        self.free_replica_machine_specs = [
            Neo4jMachineSpec(
                name=chr(49 + i),
                service_name=self.name,
                image=self.image,
                bolt_port=replica_bolt_port_range[i],
                http_port=replica_http_port_range[i],
                https_port=replica_https_port_range[i],
                # Only suspend first core in cluster, otherwise cluster won't form until debuggers
                # connect to all of them.
                debug_opts=debug_opts_type(debug_suspend if i == 0 else False,
                                           replica_debug_port_range[i]),
                dir_spec=dir_spec,
                config=dict(config or {}, **{
                    "dbms.mode": "READ_REPLICA",
                }),
                env=env,
            )
            for i in range(self.max_replicas)
        ]

        # Add core machine specs
        for i in range(n_cores or self.min_cores):
            spec = self.free_core_machine_specs.pop(0)
            self.machines[spec] = None
        # Add replica machine specs
        for i in range(n_replicas or self.min_replicas):
            spec = self.free_replica_machine_specs.pop(0)
            self.machines[spec] = None

        self.boot()

    def boot(self):
        discovery_addresses = [spec.discovery_address for spec in self.machines
                               if spec.dbms_mode == "CORE"]
        log.debug("Discovery addresses set to %r" % discovery_addresses)
        for spec, machine in self.machines.items():
            if machine is None:
                spec.config.update({
                    "causal_clustering.initial_discovery_members":
                        ",".join(discovery_addresses),
                })
                self.machines[spec] = Neo4jMachine(spec, self.image, self.auth, self.user)

    def cores(self):
        return [machine for spec, machine in self.machines.items()
                if spec.dbms_mode == "CORE"]

    def replicas(self):
        return [machine for spec, machine in self.machines.items()
                if spec.dbms_mode == "READ_REPLICA"]

    def routers(self):
        return list(self.cores())

    def run_console(self):
        self.console = Neo4jClusterConsole(self)
        self.console.run()

    def add_core(self):
        """ Add new core server
        """
        if len(self.cores()) < self.max_cores:
            spec = self.free_core_machine_specs.pop(0)
            self.machines[spec] = None
            self.boot()
            self.machines[spec].start()
            self.machines[spec].await_started(300)
        else:
            raise RuntimeError("A maximum of {} cores "
                               "is permitted".format(self.max_cores))

    def add_replica(self):
        """ Add new replica server
        """
        if len(self.replicas()) < self.max_replicas:
            spec = self.free_replica_machine_specs.pop(0)
            self.machines[spec] = None
            self.boot()
            self.machines[spec].start()
            self.machines[spec].await_started(300)
        else:
            raise RuntimeError("A maximum of {} replicas "
                               "is permitted".format(self.max_replicas))

    def _remove_machine(self, spec):
        machine = self.machines[spec]
        del self.machines[spec]
        machine.stop()
        if spec.dbms_mode == "CORE":
            self.free_core_machine_specs.append(spec)
        elif spec.dbms_mode == "READ_REPLICA":
            self.free_replica_machine_specs.append(spec)

    def remove(self, name):
        """ Remove a server by name (e.g. 'a', 'a.fbe340d').
        """
        found = 0
        for spec, machine in list(self.machines.items()):
            if name in (spec.name, spec.fq_name):
                self._remove_machine(spec)
                found += 1
        return found

    def reboot(self, name):
        found = 0
        for spec, machine in list(self.machines.items()):
            if name in (spec.name, spec.fq_name):
                machine.restart()
                machine.await_started(300)
                found += 1
        return found


class Neo4jConsole(object):

    args = None

    def __init__(self, service):
        self.service = service

    def __iter__(self):
        for name, value in getmembers(self):
            if isinstance(value, click.Command):
                yield name

    def __getitem__(self, name):
        try:
            f = getattr(self, name)
        except AttributeError:
            raise BadParameter('No such command "%s".' % name)
        else:
            if isinstance(f, click.Command):
                return f
            else:
                raise BadParameter('No such command "%s".' % name)

    def _iter_machines(self, name):
        if not name:
            name = "a"
        for spec in list(self.service.machines):
            if name in (spec.name, spec.fq_name):
                yield self.service.machines[spec]

    def _for_each_machine(self, name, f):
        found = 0
        for machine_obj in self._iter_machines(name):
            f(machine_obj)
            found += 1
        return found

    def prompt(self):
        # We don't use click.prompt functionality here as that doesn't play
        # nicely with readline. Instead, we use click.echo for the main prompt
        # text and a raw input call to read from stdin.
        text = "".join([
            click.style(self.service.name, fg="green"),
            click.style(">"),
        ])
        prompt_suffix = " "
        click.echo(text, nl=False)
        return input(prompt_suffix)

    def run(self):
        while True:
            text = self.prompt()
            if text:
                self.args = shlex_split(text)
                self.invoke(*self.args)

    def invoke(self, *args):
        try:
            arg0, args = args[0], list(args[1:])
            f = self[arg0]
            ctx = f.make_context(arg0, args, obj=self)
            return f.invoke(ctx)
        except click.exceptions.Exit:
            pass
        except ClickException as error:
            click.echo(error.format_message(), err=True)

    @click.command()
    @click.argument("machine", required=False)
    @click.pass_obj
    def browser(self, machine):
        """ Start the Neo4j browser.

        A machine name may optionally be passed, which denotes the server to
        which the browser should be tied. If no machine name is given, 'a' is
        assumed.
        """

        def f(m):
            http_uri = m.uri("http")
            click.echo("Opening web browser for machine {!r} at "
                       "«{}»".format(m.spec.fq_name, http_uri))
            open_browser(http_uri)

        if not self._for_each_machine(machine, f):
            raise BadParameter("Machine {!r} not found".format(machine))

    @click.command()
    @click.pass_obj
    def env(self):
        """ Show available environment variables.

        Each service exposes several environment variables which contain
        information relevant to that service. These are:

          BOLT_SERVER_ADDR   space-separated string of router addresses
          NEO4J_AUTH         colon-separated user and password

        """
        for key, value in sorted(self.service.env().items()):
            click.echo("%s=%r" % (key, value))

    @click.command()
    @click.pass_obj
    def exit(self):
        """ Shutdown all machines and exit the console.
        """
        raise SystemExit()

    @click.command()
    @click.argument("command", required=False)
    @click.pass_obj
    def help(self, command):
        """ Get help on a command or show all available commands.
        """
        if command:
            try:
                f = self[command]
            except KeyError:
                raise BadParameter('No such command "%s".' % command)
            else:
                ctx = self.help.make_context(command, [], obj=self)
                click.echo(f.get_help(ctx))
        else:
            click.echo("Commands:")
            command_width = max(map(len, self))
            text_width = 73 - command_width
            template = "  {:<%d}   {}" % command_width
            for arg0 in sorted(self):
                f = self[arg0]
                text = [f.get_short_help_str(limit=text_width)]
                for i, line in enumerate(text):
                    if i == 0:
                        click.echo(template.format(arg0, line))
                    else:
                        click.echo(template.format("", line))

    @click.command()
    @click.pass_obj
    def ls(self):
        """ Show a detailed list of the available servers.

        Routing information for the current transaction context is refreshed
        automatically if expired, or can be manually refreshed with the -r
        option. Each server is listed by name, along with the following
        details:

        \b
        - Docker container in which the server is running
        - Server mode: CORE, READ_REPLICA or SINGLE
        - Bolt port
        - HTTP port
        - Debug port

        """
        click.echo("NAME        CONTAINER   MODE           "
                   "BOLT PORT   HTTP PORT   DEBUG PORT")
        for spec, machine in self.service.machines.items():
            if spec is None or machine is None:
                continue
            click.echo("{:<12}{:<12}{:<15}{:<12}{:<12}{}".format(
                spec.fq_name,
                machine.container.short_id,
                spec.config.get("dbms.mode", "SINGLE"),
                spec.bolt_port or "-",
                spec.http_port or "-",
                spec.debug_opts.port or "-",
            ))

    @click.command()
    @click.argument("machine", required=False)
    @click.pass_obj
    def ping(self, machine):
        """ Ping a server by name to check it is available. If no server name
        is provided, 'a' is used as a default.
        """

        def f(m):
            m.ping(timeout=0)

        if not self._for_each_machine(machine, f):
            raise BadParameter("Machine {!r} not found".format(machine))

    @click.command()
    @click.argument("gdb", required=False)
    @click.pass_obj
    def rt(self, gdb):
        """ Display the routing table for a given graph database.
        """
        routers = self.service.routers()
        cx = Connector(ServiceProfile(routers[0].profiles["bolt"], routing=True))
        if gdb is None:
            click.echo("Refreshing routing information for the default graph database...")
        else:
            click.echo("Refreshing routing information for graph database %r..." % gdb)
        rt = cx.refresh_routing_table(gdb)
        ro_profiles, rw_profiles, _ = rt.runners()
        click.echo("Routers: %s" % " ".join(map(str, cx.get_router_profiles())))
        click.echo("Readers: %s" % " ".join(map(str, ro_profiles)))
        click.echo("Writers: %s" % " ".join(map(str, rw_profiles)))
        cx.close()

    @click.command()
    @click.argument("machine", required=False)
    @click.pass_obj
    def logs(self, machine):
        """ Display logs for a named server.

        If no server name is provided, 'a' is used as a default.
        """

        def f(m):
            click.echo(m.container.logs())

        if not self._for_each_machine(machine, f):
            raise BadParameter("Machine {!r} not found".format(machine))

    @click.command()
    @click.argument("time", type=float)
    @click.argument("machine", required=False)
    @click.pass_obj
    def pause(self, time, machine):
        """ Pause a server for a given number of seconds.

        If no server name is provided, 'a' is used as a default.
        """

        def f(m):
            click.echo("Pausing machine {!r} for {}s".format(m.spec.fq_name,
                                                             time))
            m.container.pause()
            sleep(time)
            m.container.unpause()
            m.ping(timeout=0)

        if not self._for_each_machine(machine, f):
            raise BadParameter("Machine {!r} not found".format(machine))


class Neo4jClusterConsole(Neo4jConsole):

    @click.command()
    @click.argument("mode")
    @click.pass_obj
    def add(self, mode):
        """ Add a new server by mode.

        The new server can be added in either "core" or "read-replica" mode.
        The full set of MODE values available are:

        - c, core
        - r, rr, replica, read-replica, read_replica

        """
        if mode in ("c", "core"):
            self.service.add_core()
        elif mode in ("r", "rr", "replica", "read-replica", "read_replica"):
            self.service.add_replica()
        else:
            raise BadParameter('Invalid value for "MODE", choose from '
                               '"core" or "read-replica"'.format(mode))

    @click.command()
    @click.argument("machine")
    @click.pass_obj
    def rm(self, machine):
        """ Remove a server by name or role.

        Servers can be identified either by their name (e.g. 'a', 'a.fbe340d')
        or by the role they fulfil (i.e. 'r' or 'w').
        """
        if not self.service.remove(machine):
            raise BadParameter("Machine {!r} not found".format(machine))

    @click.command()
    @click.argument("machine")
    @click.pass_obj
    def reboot(self, machine):
        """ Reboot a server by name or role.

        Servers can be identified either by their name (e.g. 'a', 'a.fbe340d')
        or by the role they fulfil (i.e. 'r' or 'w').
        """
        if not self.service.reboot(machine):
            raise BadParameter("Machine {!r} not found".format(machine))

#!/usr/bin/env python
# coding: utf-8

# Copyright 2011-2018, Nigel Small
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


from hashlib import sha256
from os import curdir, getenv, kill, listdir, makedirs, rename
from os.path import abspath, dirname, exists as path_exists, expanduser, isdir, isfile, join as path_join
from random import randint
from re import compile as re_compile
from shutil import rmtree
from socket import create_connection
from subprocess import check_call, check_output, CalledProcessError
from tarfile import TarFile, ReadError
from time import sleep
from threading import Thread

import click

from py2neo.internal.compat import bstr, DEVNULL, urlretrieve
from py2neo.internal.versioning import Version


editions = [
    "community",
    "enterprise",
]

versions = [

    # 3.0 series
    "3.0.0", "3.0.1", "3.0.2", "3.0.3", "3.0.4", "3.0.5", "3.0.6", "3.0.7", "3.0.8", "3.0.9", "3.0.10", "3.0.11",
    "3.0.12",

    # 3.1 series
    "3.1.0", "3.1.1", "3.1.2", "3.1.3", "3.1.4", "3.1.5", "3.1.6", "3.1.7", "3.1.8",

    # 3.2 series
    "3.2.0", "3.2.1", "3.2.2", "3.2.3",

    # 3.3 series
    "3.3.0", "3.3.1", "3.3.2", "3.3.3", "3.3.4", "3.3.5",

    # 3.4 series
    "3.4.0-beta01",

]

# TODO: make this a bit easier to read
version_tuples = [Version(v) for v in versions]
minor_version_tuples = sorted({(str(v.major), str(v.minor)) for v in version_tuples})
minor_versions = [".".join(map(str, v)) for v in minor_version_tuples]
latest_version_tuples = {w: sorted(v for v in version_tuples if v[:2] == w)[-1] for w in minor_version_tuples}
latest_versions = {".".join(map(str, k)): str(v) for k, v in latest_version_tuples.items()}
version_aliases = dict(latest_versions, **{k + "-LATEST": v for k, v in latest_versions.items()})
version_aliases["LATEST"] = versions[-1]

dist = "http://{}".format(getenv("NEO4J_DIST_HOST") or "dist.neo4j.org")
dist_overrides = {
    # "3.0.0-NIGHTLY": "http://alpha.neohq.net/dist",
}

archive_format = getenv("ARCHIVE_FORMAT") or "gz"


def hex_bytes(data):
    return "".join("%02X" % b for b in bytearray(data)).encode("utf-8")


def unhex_bytes(h):
    return bytearray(int(h[i:(i + 2)], 0x10) for i in range(0, len(h), 2))


class Distribution(object):
    """ Represents a Neo4j archive.
    """

    def __init__(self, edition=None, version=None):
        edition = edition.lower() if edition else "community"
        if edition in editions:
            self.edition = edition
        else:
            raise ValueError("Unknown edition %r" % edition)
        version = version.upper() if version else "LATEST"
        self.snapshot = "SNAPSHOT" in version
        if version in version_aliases:
            version = version_aliases[version]
        if version in versions:
            self.version = version
        else:
            raise ValueError("Unknown version %r" % version)

    @property
    def key(self):
        """ The unique key that identifies the archive, e.g.
        ``community-2.3.2``.
        """
        return "%s-%s" % (self.edition, self.version)

    @property
    def name(self):
        """ The full name of the archive file, e.g.
        ``neo4j-community-2.3.2-unix.tar.gz``.
        """
        return "neo4j-{}-unix.tar.{}".format(self.key, archive_format)

    @property
    def uri(self):
        """ The URI from which this archive may be downloaded, e.g.
        ``http://dist.neo4j.org/neo4j-community-2.3.2-unix.tar.gz``.
        """
        if self.version in dist_overrides:
            return "%s/%s" % (dist_overrides[self.version], self.name)
        else:
            return "%s/%s" % (dist, self.name)

    def download(self, path=".", overwrite=False):
        """ Download a Neo4j distribution to the specified path.

        :param path:
        :param overwrite:
        :return: the name of the downloaded file
        """
        file_name = path_join(path, self.name)
        if overwrite:
            if path_exists(file_name) and not isfile(file_name):
                raise IOError("Cannot overwrite directory %r" % file_name)
        elif not self.snapshot and path_exists(file_name):
            return file_name
        try:
            makedirs(path)
        except OSError:
            pass
        print("Downloading <%s>" % self.uri)
        urlretrieve(self.uri, file_name)
        return file_name


class Warehouse(object):
    """ A local storage area for Neo4j installations.
    """

    def __init__(self, home=None):
        self.home = home or getenv("PY2NEO_HOME") or expanduser("~/.py2neo")
        self.dist = path_join(self.home, "dist")
        self.run = path_join(self.home, "run")
        self.cc = path_join(self.home, "cc")

    def get(self, name, database=None, role=None, member=None):
        """ Obtain a Neo4j installation by name.

        :param name:
        :param database:
        :param role:
        :param member:
        :return:
        """
        if database and role and member is not None:
            container = path_join(self.cc, name, database, role, str(member))
        else:
            container = path_join(self.run, name)
        for dir_name in listdir(container):
            dir_path = path_join(container, dir_name)
            if isdir(dir_path):
                return Installation(dir_path)
        raise IOError("Could not locate installation directory")

    def install(self, name, edition=None, version=None, database=None, role=None, member=None):
        """ Install Neo4j.

        :param name:
        :param edition:
        :param version:
        :param database:
        :param role:
        :param member:
        :return:
        """
        if database and role and member is not None:
            container = path_join(self.cc, name, database, role, str(member))
        else:
            container = path_join(self.run, name)
        rmtree(container, ignore_errors=True)
        makedirs(container)
        archive_file = Distribution(edition, version).download(self.dist)
        try:
            with TarFile.open(archive_file, "r:{}".format(archive_format)) as archive:
                archive.extractall(container)
        except ReadError:
            # The tarfile module sometimes has trouble with certain tar
            # files for unknown reasons. This workaround falls back to
            # command line.
            check_call(["tar", "x", "-C", container, "-f", archive_file])
        return self.get(name, database, role, member)

    def install_local_cluster(self, name, version, **databases):
        """ Install a Neo4j Causal Cluster.
        """
        return LocalCluster.install(self, name, version, **databases)

    def uninstall(self, name, database=None, role=None, member=None):
        """ Remove a Neo4j installation.

        :param name:
        :param database:
        :param role:
        :param member:
        :return:
        """
        if database and role and member is not None:
            container = path_join(self.cc, name, database, role, str(member))
        else:
            container = path_join(self.run, name)
        rmtree(container, ignore_errors=True)

    def directory(self):
        """ Fetch a dictionary of :class:`.Installation` objects, keyed
        by name, for all available Neo4j installations.
        """
        try:
            return {name: self.get(name) for name in listdir(self.run) if not name.startswith(".")}
        except OSError:
            return {}

    def rename(self, name, new_name):
        """ Rename a Neo4j installation.

        :param name:
        :param new_name:
        :return:
        """
        rename(path_join(self.run, name), path_join(self.run, new_name))


class Installation(object):
    """ A Neo4j 3.0+ server installation.
    """

    config_file = "neo4j.conf"
    default_http_port = 7474

    def __init__(self, home=None):
        self.home = home or abspath(curdir)
        self.server = Server(self)
        self.auth = AuthFile(path_join(self.home, "data", "dbms", "auth"))

    def __repr__(self):
        return "<%s home=%r>" % (self.__class__.__name__, self.home)

    @property
    def store_path(self):
        """ The location of the graph database store on disk.
        """
        return path_join(self.home, "data", "databases",
                         self.get_config("dbms.active_database", "graph.db"))

    def get_config(self, key, default=None):
        """ Retrieve the value of a configuration item.

        :param key:
        :param default:
        :return:
        """
        config_file_path = path_join(self.home, "conf", self.config_file)
        with open(config_file_path, "r") as f_in:
            for line in f_in:
                if line.startswith(key + "="):
                    return line.strip().partition("=")[-1]
        return default

    def set_config(self, key, value):
        """ Update a single configuration value.

        :param key:
        :param value:
        """
        self.update_config({key: value})

    def update_config(self, properties):
        """ Update multiple configuration values.

        :param properties:
        """
        config_file_path = path_join(self.home, "conf", self.config_file)
        with open(config_file_path, "r") as f_in:
            lines = f_in.readlines()
        with open(config_file_path, "w") as f_out:
            properties2 = dict(properties)
            for line in lines:
                for key, value in properties2.items():
                    if line.startswith(key + "=") or \
                            (line.startswith("#") and line[1:].lstrip().startswith(key + "=")):
                        if value is True:
                            value = "true"
                        if value is False:
                            value = "false"
                        f_out.write("%s=%s\n" % (key, value))
                        del properties2[key]
                        break
                else:
                    f_out.write(line)
            for key, value in properties2.items():
                if value is True:
                    value = "true"
                if value is False:
                    value = "false"
                f_out.write("%s=%s\n" % (key, value))

    @property
    def auth_enabled(self):
        """ Settable boolean property for enabling and disabling auth
        on this server.
        """
        return self.get_config("dbms.security.auth_enabled", "true") == "true"

    @auth_enabled.setter
    def auth_enabled(self, value):
        self.set_config("dbms.security.auth_enabled", value)

    @property
    def http_port(self):
        """ The port on which this server expects HTTP communication.
        """
        port = None
        if port is None:
            http_address = self.get_config("dbms.connector.http.address")
            if http_address:
                host, _, port = http_address.partition(":")
            else:
                port = self.default_http_port
        try:
            return int(port)
        except (TypeError, ValueError):
            return None

    @http_port.setter
    def http_port(self, port):
        """ Set the port on which this server expects HTTP communication.
        """
        http_address = self.get_config("dbms.connector.http.address")
        if http_address:
            host, _, _ = http_address.partition(":")
        else:
            host = "localhost"
        self.set_config("dbms.connector.http.address", "%s:%d" % (host, port))

    @property
    def http_uri(self):
        """ The full HTTP URI for this server.
        """
        return "http://localhost:%d" % self.http_port

    def delete_store(self, force=False):
        """ Delete the store directory for this server.

        :param force:
        """
        if force or not self.server.running():
            try:
                rmtree(self.store_path, ignore_errors=force)
            except FileNotFoundError:
                pass
        else:
            raise RuntimeError("Refusing to drop database store while server is running")


class LocalCluster(object):

    @classmethod
    def install(cls, warehouse, name, version, **databases):
        """ Install a new Causal Cluster or Multicluster.

        :param warehouse: warehouse in which to install
        :param name: cluster or multicluster name
        :param version: Neo4j version (Enterprise edition is required)
        :param databases: pairs of db_name=core_size or db_name=(core_size, rr_size)
        :return: :class:`.LocalCluster` instance
        """
        core_databases = []
        read_replica_databases = []
        for database, size in databases.items():
            if isinstance(size, tuple):
                core_databases.extend([database] * size[0])
                read_replica_databases.extend([database] * size[1])
            else:
                core_databases.extend([database] * size)
        initial_discovery_members = ",".join("localhost:%d" % (18100 + i) for i, database in enumerate(core_databases))
        for i, database in enumerate(core_databases):
            install = warehouse.install(name, "enterprise", version, database=database, role="core", member=i)
            install.set_config("dbms.mode", "CORE")
            install.set_config("dbms.backup.enabled", False)
            install.set_config("dbms.connector.bolt.listen_address", ":%d" % (17100 + i))
            install.set_config("dbms.connector.http.listen_address", ":%d" % (17200 + i))
            install.set_config("dbms.connector.https.listen_address", ":%d" % (17300 + i))
            install.set_config("causal_clustering.database", database)
            install.set_config("causal_clustering.discovery_listen_address", ":%d" % (18100 + i))
            install.set_config("causal_clustering.expected_core_cluster_size", 3)
            install.set_config("causal_clustering.initial_discovery_members", initial_discovery_members)
            install.set_config("causal_clustering.raft_listen_address", ":%d" % (18200 + i))
            install.set_config("causal_clustering.transaction_listen_address", ":%d" % (18300 + i))
        for i, database in enumerate(read_replica_databases):
            install = warehouse.install(name, "enterprise", version, database=database, role="rr", member=i)
            install.set_config("dbms.mode", "READ_REPLICA")
            install.set_config("dbms.backup.enabled", False)
            install.set_config("dbms.connector.bolt.listen_address", ":%d" % (27100 + i))
            install.set_config("dbms.connector.http.listen_address", ":%d" % (27200 + i))
            install.set_config("dbms.connector.https.listen_address", ":%d" % (27300 + i))
            install.set_config("causal_clustering.database", database)
            install.set_config("causal_clustering.discovery_listen_address", ":%d" % (28100 + i))
            install.set_config("causal_clustering.expected_core_cluster_size", 3)
            install.set_config("causal_clustering.initial_discovery_members", initial_discovery_members)
            install.set_config("causal_clustering.raft_listen_address", ":%d" % (28200 + i))
            install.set_config("causal_clustering.transaction_listen_address", ":%d" % (28300 + i))
        return cls(warehouse, name)

    def __init__(self, warehouse, name):
        self.warehouse = warehouse
        self.name = name
        self.installations = {}
        for database, role, member in self._walk_installations():
            self.installations.setdefault(database, {}).setdefault(role, {})[member] = warehouse.get(name, database, role, member)

    def databases(self):
        for database in listdir(path_join(self.warehouse.cc, self.name)):
            yield database

    def members(self, database):
        core_path = path_join(self.warehouse.cc, self.name, database, "core")
        if isdir(core_path):
            for member in listdir(core_path):
                yield "core", member
        else:
            raise RuntimeError("Database %s has no cores" % database)
        rr_path = path_join(self.warehouse.cc, self.name, database, "rr")
        if isdir(rr_path):
            for member in listdir(rr_path):
                yield "rr", member

    def _walk_installations(self):
        """ For each installation in the cluster, yield a 3-tuple
        of (database, role, member).
        """
        for database in listdir(path_join(self.warehouse.cc, self.name)):
            core_path = path_join(self.warehouse.cc, self.name, database, "core")
            if isdir(core_path):
                for member in listdir(core_path):
                    yield database, "core", member
            else:
                raise RuntimeError("Database %s has no cores" % database)
            rr_path = path_join(self.warehouse.cc, self.name, database, "rr")
            if isdir(rr_path):
                for member in listdir(rr_path):
                    yield database, "rr", member

    def __repr__(self):
        return "<%s ?>" % (self.__class__.__name__,)

    def for_each(self, database, role, f):
        if not callable(f):
            raise TypeError("Callback is not callable")

        threads = []

        def call_f(i):
            t = Thread(target=f, args=[i])
            t.start()
            threads.append(t)

        for r, member in self.members(database):
            if r == role:
                install = self.warehouse.get(self.name, database, role, member)
                call_f(install)

        return threads

    @staticmethod
    def join_all(threads):
        while threads:
            for thread in list(threads):
                if thread.is_alive():
                    thread.join(timeout=0.1)
                else:
                    threads.remove(thread)

    def start(self):
        threads = []
        for database in self.databases():
            threads.extend(self.for_each(database, "core", lambda install: install.server.start()))
            threads.extend(self.for_each(database, "rr", lambda install: install.server.start()))
        self.join_all(threads)

    def stop(self):
        threads = []
        for database in self.databases():
            threads.extend(self.for_each(database, "core", lambda install: install.server.stop()))
            threads.extend(self.for_each(database, "rr", lambda install: install.server.stop()))
        self.join_all(threads)

    def uninstall(self):
        self.stop()
        self.installations.clear()
        rmtree(self.warehouse.cc, self.name)

    def update_auth(self, user, password):
        """ Update the auth file for each member with the given credentials.
        """
        threads = []
        for database in self.databases():
            threads.extend(self.for_each(database, "core", lambda install: install.auth.update(user, password)))
            threads.extend(self.for_each(database, "rr", lambda install: install.auth.update(user, password)))
        self.join_all(threads)


class Server(object):
    """ Represents a Neo4j server process that can be started and stopped.
    """

    def __init__(self, installation):
        self.installation = installation

    @property
    def control_script(self):
        return path_join(self.installation.home, "bin", "neo4j")

    def start(self, wait=True, verbose=False):
        """ Start the server.
        """
        try:
            out = check_output("%s start" % self.control_script, shell=True)
        except CalledProcessError as error:
            if error.returncode == 2:
                raise OSError("Another process is listening on the server port")
            elif error.returncode == 512:
                raise OSError("Another server process is already running")
            else:
                raise OSError("An error occurred while trying to start "
                              "the server [%s]" % error.returncode)
        else:
            pid = None
            for line in out.decode("utf-8").splitlines(False):
                if verbose:
                    print(line)
                if line.startswith("process"):
                    number_in_brackets = re_compile("\[(\d+)\]")
                    numbers = number_in_brackets.search(line).groups()
                    if numbers:
                        pid = int(numbers[0])
                elif "(pid " in line:
                    pid = int(line.partition("(pid ")[-1].partition(")")[0])
            if wait:
                running = False
                port = self.installation.http_port
                t = 0
                while not running and t < 30:
                    try:
                        s = create_connection(("localhost", port))
                    except IOError:
                        sleep(1)
                        t += 1
                    else:
                        s.close()
                        running = True
            return pid

    def stop(self):
        """ Stop the server.
        """
        pid = self.running()
        if not pid:
            return
        try:
            check_output(("%s stop" % self.control_script), shell=True)
        except CalledProcessError as error:
            raise OSError("An error occurred while trying to stop the server "
                          "[%s]" % error.returncode)
        while pid:
            try:
                kill(pid, 0)
            except OSError:
                pid = 0
            else:
                pass

    def restart(self):
        """ Restart the server.
        """
        self.stop()
        return self.start()

    def running(self):
        """ The PID of the current executing process for this server.
        """
        try:
            out = check_output(("%s status" % self.control_script), shell=True)
        except CalledProcessError as error:
            if error.returncode == 3:
                return None
            else:
                raise OSError("An error occurred while trying to query the "
                              "server status [%s]" % error.returncode)
        else:
            p = None
            for line in out.decode("utf-8").splitlines(False):
                if "running" in line:
                    p = int(line.rpartition(" ")[-1])
            return p


class AuthFile(object):
    """ A Neo4j auth file, generally located in data/dbms/auth.
    """

    def __init__(self, name):
        self.name = name
        if not isfile(self.name):
            d = dirname(self.name)
            try:
                makedirs(d)
            except OSError:
                pass
            with open(self.name, "wb"):
                pass

    def __iter__(self):
        with open(self.name, "rb") as f:
            for line in f:
                yield AuthUser.load(line)

    def remove(self, user_name):
        """ Remove a user.
        """
        user_name = bstr(user_name)
        with open(self.name, "rb") as f:
            lines = [line for line in f.readlines() if not AuthUser.match(line, user_name)]
        with open(self.name, "wb") as f:
            f.writelines(lines)

    def update(self, user_name, password):
        """ Add or update a user.
        """
        user_name = bstr(user_name)
        password = bstr(password)
        updated = False
        with open(self.name, "rb") as f:
            lines = []
            for line in f.readlines():
                if AuthUser.match(line, user_name):
                    lines.append(AuthUser.create(user_name, password).dump())
                    updated = True
                else:
                    lines.append(line)
        if not updated:
            lines.append(AuthUser.create(user_name, password).dump())
        with open(self.name, "wb") as f:
            f.writelines(lines)


class AuthUser(object):

    #: Name of user
    name = None

    #: The hash algorithm mused to encode the user data
    hash_algorithm = None

    #:
    digest = None

    #:
    salt = None

    @classmethod
    def create(cls, user_name, password):
        user_name = bstr(user_name)
        password = bstr(password)
        inst = cls(user_name, b"SHA-256", None, None)
        inst.set_password(password)
        return inst

    @classmethod
    def load(cls, s):
        s = bstr(s)
        fields = s.rstrip().split(b":")
        name = fields[0]
        hash_algorithm, digest, salt = fields[1].split(b",")
        return cls(name, hash_algorithm, unhex_bytes(digest), unhex_bytes(salt))

    @classmethod
    def match(cls, s, user_name):
        s = bstr(s)
        user_name = bstr(user_name)
        candidate_user_name, _, _ = s.partition(b":")
        return candidate_user_name == user_name

    def dump(self, eol=b"\r\n"):
        return self.name + b":" + self.hash_algorithm + b"," + hex_bytes(self.digest) + b"," + \
               hex_bytes(self.salt) + b":" + bstr(eol)

    def __init__(self, name, hash_algorithm, digest, salt):
        assert hash_algorithm == b"SHA-256"
        self.name = bstr(name)
        self.hash_algorithm = bstr(hash_algorithm)
        self.digest = digest
        self.salt = salt

    def __repr__(self):
        return "<AuthUser name=%r>" % self.name

    def set_password(self, password):
        assert self.hash_algorithm == b"SHA-256"
        salt = bytearray(randint(0x00, 0xFF) for _ in range(16))
        m = sha256()
        m.update(salt)
        m.update(bstr(password))
        self.digest = m.digest()
        self.salt = salt

    def check_password(self, password):
        assert self.hash_algorithm == b"SHA-256"
        m = sha256()
        m.update(self.salt)
        m.update(bstr(password))
        return m.digest() == self.digest


@click.group(help="""\
Tool for managing Neo4j installations.
""")
def cli():
    pass


@cli.command("list-users", help="""\
List users in a Neo4j auth file.
""")
@click.argument("auth_file")
def list_users(auth_file):
    for user in AuthFile(auth_file):
        click.echo(user.name)


@cli.command("del-user", help="""\
Remove a user from a Neo4j auth file.
""")
@click.argument("auth_file")
@click.argument("user_name")
def del_user(auth_file, user_name):
    AuthFile(auth_file).remove(user_name)


@cli.command("put-user", help="""\
Create or set the password for a user in a Neo4j auth file.

For general interactive use, password and confirmation prompts will be presented.
For batch mode, use the --password option.

Example:

    py2neo-admin put-user data/dbms/auth alice

If AUTH_FILE contains only a dash `-` then the auth file entry will be written to stdout instead.
""")
@click.argument("auth_file")
@click.argument("user_name")
@click.password_option()
def put_user(auth_file, user_name, password):
    AuthFile(auth_file).update(user_name, password)


def main():
    try:
        cli(obj={})
    except Exception as error:
        click.secho(error.args[0], err=True)
        exit(1)
    else:
        exit(0)


if __name__ == "__main__":
    main()

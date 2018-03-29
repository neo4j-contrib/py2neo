#!/usr/bin/env python
# coding: utf-8

# Copyright 2011-2017, Nigel Small
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
from os.path import abspath, dirname, expanduser, isdir, isfile, join as path_join
from random import randint
from re import compile as re_compile
from shutil import rmtree
from socket import create_connection
from subprocess import check_call, check_output, CalledProcessError
from sys import stdout
from tarfile import TarFile, ReadError
from time import sleep

from py2neo.compat import bstr, DEVNULL
from py2neo.dist import archive_format, Distribution


number_in_brackets = re_compile("\[(\d+)\]")


def hex_bytes(data):
    return "".join("%02X" % b for b in bytearray(data)).encode("utf-8")


def unhex_bytes(h):
    return bytearray(int(h[i:(i + 2)], 0x10) for i in range(0, len(h), 2))


class Warehouse(object):
    """ A local storage area for Neo4j installations.
    """

    def __init__(self, home=None):
        self.home = home or getenv("PY2NEO_HOME") or expanduser("~/.py2neo")
        self.dist = path_join(self.home, "dist")
        self.run = path_join(self.home, "run")

    def get(self, name):
        """ Obtain a Neo4j installation by name.

        :param name:
        :return:
        """
        container = path_join(self.run, name)
        for dir_name in listdir(container):
            dir_path = path_join(container, dir_name)
            if isdir(dir_path):
                return Installation(dir_path)
        raise IOError("Could not locate installation directory")

    def install(self, name, edition=None, version=None):
        """ Install Neo4j.

        :param name:
        :param edition:
        :param version:
        :return:
        """
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
        return self.get(name)

    def uninstall(self, name):
        """ Remove a Neo4j installation.

        :param name:
        :return:
        """
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
            for line in lines:
                for key, value in properties.items():
                    if line.startswith(key + "=") or \
                            (line.startswith("#") and line[1:].lstrip().startswith(key + "=")):
                        if value is True:
                            value = "true"
                        if value is False:
                            value = "false"
                        f_out.write("%s=%s\n" % (key, value))
                        break
                else:
                    f_out.write(line)

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
        if self.server.running():
            try:
                port = self.server.info("NEO4J_SERVER_PORT")
            except OSError:
                pass
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


class Server(object):
    """ Represents a Neo4j server process that can be started and stopped.
    """

    def __init__(self, installation):
        self.installation = installation

    @property
    def control_script(self):
        return path_join(self.installation.home, "bin", "neo4j")

    def start(self):
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
                if line.startswith("process"):
                    numbers = number_in_brackets.search(line).groups()
                    if numbers:
                        pid = int(numbers[0])
                elif "(pid " in line:
                    pid = int(line.partition("(pid ")[-1].partition(")")[0])
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

    def info(self, key):
        """ Look up an item of server information from a running server.

        :param key: the key of the item to look up
        """
        try:
            out = check_output("%s info" % self.control_script, shell=True, stderr=DEVNULL)
        except CalledProcessError as error:
            if error.returncode == 3:
                return None
            else:
                raise OSError("An error occurred while trying to fetch server "
                              "info [%s]" % error.returncode)
        else:
            for line in out.decode("utf-8").splitlines(False):
                try:
                    colon = line.index(":")
                except ValueError:
                    pass
                else:
                    k = line[:colon]
                    v = line[colon+1:].lstrip()
                    if k == "CLASSPATH":
                        v = v.split(":")
                    if k == key:
                        return v


class AuthFile(object):

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

    def append(self, user_name, password):
        user_name = bstr(user_name)
        password = bstr(password)
        line = AuthUser.create(user_name, password).dump()
        if self.name == "-":
            stdout.write(line.decode("utf-8"))
        else:
            with open(self.name, "ab") as f:
                f.write(line)

    def remove(self, user_name):
        user_name = bstr(user_name)
        with open(self.name, "rb") as f:
            lines = [line for line in f.readlines() if not AuthUser.match(line, user_name)]
        with open(self.name, "wb") as f:
            f.writelines(lines)

    def update(self, user_name, password):
        user_name = bstr(user_name)
        password = bstr(password)
        with open(self.name, "rb") as f:
            lines = []
            for line in f.readlines():
                if AuthUser.match(line, user_name):
                    lines.append(AuthUser.create(user_name, password).dump())
                else:
                    lines.append(line)
        with open(self.name, "wb") as f:
            f.writelines(lines)


class AuthUser(object):

    #: Name of user
    user = None

    #:
    digest = None

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

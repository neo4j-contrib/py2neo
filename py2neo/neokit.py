#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright 2011-2016, Nigel Small
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


import fileinput
from os import getenv, listdir, makedirs
from os.path import basename as path_basename, exists as path_exists, expanduser as path_expanduser, isdir as path_isdir, \
    isfile as path_isfile, join as path_join
import re
import shlex
from shutil import rmtree
from subprocess import check_output, CalledProcessError
from sys import stdout
from tarfile import TarFile
try:
    from urllib.request import urlopen
except ImportError:
    from urllib2 import urlopen

try:
    from configparser import SafeConfigParser

    class PropertiesParser(SafeConfigParser):

        def read_properties(self, filename, section=None):
            if not section:
                basename = path_basename(filename)
                if basename.endswith(".properties"):
                    section = basename[:-11]
                else:
                    section = basename
            with open(filename) as f:
                data = f.read()
            self.read_string("[%s]\n%s" % (section, data), filename)

except ImportError:
    from ConfigParser import SafeConfigParser
    from io import StringIO
    from codecs import open as codecs_open
    from os import SEEK_SET

    class PropertiesParser(SafeConfigParser):

        def read_properties(self, filename, section=None):
            if not section:
                basename = path_basename(filename)
                if basename.endswith(".properties"):
                    section = basename[:-11]
                else:
                    section = basename
            data = StringIO()
            data.write("[%s]\n" % section)
            with codecs_open(filename, encoding="utf-8") as f:
                data.write(f.read())
            data.seek(0, SEEK_SET)
            self.readfp(data)


number_in_brackets = re.compile("\[(\d+)\]")

editions = [
    "community",
    "enterprise",
]
versions = [
    "2.0.0", "2.0.1", "2.0.2", "2.0.3", "2.0.4",
    "2.1.2", "2.1.3", "2.1.4", "2.1.5", "2.1.6", "2.1.7", "2.1.8",
    "2.2.0", "2.2.1", "2.2.2", "2.2.3", "2.2.4", "2.2.5", "2.2.6", "2.2.7", "2.2.8",
    "2.3.0", "2.3.1", "2.3.2",
    "3.0.0-M02", "3.0.0-NIGHTLY",
]
version_aliases = {
    "2.0": "2.0.4",
    "2.0-LATEST": "2.0.4",
    "2.1": "2.1.8",
    "2.1-LATEST": "2.1.8",
    "2.2": "2.2.8",
    "2.2-LATEST": "2.2.8",
    "2.3": "2.3.2",
    "2.3-LATEST": "2.3.2",
    "3.0": "3.0.0-M02",
    "3.0-MILESTONE": "3.0.0-M02",
    "3.0-LATEST": "3.0.0-M02",
    "3.0-SNAPSHOT": "3.0.0-NIGHTLY",
    "LATEST": "2.3.2",
    "MILESTONE": "3.0.0-M02",
    "SNAPSHOT": "3.0.0-NIGHTLY",
}

dist = "http://dist.neo4j.org"
dist_overrides = {
    "3.0.0-NIGHTLY": "http://alpha.neohq.net/dist",
}


class Package(object):

    def __init__(self, edition=None, version=None):
        edition = edition.lower() if edition else "community"
        if edition in editions:
            self.edition = edition
        else:
            raise ValueError("Unknown edition %r" % edition)
        version = version.upper() if version else "LATEST"
        if version in version_aliases:
            version = version_aliases[version]
        if version in versions:
            self.version = version
        else:
            raise ValueError("Unknown version %r" % version)

    @property
    def key(self):
        return "%s-%s" % (self.edition, self.version)

    @property
    def name(self):
        return "neo4j-%s-unix.tar.gz" % self.key

    @property
    def url(self):
        if self.version in dist_overrides:
            return "%s/%s" % (dist_overrides[self.version], self.name)
        else:
            return "%s/%s" % (dist, self.name)

    def download(self, path=".", overwrite=False):
        file_name = path_join(path, self.name)
        if overwrite:
            if path_exists(file_name) and not path_isfile(file_name):
                raise IOError("Cannot overwrite directory %r" % file_name)
        else:
            if path_exists(file_name):
                return file_name
        with urlopen(self.url) as f_in:
            with open(file_name, "wb") as f_out:
                f_out.write(f_in.read())
        return file_name


class Warehouse(object):

    def __init__(self, home=None):
        self.home = home or getenv("NEOKIT_HOME") or path_expanduser("~/.neokit")
        self.dist = path_join(self.home, "dist")
        self.run = path_join(self.home, "run")

    def get(self, name):
        container = path_join(self.run, name)
        for dir_name in listdir(container):
            dir_path = path_join(container, dir_name)
            if path_isdir(dir_path):
                return GraphServer(dir_path)
        raise IOError("Could not locate server directory")

    def install(self, name, edition=None, version=None):
        container = path_join(self.run, name)
        rmtree(container, ignore_errors=True)
        makedirs(container)
        archive_file = Package(edition, version).download(self.dist)
        with TarFile.open(archive_file, "r") as archive:
            archive.extractall(container)
        return self.get(name)


class GraphServer(object):
    """ Represents a Neo4j server installation on disk.
    """

    def __init__(self, home):
        self.home = home

    def __repr__(self):
        return "<GraphServer home=%r>" % self.home

    @property
    def control_script(self):
        """ The file name of the control script for this server installation.
        """
        return path_join(self.home, "bin", "neo4j")

    @property
    def store_path(self):
        return path_join(self.home, self.get_config("neo4j-server.properties", "org.neo4j.server.database.location"))

    def get_config(self, file_name, key):
        file_path = path_join(self.home, "conf", file_name)
        with open(file_path, "r") as f_in:
            for line in f_in:
                if line.startswith(key + "="):
                    return line.strip().partition("=")[-1]

    def set_config(self, file_name, key, value):
        self.update_config(file_name, {key: value})

    def update_config(self, file_name, properties):
        file_path = path_join(self.home, "conf", file_name)
        with open(file_path, "r") as f_in:
            lines = f_in.readlines()
        with open(file_path, "w") as f_out:
            for line in lines:
                for key, value in properties.items():
                    if line.startswith(key + "="):
                        if value is True:
                            value = "true"
                        if value is False:
                            value = "false"
                        f_out.write("%s=%s\n" % (key, value))
                        break
                else:
                    f_out.write(line)

    def enable_auth(self):
        self.set_config("neo4j-server.properties", "dbms.security.auth_enabled", True)

    def disable_auth(self):
        self.set_config("neo4j-server.properties", "dbms.security.auth_enabled", False)

    def set_http_port(self, port):
        self.set_config("neo4j-server.properties", "org.neo4j.server.webserver.port", port)

    def delete_store(self, force=False):
        """ Delete this store directory.

        :param force:

        """
        if force or not self.running():
            try:
                rmtree(self.store_path, ignore_errors=force)
            except FileNotFoundError:
                pass
        else:
            raise RuntimeError("Refusing to drop database store while server is running")

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
            return pid

    def stop(self):
        """ Stop the server.
        """
        try:
            _ = check_output(("%s stop" % self.control_script), shell=True)
        except CalledProcessError as error:
            raise OSError("An error occurred while trying to stop the server [%s]" % error.returncode)

    def restart(self):
        """ Restart the server.
        """
        self.stop()
        self.start()

    def run(self):
        self.start()
        self.stop()

    def running(self):
        """ The PID of the current executing process for this server.
        """
        try:
            out = check_output(("%s status" % self.control_script), shell=True)
        except CalledProcessError as error:
            if error.returncode == 3:
                return None
            else:
                raise OSError("An error occurred while trying to query the server status [%s]" % error.returncode)
        else:
            p = None
            for line in out.decode("utf-8").splitlines(False):
                if "running" in line:
                    p = int(line.rpartition(" ")[-1])
            return p

    def info(self):
        """ Dictionary of server information.
        """
        try:
            out = check_output("%s info" % self.control_script, shell=True)
        except CalledProcessError as error:
            if error.returncode == 3:
                return None
            else:
                raise OSError("An error occurred while trying to fetch server info [%s]" % error.returncode)
        else:
            data = {}
            for line in out.decode("utf-8").splitlines(False):
                try:
                    colon = line.index(":")
                except ValueError:
                    pass
                else:
                    key = line[:colon]
                    value = line[colon+1:].lstrip()
                    if key.endswith("_PORT"):
                        try:
                            value = int(value)
                        except ValueError:
                            pass
                    elif key == "CLASSPATH":
                        value = value.split(":")
                    data[key] = value
            return data

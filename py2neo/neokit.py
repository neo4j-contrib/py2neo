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


from os import linesep, getenv, listdir, makedirs, rename
from os.path import basename as path_basename, exists as path_exists, expanduser as path_expanduser, isdir as path_isdir, \
    isfile as path_isfile, join as path_join
import re
from shutil import rmtree
from subprocess import call, check_output, CalledProcessError
from sys import stdout, stderr
from tarfile import TarFile
from textwrap import dedent
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

    def uninstall(self, name):
        container = path_join(self.run, name)
        rmtree(container)

    def directory(self):
        return {name: self.get(name) for name in listdir(self.run) if not name.startswith(".")}

    def rename(self, name, new_name):
        rename(path_join(self.run, name), path_join(self.run, new_name))


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
        return path_join(self.home, self.config("neo4j-server.properties",
                                                "org.neo4j.server.database.location"))

    def config(self, file_name, key):
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

    @property
    def auth_enabled(self):
        return self.config("neo4j-server.properties", "dbms.security.auth_enabled") == "true"

    @auth_enabled.setter
    def auth_enabled(self, value):
        self.set_config("neo4j-server.properties", "dbms.security.auth_enabled", value)

    @property
    def users(self):
        file_path = path_join(self.home, "data", "dbms", "auth")
        with open(file_path, "r") as f_in:
            return [line.partition(":")[0] for line in f_in]

    def delete_user(self, user):
        pass

    def set_user_password(self, user, password):
        pass

    @property
    def http_port(self):
        port = None
        if self.running():
            port = self.info("NEO4J_SERVER_PORT")
        if port is None:
            port = self.config("neo4j-server.properties", "org.neo4j.server.webserver.port")
        try:
            return int(port)
        except (TypeError, ValueError):
            return None

    @http_port.setter
    def http_port(self, port):
        self.set_config("neo4j-server.properties", "org.neo4j.server.webserver.port", port)

    @property
    def http_uri(self):
        return "http://localhost:%d/" % self.http_port

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
            check_output(("%s stop" % self.control_script), shell=True)
        except CalledProcessError as error:
            raise OSError("An error occurred while trying to stop the server "
                          "[%s]" % error.returncode)

    def restart(self):
        """ Restart the server.
        """
        self.stop()
        self.start()

    def run(self, *commands):
        self.start()
        exit_status = 0
        for command in commands:
            exit_status = call(command)
            if exit_status:
                break
        self.stop()
        return exit_status

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
        """ Lookup an item of server information from a running server.

        :arg key: the key of the item to look up
        """
        try:
            out = check_output("%s info" % self.control_script, shell=True)
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


class Commander(object):

    epilog = "Report bugs to nigel@py2neo.org"

    def __init__(self, out=None):
        self.out = out or stdout

    def write(self, s):
        self.out.write(s)

    def write_line(self, s):
        self.out.write(s)
        self.out.write(linesep)

    def usage(self, script):
        self.write_line("usage: %s <command> <arguments>" % path_basename(script))
        self.write_line("")
        self.write_line("commands:")
        for attr in sorted(dir(self)):
            method = getattr(self, attr)
            if callable(method) and not attr.startswith("_") and method.__doc__:
                doc = dedent(method.__doc__).strip()
                self.write_line("    " + doc[6:].strip())
        if self.epilog:
            self.write_line("")
            self.write_line(self.epilog)

    def execute(self, *args):
        try:
            command, command_args = args[1], args[2:]
        except IndexError:
            self.usage(args[0])
        else:
            command = command.replace("-", "_")
            try:
                method = getattr(self, command)
            except AttributeError:
                self.write_line("Unknown command %r" % command)
            else:
                try:
                    method(*args[1:])
                except Exception as err:
                    stderr.write("Error: %s" % err)
                    stderr.write(linesep)

    def parser(self, script):
        from argparse import ArgumentParser
        return ArgumentParser(prog=script, epilog=self.epilog)

    def download(self, *args):
        """ usage: download [<version>]
        """
        parser = self.parser(args[0])
        parser.description = "Download a Neo4j server package"
        parser.add_argument("version", nargs="?", help="Neo4j version")
        parsed = parser.parse_args(args[1:])
        Package(version=parsed.version).download()
        self.write_line(Package(version=parsed.version).download())

    def install(self, *args):
        """ usage: install <name> [<version>]
        """
        parser = self.parser(args[0])
        parser.description = "Install a Neo4j server"
        parser.add_argument("name", help="server name")
        parser.add_argument("version", nargs="?", help="Neo4j version")
        parsed = parser.parse_args(args[1:])
        name = parsed.name
        warehouse = Warehouse()
        server = warehouse.install(name, version=parsed.version)
        self.write_line("Server %r is available at %r" % (name, server.home))

    def uninstall(self, *args):
        """ usage: uninstall <name>
        """
        parser = self.parser(args[0])
        parser.description = "Uninstall a Neo4j server"
        parser.add_argument("name", help="server name")
        parsed = parser.parse_args(args[1:])
        name = parsed.name
        warehouse = Warehouse()
        server = warehouse.get(name)
        if server.running():
            server.stop()
        warehouse.uninstall(name)
        self.write_line("Server %r uninstalled" % name)

    def directory(self, *args):
        """ usage: list
        """
        parser = self.parser(args[0])
        parser.description = "List all installed Neo4j servers"
        parser.parse_args(args[1:])
        warehouse = Warehouse()
        for name in sorted(warehouse.directory()):
            self.write_line(name)

    def rename(self, *args):
        """ usage: rename <name> <new-name>
        """
        parser = self.parser(args[0])
        parser.description = "Rename a Neo4j server"
        parser.add_argument("name", help="server name")
        parser.add_argument("new_name", help="new server name")
        parsed = parser.parse_args(args[1:])
        warehouse = Warehouse()
        warehouse.rename(parsed.name, parsed.new_name)
        self.write_line("Renamed server %r to %r" % (parsed.name, parsed.new_name))

    def start(self, *args):
        """ usage: start <name>
        """
        parser = self.parser(args[0])
        parser.description = "Start a Neo4j server"
        parser.add_argument("name", help="server name")
        parsed = parser.parse_args(args[1:])
        name = parsed.name
        warehouse = Warehouse()
        server = warehouse.get(name)
        if server.running():
            stderr.write("Server %r is already running" % name)
            stderr.write(linesep)
        else:
            pid = server.start()
            self.write_line("Server %r started as process %d" % (name, pid))

    def stop(self, *args):
        """ usage: stop <name>
        """
        parser = self.parser(args[0])
        parser.description = "Stop a Neo4j server"
        parser.add_argument("name", help="server name")
        parsed = parser.parse_args(args[1:])
        name = parsed.name
        warehouse = Warehouse()
        server = warehouse.get(name)
        if server.running():
            server.stop()
            self.write_line("Server %r stopped" % name)
        else:
            stderr.write("Server %r is not running" % name)
            stderr.write(linesep)

    def enable_auth(self, *args):
        """ usage: enable-auth <name>
        """
        parser = self.parser(args[0])
        parser.description = "Enable auth on a Neo4j server"
        parser.add_argument("name", help="server name")
        parsed = parser.parse_args(args[1:])
        name = parsed.name
        warehouse = Warehouse()
        server = warehouse.get(name)
        server.auth_enabled = True
        if server.running():
            self.write_line("Auth enabled - this will take effect when the server is restarted")
        else:
            self.write_line("Auth enabled")

    def disable_auth(self, *args):
        """ usage: disable-auth <name>
        """
        parser = self.parser(args[0])
        parser.description = "Disable auth on a Neo4j server"
        parser.add_argument("name", help="server name")
        parsed = parser.parse_args(args[1:])
        name = parsed.name
        warehouse = Warehouse()
        server = warehouse.get(name)
        server.auth_enabled = False
        if server.running():
            self.write_line("Auth disabled - this will take effect when the server is restarted")
        else:
            self.write_line("Auth disabled")


def main(args=None, out=None):
    from sys import argv
    Commander(out).execute(*args or argv)

if __name__ == "__main__":
    main()

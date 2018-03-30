#!/usr/bin/env python
# -*- encoding: utf-8 -*-

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


"""
********************************************
``neokit`` -- Command Line Toolkit for Neo4j
********************************************

Neokit is a standalone module for managing one or more Neo4j
installations. The Neokit classes may be used programmatically but
will generally be invoked via the command line interface. If Neokit
has been installed as part of the Py2neo package, the command line
tool will be available as `neokit`; otherwise, it can be called
as a Python module: `python -m neokit`.


Command Line Usage
==================

Installing a Neo4j archive
--------------------------
::

    $ neokit install 3.0


API
===
"""


from argparse import ArgumentParser
from contextlib import contextmanager
from os import linesep, rename
from os.path import basename, expanduser, join as path_join
from subprocess import call
from sys import argv, stdout, stderr
from textwrap import dedent

from py2neo.admin import Distribution, Installation, Warehouse, versions, version_aliases


SERVER_AUTH_FAILURE = 9
SERVER_NOT_RUNNING = 10
SERVER_ALREADY_RUNNING = 11


@contextmanager
def move_file(file_name):
    temp_file_name = file_name + ".backup"
    try:
        rename(file_name, temp_file_name)
    except OSError:
        renamed = False
    else:
        renamed = True
    yield temp_file_name
    if renamed:
        rename(temp_file_name, file_name)


class Commander(object):

    epilog = "Report bugs to py2neo@nige.tech"

    def __init__(self, out=None, err=None):
        self.out = out or stdout
        self.err = err or stderr

    def write(self, s):
        self.out.write(s)

    def write_line(self, s):
        self.out.write(s)
        self.out.write(linesep)

    def write_err(self, s):
        self.err.write(s)

    def write_err_line(self, s):
        self.err.write(s)
        self.err.write(linesep)

    def usage(self, script):
        script = basename(script)
        self.write_line("usage: %s <command> <arguments>" % script)
        self.write_line("       %s help <command>" % script)
        self.write_line("")
        self.write_line("commands:")
        for attr in sorted(dir(self)):
            method = getattr(self, attr)
            if callable(method) and not attr.startswith("_") and method.__doc__:
                doc = dedent(method.__doc__).strip()
                self.write_line("    " + doc[6:].strip())
        self.write_line("")
        self.write_line(
                "Many commands can take '.' as an installation name. This operates on the installation\n"
                "located in the current directory. For example:\n"
                "\n"
                "    neokit disable-auth .")
        if self.epilog:
            self.write_line("")
            self.write_line(self.epilog)

    def execute(self, *args):
        try:
            command = args[1]
        except IndexError:
            self.usage(args[0])
            return
        command = command.replace("-", "_")
        if command == "help":
            command = args[2]
            args = [args[0], command, "--help"]
        try:
            method = getattr(self, command)
        except AttributeError:
            self.write_err_line("Unknown command %r" % command)
            exit(1)
        else:
            try:
                return method(*args[1:]) or 0
            except Exception as err:
                self.write_err_line("Error: %s" % err)
                exit(1)

    def parser(self, script):
        return ArgumentParser(prog=script, epilog=self.epilog)

    def versions(self, *args):
        """ usage: versions
        """
        parser = self.parser(args[0])
        parser.description = "List all available Neo4j versions"
        parser.parse_args(args[1:])
        for version in versions:
            self.write(version)
            aliases = []
            for alias, original in version_aliases.items():
                if original == version:
                    aliases.append(alias)
            if aliases:
                self.write(" (%s)" % ", ".join(sorted(aliases)))
            self.write(linesep)

    def download(self, *args):
        """ usage: download [<version>]
        """
        parser = self.parser(args[0])
        parser.description = "Download a Neo4j distribution"
        parser.add_argument("version", nargs="?", help="Neo4j version")
        parsed = parser.parse_args(args[1:])
        self.write_line(Distribution(version=parsed.version).download())

    def install(self, *args):
        """ usage: install <installation> [<version>]
        """
        parser = self.parser(args[0])
        parser.description = "Install a Neo4j distribution"
        parser.add_argument("installation", help="installation name")
        parser.add_argument("version", nargs="?", help="Neo4j version")
        parsed = parser.parse_args(args[1:])
        installation = Warehouse().install(parsed.installation, version=parsed.version)
        self.write_line(installation.home)

    def uninstall(self, *args):
        """ usage: uninstall <installation>
        """
        parser = self.parser(args[0])
        parser.description = "Uninstall a Neo4j distribution"
        parser.add_argument("installation", help="installation name")
        parsed = parser.parse_args(args[1:])
        installation_name = parsed.installation
        warehouse = Warehouse()
        installation = warehouse.get(installation_name)
        if installation.server.running():
            installation.server.stop()
        warehouse.uninstall(installation_name)

    def list(self, *args):
        """ usage: list
        """
        parser = self.parser(args[0])
        parser.description = "List all Neo4j installations"
        parser.parse_args(args[1:])
        for name in sorted(Warehouse().directory()):
            self.write_line(name)

    def rename(self, *args):
        """ usage: rename <installation> <new-name>
        """
        parser = self.parser(args[0])
        parser.description = "Rename a Neo4j installation"
        parser.add_argument("installation", help="installation name")
        parser.add_argument("new_name", help="new installation name")
        parsed = parser.parse_args(args[1:])
        Warehouse().rename(parsed.installation, parsed.new_name)

    def start(self, *args):
        """ usage: start <installation>
        """
        parser = self.parser(args[0])
        parser.description = "Start Neo4j"
        parser.add_argument("installation", help="installation name")
        parsed = parser.parse_args(args[1:])
        if parsed.installation == ".":
            installation = Installation()
        else:
            installation = Warehouse().get(parsed.installation)
        if installation.server.running():
            self.write_err_line("Neo4j already running")
            return SERVER_ALREADY_RUNNING
        else:
            pid = installation.server.start()
            self.write_line("%d" % pid)

    def stop(self, *args):
        """ usage: stop <installation>
        """
        parser = self.parser(args[0])
        parser.description = "Stop Neo4j"
        parser.add_argument("installation", help="installation name")
        parsed = parser.parse_args(args[1:])
        if parsed.installation == ".":
            installation = Installation()
        else:
            installation = Warehouse().get(parsed.installation)
        if installation.server.running():
            installation.server.stop()
        else:
            self.write_err_line("Neo4j not running")
            return SERVER_NOT_RUNNING

    def restart(self, *args):
        """ usage: restart <installation>
        """
        parser = self.parser(args[0])
        parser.description = "Start or restart Neo4j"
        parser.add_argument("installation", help="installation name")
        parsed = parser.parse_args(args[1:])
        if parsed.installation == ".":
            installation = Installation()
        else:
            installation = Warehouse().get(parsed.installation)
        if installation.server.running():
            pid = installation.server.restart()
        else:
            pid = installation.server.start()
        self.write_line("%d" % pid)

    def run(self, *args):
        """ usage: run <installation> <command>
        """
        parser = self.parser(args[0])
        parser.description = "Run a shell command against Neo4j, starting and stopping before and after"
        parser.add_argument("installation", help="installation name")
        parser.add_argument("command", nargs="+", help="command to run")
        parsed = parser.parse_args(args[1:])
        with move_file(path_join(expanduser("~"), ".neo4j", "known_hosts")):
            if parsed.installation == ".":
                installation = Installation()
            else:
                installation = Warehouse().get(parsed.installation)
            if installation.server.running():
                self.write_err_line("Neo4j already running")
                return SERVER_ALREADY_RUNNING
            else:
                installation.server.start()
                try:
                    return call(parsed.command)
                finally:
                    installation.server.stop()

    def enable_auth(self, *args):
        """ usage: enable-auth <installation>
        """
        parser = self.parser(args[0])
        parser.description = "Enable auth on Neo4j"
        parser.add_argument("installation", help="installation name")
        parsed = parser.parse_args(args[1:])
        if parsed.installation == ".":
            installation = Installation()
        else:
            installation = Warehouse().get(parsed.installation)
        installation.auth_enabled = True

    def disable_auth(self, *args):
        """ usage: disable-auth <installation>
        """
        parser = self.parser(args[0])
        parser.description = "Disable auth on Neo4j"
        parser.add_argument("installation", help="installation name")
        parsed = parser.parse_args(args[1:])
        if parsed.installation == ".":
            installation = Installation()
        else:
            installation = Warehouse().get(parsed.installation)
        installation.auth_enabled = False


def main(args=None, out=None, err=None):
    exit_status = Commander(out, err).execute(*args or argv)
    exit(exit_status)


if __name__ == "__main__":
    main()

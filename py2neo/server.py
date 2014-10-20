#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright 2011-2014, Nigel Small
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


""" Linux only!
"""

import fileinput
import re
import os
from subprocess import check_output, CalledProcessError
import shlex
import sys

from py2neo import ServiceRoot
from py2neo.env import NEO4J_HOME
from py2neo.store import GraphStore


__all__ = ["GraphServerProcess", "GraphServer"]

number_in_brackets = re.compile("\[(\d+)\]")


class GraphServerProcess(object):
    """ A Neo4j server process.
    """

    def __init__(self, server, service_root, **kwargs):
        self.server = server
        self.service_root = service_root
        self.pid = kwargs.get("pid")
        self.jvm_arguments = kwargs.get("jvm_arguments")

    def stop(self):
        self.server.stop()

    @property
    def graph(self):
        return self.service_root.graph


class GraphServer(object):
    """ Represents a Neo4j server installation on disk.
    """

    # TODO: __instances

    def __init__(self, home=NEO4J_HOME):
        self.home = home

    def __repr__(self):
        return "<GraphServer home=%r>" % self.home

    @property
    def script(self):
        """ The file name of the control script for this server installation.
        """
        return os.path.join(self.home, "bin", "neo4j")

    @property
    def store(self):
        return GraphStore.for_server(self)

    def start(self):
        """ Start the server.

        :rtype: :class:`py2neo.server.GraphServerProcess`
        """
        try:
            out = check_output("%s start" % self.script, shell=True)
        except CalledProcessError as error:
            if error.returncode == 2:
                raise OSError("Another process is listening on the server port")
            elif error.returncode == 512:
                raise OSError("Another server process is already running")
            else:
                raise OSError("An error occurred while trying to start "
                              "the server [%s]" % error.returncode)
        else:
            uri = None
            kwargs = {}
            for line in out.decode("utf-8").splitlines(keepends=False):
                if line.startswith("Using additional JVM arguments:"):
                    kwargs["jvm_arguments"] = shlex.split(line[32:])
                elif line.startswith("process"):
                    numbers = number_in_brackets.search(line).groups()
                    if numbers:
                        kwargs["pid"] = int(numbers[0])
                elif line.startswith("http"):
                    uri = line.partition(" ")[0]
            if not uri:
                raise RuntimeError("Unable to parse output from server startup")
            return GraphServerProcess(self, ServiceRoot(uri), **kwargs)

    def stop(self):
        """ Stop the server.
        """
        try:
            _ = check_output(("%s stop" % self.script), shell=True)
        except CalledProcessError as error:
            raise OSError("An error occurred while trying to stop "
                          "the server [%s]" % error.returncode)

    def restart(self):
        """ Restart the server.
        """
        self.stop()
        self.start()

    @property
    def pid(self):
        """ The PID of the current executing process for this server,
        """
        try:
            out = check_output(("%s status" % self.script), shell=True)
        except CalledProcessError as error:
            if error.returncode == 3:
                return None
            else:
                raise OSError("An error occurred while trying to query the "
                              "server status [%s]" % error.returncode)
        else:
            p = None
            for line in out.decode("utf-8").splitlines(keepends=False):
                if "running" in line:
                    p = int(line.rpartition(" ")[-1])
            return p

    @property
    def info(self):
        """ Dictionary of server information.
        """
        try:
            out = check_output("%s info" % self.script, shell=True)
        except CalledProcessError as error:
            if error.returncode == 3:
                return None
            else:
                raise OSError("An error occurred while trying to fetch "
                              "server info [%s]" % error.returncode)
        else:
            data = {}
            for line in out.decode("utf-8").splitlines(keepends=False):
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

    def update_server_properties(self, **properties):
        properties = {"org.neo4j.server." + key.replace("_", "."): value
                      for key, value in properties.items()}
        conf_filename = os.path.join(self.home, "conf", "neo4j-server.properties")
        for line in fileinput.input(conf_filename, inplace=1):
            for key, value in properties.items():
                if line.startswith(key + "="):
                    line = "%s=%s\n" % (key, value)
            sys.stdout.write(line)

    @property
    def service_root(self):
        return ServiceRoot("http://localhost:%s" % self.info["NEO4J_SERVER_PORT"])

    @property
    def graph(self):
        return self.service_root.graph

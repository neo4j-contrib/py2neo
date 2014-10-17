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


import os
from subprocess import check_output
from shutil import rmtree

from py2neo.ext.distro.core import download, dist_archive_name
from py2neo.ext.distro.env import DIST_HOST
from py2neo.packages.httpstream import NetworkAddressError
from py2neo.server import GraphServer


class NeoBox(object):

    def __init__(self, home=None):
        self.home = home or os.getenv("NEOBOX_HOME") or os.path.expanduser("~/.neobox")

    def inst_path(self, name):
        return os.path.join(self.home, "inst", name)

    def server_home(self, name):
        return os.path.join(self.inst_path(name), "neo4j")

    def dist_path(self):
        return os.path.join(self.home, "dist")

    def ports_path(self):
        return os.path.join(self.home, "ports")

    def ensure_downloaded(self, edition, version):
        dist_path = self.dist_path()
        try:
            os.makedirs(dist_path)
        except FileExistsError:
            pass
        filename = os.path.join(dist_path, dist_archive_name(edition, version))
        if os.path.isfile(filename):
            return filename
        try:
            return download(edition, version, dist_path)
        except NetworkAddressError:
            raise RuntimeError("Not able to connect to %s" % DIST_HOST)

    def server_list(self):
        ports_path = self.ports_path()
        try:
            os.makedirs(ports_path)
        except FileExistsError:
            pass
        ports = [port for port in os.listdir(ports_path)]
        return {os.path.basename(os.readlink(os.path.join(ports_path, port))): int(port)
                for port in ports}

    def get_server(self, name):
        server_home = self.server_home(name)
        if not os.path.exists(server_home):
            raise ValueError("No server instance named %r exists" % name)
        return GraphServer(server_home)

    def make_server(self, name, edition, version):
        inst_path = self.inst_path(name)
        if os.path.exists(inst_path):
            raise ValueError("A server instance named %r already exists" % name)
        filename = self.ensure_downloaded(edition, version)
        os.makedirs(inst_path)
        # The Python tarfile module doesn't seem to recognise the Neo4j tar format.
        check_output("tar -x -C \"%s\" -f \"%s\"" % (inst_path, filename), shell=True)
        server_home = self.server_home(name)
        os.symlink(os.listdir(inst_path)[0], server_home)
        port = self._assign_port(name)
        server = self.get_server(name)
        server.update_server_properties(webserver_port=port, webserver_https_port=(port + 1))
        return server

    def _assign_port(self, name, port=None):
        if not port:
            ports = self.server_list().values()
            if ports:
                port = max(ports) + 2
            else:
                port = 47470
        os.symlink(os.path.join("..", "inst", name), os.path.join(self.ports_path(), str(port)))
        return port

    def remove_server(self, name, force=False):
        try:
            server = self.get_server(name)
        except ValueError:
            if not force:
                raise
        else:
            if server.pid and not force:
                raise RuntimeError("Cannot remove a running server instance")
        self._remove_port(name)
        rmtree(self.inst_path(name), ignore_errors=(not force))

    def _remove_port(self, name):
        servers = self.server_list()
        try:
            port = servers[name]
        except KeyError:
            pass
        else:
            os.remove(os.path.join(self.ports_path(), str(port)))

    def rename_server(self, name, new_name):
        inst_path = self.inst_path(name)
        if not os.path.isdir(inst_path):
            raise ValueError("No server instance named %r exists" % name)
        new_inst_path = self.inst_path(new_name)
        if os.path.isdir(new_inst_path):
            raise ValueError("A server instance named %r already exists" % new_name)
        port = self.server_list()[name]
        self._remove_port(name)
        os.rename(inst_path, new_inst_path)
        self._assign_port(new_name, port)

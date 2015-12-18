#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright 2011-2015, Nigel Small
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


from py2neo import Resource
from py2neo.ext import ServerPlugin, UnmanagedExtension
from test.util import Py2neoTestCase
from test.compat import patch


class FakePlugin(ServerPlugin):

    def __init__(self, graph):
        super(FakePlugin, self).__init__(graph, "FakePlugin")


class NaughtyPlugin(ServerPlugin):

    def __init__(self, graph):
        super(NaughtyPlugin, self).__init__(graph, "NaughtyPlugin")


class ServerPluginTestCase(Py2neoTestCase):

    def test_can_init_server_plugin(self):
        metadata = self.graph.resource.metadata
        metadata["extensions"]["FakePlugin"] = {}
        self.graph.resource = Resource(self.graph.uri, metadata)
        plugin = FakePlugin(self.graph)
        assert plugin.resources == {}

    def test_cannot_init_non_existent_server_plugin(self):
        with self.assertRaises(LookupError):
            NaughtyPlugin(self.graph)


class FakeExtension(UnmanagedExtension):

    def __init__(self, graph):
        super(FakeExtension, self).__init__(graph, "/fake/")


class NaughtyExtension(UnmanagedExtension):

    def __init__(self, graph):
        super(NaughtyExtension, self).__init__(graph, "/naughty/")


class UnmanagedExtensionTestCase(Py2neoTestCase):

    def test_can_init_unmanaged_extension(self):
        with patch("py2neo.core.Resource.get"):
            plugin = FakeExtension(self.graph)
            assert plugin.resource.uri == "http://localhost:7474/fake/"

    def test_cannot_init_non_existent_server_plugin(self):
        with self.assertRaises(NotImplementedError):
            NaughtyExtension(self.graph)

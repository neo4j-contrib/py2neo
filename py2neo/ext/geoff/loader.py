#!/usr/bin/env python
# -*- coding: utf-8 -*-

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


import json

from py2neo.core import Resource, UnmanagedExtension
from py2neo.util import version_tuple

from py2neo.ext.geoff.xmlutil import xml_to_geoff


__all__ = ["GeoffLoader", "LoadedSubgraph"]


class LoadedSubgraph(object):

    def __init__(self, graph, data):
        self.graph = graph
        self.__data = data

    def __len__(self):
        return len(self.__data)

    def __iter__(self):
        return iter(self.__data)

    def __getitem__(self, item):
        return self.graph.node(self.__data[item])

    def get_ref(self, item):
        return "node/%s" % self.__data[item]

    def keys(self):
        return self.__data.keys()

    def values(self):
        return self.__data.values()

    def items(self):
        return self.__data.items()


class GeoffLoader(UnmanagedExtension):

    DEFAULT_PATH = "/load2neo/"

    def __init__(self, graph, path=DEFAULT_PATH):
        UnmanagedExtension.__init__(self, graph, path)
        self.geoff_loader = Resource(self.resource.metadata["geoff_loader"])

    @property
    def load2neo_version(self):
        return version_tuple(self.resource.metadata["load2neo_version"])

    def load(self, string):
        rs = self.geoff_loader.post(string)
        return [LoadedSubgraph(self.graph, json.loads(line))
                for line in rs.content.splitlines(False)]

    def load_xml(self, string):
        return self.load(xml_to_geoff(string))

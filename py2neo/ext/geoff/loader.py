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


from __future__ import unicode_literals

import json

from py2neo.error import GraphError
from py2neo.core import Bindable, Resource


class Loader(Bindable):

    def __init__(self, graph):
        Bindable.__init__(self, graph.service_root.uri.resolve("load2neo"))
        try:
            self.__load2neo_version = self.resource.metadata["load2neo_version"]
        except GraphError:
            raise NotImplementedError("Load2neo extension not available")
        self.__geoff_loader = Resource(self.resource.metadata["geoff_loader"])

    @property
    def load2neo_version(self):
        return self.__load2neo_version

    def load_geoff(self, geoff):
        """ Load Geoff data via the load2neo extension.

        >>> from py2neo import Graph
        >>> from py2neo.ext.geoff import Loader
        >>> graph = Graph()
        >>> loader = Loader(graph)
        >>> loader.load_geoff("(alice)<-[:KNOWS]->(bob)")
        [{'alice': (N1), 'bob': (N2)}]

        :param geoff: geoff data to load
        :return: list of node mappings

        """
        return [
            {key: self.graph.node(value) for key, value in json.loads(line).items()}
            for line in self.__geoff_loader.post(geoff).content.splitlines(keepends=False)
        ]

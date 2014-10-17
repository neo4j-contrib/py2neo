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


import codecs

from py2neo.ext.load.load2neo import Load2Neo
from py2neo.ext.load.xmlutil import xml_to_cypher


class GraphLoader(object):

    def __init__(self, graph):
        self.graph = graph
        try:
            self.__load2neo = Load2Neo(graph)
        except NotImplementedError:
            self.__load2neo = NotImplemented

    @property
    def load2neo(self):
        if self.__load2neo is NotImplemented:
            raise NotImplementedError("The load2neo extension is not installed on this server")
        else:
            return self.__load2neo

    def load_geoff(self, string):
        return self.load2neo.load_geoff(string)

    def load_geoff_file(self, filename):
        with codecs.open(filename, encoding="utf-8") as f:
            return self.load_geoff(f.read())

    def load_xml(self, string):
        statement = xml_to_cypher(string)
        self.graph.cypher.run(statement)

    def load_xml_file(self, filename):
        with codecs.open(filename, encoding="utf-8") as f:
            return self.load_xml(f.read())

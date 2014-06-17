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

""" The neo4j module provides the main `Neo4j <http://neo4j.org/>`_ client
functionality and will be the starting point for most applications. The main
classes provided are:

- :py:class:`Graph` - an instance of a Neo4j database server,
  providing a number of graph-global methods for handling nodes and
  relationships
- :py:class:`Node` - a representation of a database node
- :py:class:`Relationship` - a representation of a relationship between two
  database nodes
- :py:class:`Path` - a sequence of alternating nodes and relationships
- :py:class:`ReadBatch` - a batch of read requests to be carried out within a
  single transaction
- :py:class:`WriteBatch` - a batch of write requests to be carried out within
  a single transaction
"""


from __future__ import division, unicode_literals

from collections import namedtuple
from datetime import datetime

from py2neo.core import Bindable, Resource, ServiceRoot
from py2neo.util import numberise


class Monitor(Bindable):

    __instances = {}

    def __new__(cls, uri=None):
        """ Fetch a cached instance if one is available, otherwise create,
        cache and return a new instance.

        :param uri: URI of the cached resource
        :return: a resource instance
        """
        inst = super(Monitor, cls).__new__(cls, uri)
        return cls.__instances.setdefault(uri, inst)

    def __init__(self, uri=None):
        if uri is None:
            service_root = ServiceRoot()
            manager = Resource(service_root.resource.metadata["management"])
            monitor = Monitor(manager.metadata["services"]["monitor"])
            uri = monitor.resource.uri
        Bindable.__init__(self, uri)

    def fetch_latest_stats(self):
        """ Fetch the latest server statistics as a list of 2-tuples, each
        holding a `datetime` object and a named tuple of node, relationship and
        property counts.
        """
        counts = namedtuple("Stats", ("node_count",
                                      "relationship_count",
                                      "property_count"))
        uri = self.resource.metadata["resources"]["latest_data"]
        latest_data = Resource(uri).get().content
        timestamps = latest_data["timestamps"]
        data = latest_data["data"]
        data = zip(
            (datetime.fromtimestamp(t) for t in timestamps),
            (counts(*x) for x in zip(
                (numberise(n) for n in data["node_count"]),
                (numberise(n) for n in data["relationship_count"]),
                (numberise(n) for n in data["property_count"]),
            )),
        )
        return data

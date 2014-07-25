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

import logging

from py2neo.core import Service
from py2neo.cypher.error import CypherError
from py2neo.cypher.results import IterableCypherResults


log = logging.getLogger("py2neo.cypher")


class CypherResource(Service):

    error_class = CypherError

    __instances = {}

    def __new__(cls, uri):
        try:
            inst = cls.__instances[uri]
        except KeyError:
            inst = super(CypherResource, cls).__new__(cls)
            inst.bind(uri)
            cls.__instances[uri] = inst
        return inst

    def post(self, query, params=None):
        if __debug__:
            log.debug("Query: " + repr(query))
        payload = {"query": query}
        if params:
            if __debug__:
                log.debug("Params: " + repr(params))
            payload["params"] = params
        return self.resource.post(payload)

    def run(self, query, params=None):
        self.post(query, params).close()

    def execute(self, query, params=None):
        response = self.post(query, params)
        try:
            return self.graph.hydrate(response.content)
        finally:
            response.close()

    def execute_one(self, query, params=None):
        # TODO: make sure post response is closed
        try:
            return self.execute(query, params).data[0][0]
        except IndexError:
            return None

    def stream(self, query, params=None):
        """ Execute the query and return a result iterator.
        """
        return IterableCypherResults(self.graph, self.post(query, params))


# TODO: rework to use Cypher resource
class CypherQuery(object):
    """ A reusable Cypher query. To create a new query object, a graph and the
    query text need to be supplied::

        >>> from py2neo import Graph
        >>> from py2neo.cypher.simple import CypherQuery
        >>> graph = Graph()
        >>> query = CypherQuery(graph, "CREATE (a) RETURN a")

    """

    def __init__(self, graph, query):
        self.graph = graph
        self.query = query

    def __repr__(self):
        return self.query

    @property
    def string(self):
        """ The text of the query.
        """
        return self.query

    def post(self, **params):
        return self.graph.cypher.post(self.query, params)

    def run(self, **params):
        self.graph.cypher.run(self.query, params)

    def execute(self, **params):
        return self.graph.cypher.execute(self.query, params)

    def execute_one(self, **params):
        return self.graph.cypher.execute_one(self.query, params)

    def stream(self, **params):
        return self.graph.cypher.stream(self.query, params)

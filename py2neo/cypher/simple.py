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

from py2neo.core import Service, Node, Relationship
from py2neo.cypher.error import CypherError
from py2neo.cypher.results import IterableCypherResults


__all__ = ["CypherResource"]


log = logging.getLogger("py2neo.cypher")


class CypherResource(Service):
    """ Wrapper for the standard Cypher endpoint, providing
    non-transactional Cypher execution capabilities. Instances
    of this class will generally be created by and accessed via
    the associated Graph object::

        from py2neo import Graph
        graph = Graph()
        results = graph.cypher.execute("MATCH (n:Person) RETURN n")

    """

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
        log.debug("Query: " + repr(query))
        payload = {"query": query}
        if params:
            log.debug("Params: " + repr(params))
            payload["params"] = {}
            for key, value in params.items():
                if isinstance(value, (Node, Relationship)):
                    value = value._id
                payload["params"][key] = value
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
        response = self.post(query, params)
        results = self.graph.hydrate(response.content)
        try:
            return results.data[0][0]
        except IndexError:
            return None
        finally:
            response.close()

    def stream(self, query, params=None):
        """ Execute the query and return a result iterator.
        """
        return IterableCypherResults(self.graph, self.post(query, params))

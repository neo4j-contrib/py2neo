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

from py2neo.core import Bindable
from py2neo.error import ClientError
from py2neo.cypher.error import CypherError
from py2neo.cypher.results import CypherResults, IterableCypherResults


log = logging.getLogger("cypher")


class Cypher(Bindable):

    __instances = {}

    def __new__(cls, uri):
        try:
            inst = cls.__instances[uri]
        except KeyError:
            inst = super(Cypher, cls).__new__(cls)
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
        try:
            response = self.resource.post(payload)
        except ClientError as e:
            if e.exception:
                # A CustomCypherError is a dynamically created subclass of
                # CypherError with the same name as the underlying server
                # exception
                CustomCypherError = type(str(e.exception), (CypherError,), {})
                raise CustomCypherError(e)
            else:
                raise CypherError(e)
        else:
            return response

    def execute(self, query, params=None):
        return CypherResults(self.graph, self.post(query, params))

    def execute_one(self, query, params=None):
        try:
            return self.execute(query, params).data[0][0]
        except IndexError:
            return None

    def stream(self, query, params=None):
        """ Execute the query and return a result iterator.
        """
        return IterableCypherResults(self.graph, self.post(query, params))


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
        self.__cypher_resource = self.graph.cypher.resource
        self.query = query

    def __repr__(self):
        return self.query

    @property
    def string(self):
        """ The text of the query.
        """
        return self.query

    def post(self, **params):
        if __debug__:
            log.debug("Query: " + repr(self.query))
            if params:
                log.debug("Params: " + repr(params))
        try:
            response = self.__cypher_resource.post({"query": self.query, "params": params})
        except ClientError as e:
            if e.exception:
                # A CustomCypherError is a dynamically created subclass of
                # CypherError with the same name as the underlying server
                # exception
                CustomCypherError = type(str(e.exception), (CypherError,), {})
                raise CustomCypherError(e)
            else:
                raise CypherError(e)
        else:
            return response

    def run(self, **params):
        """ Execute the query and discard any results.

        :param params:
        """
        self.post(**params).close()

    def execute(self, **params):
        """ Execute the query and return the results.

        :param params:
        :return:
        :rtype: :py:class:`CypherResults <py2neo.neo4j.CypherResults>`
        """
        return CypherResults(self.graph, self.post(**params))

    def execute_one(self, **params):
        """ Execute the query and return the first value from the first row.

        :param params:
        :return:
        """
        try:
            return self.execute(**params).data[0][0]
        except IndexError:
            return None

    def stream(self, **params):
        """ Execute the query and return a result iterator.

        :param params:
        :return:
        :rtype: :py:class:`IterableCypherResults <py2neo.neo4j.IterableCypherResults>`
        """
        return IterableCypherResults(self.graph, self.post(**params))

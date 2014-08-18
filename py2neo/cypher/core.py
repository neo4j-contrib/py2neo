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

from collections import OrderedDict
import logging

from py2neo.core import Service, Resource, Node, Rel, Relationship
from py2neo.cypher.error import CypherError, TransactionError, TransactionFinished
from py2neo.cypher.results import IterableCypherResults, RecordProducer


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

    def __new__(cls, uri, transaction_uri=None):
        try:
            inst = cls.__instances[uri]
        except KeyError:
            inst = super(CypherResource, cls).__new__(cls)
            inst.bind(uri)
            inst.transaction_uri = transaction_uri
            cls.__instances[uri] = inst
        return inst

    def post(self, query, params=None):
        log.debug("Query: %r", query)
        payload = {"query": query}
        if params:
            payload["params"] = {}
            for key, value in params.items():
                if isinstance(value, (Node, Rel, Relationship)):
                    value = value._id
                payload["params"][key] = value
            log.debug("Params: %r", payload["params"])
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

    def begin(self):
        if self.transaction_uri:
            return CypherTransaction(self.transaction_uri)
        else:
            raise NotImplementedError("Transaction support not available from this Neo4j server version")


class CypherTransaction(object):
    """ A transaction is a transient resource that allows multiple Cypher
    statements to be executed within a single server transaction.
    """

    def __init__(self, uri):
        self.statements = []
        self.__begin = Resource(uri)
        self.__begin_commit = Resource(uri + "/commit")
        self.__execute = None
        self.__commit = None
        self.__finished = False

    def __assert_unfinished(self):
        if self.__finished:
            raise TransactionFinished()

    @property
    def finished(self):
        """ Indicates whether or not this transaction has been completed or is
        still open.

        :return: :py:const:`True` if this transaction has finished,
                 :py:const:`False` otherwise
        """
        return self.__finished

    def append(self, statement, parameters=None):
        """ Append a statement to the current queue of statements to be
        executed.

        :param statement: the statement to execute
        :param parameters: a dictionary of execution parameters
        """
        self.__assert_unfinished()
        # OrderedDict is used here to avoid statement/parameters ordering bug
        self.statements.append(OrderedDict([
            ("statement", statement),
            ("parameters", dict(parameters or {})),
            ("resultDataContents", ["REST"]),
        ]))

    def post(self, resource):
        self.__assert_unfinished()
        rs = resource.post({"statements": self.statements})
        location = dict(rs.headers).get("location")
        if location:
            self.__execute = Resource(location)
        j = rs.content
        rs.close()
        self.statements = []
        if "commit" in j:
            self.__commit = Resource(j["commit"])
        if "errors" in j:
            errors = j["errors"]
            if len(errors) >= 1:
                error = errors[0]
                raise TransactionError.new(error["code"], error["message"])
        out = []
        for result in j["results"]:
            producer = RecordProducer(result["columns"])
            out.append([
                producer.produce(self.__begin.service_root.graph.hydrate(r["rest"]))
                for r in result["data"]
            ])
        return out

    def execute(self):
        """ Send all pending statements to the server for execution, leaving
        the transaction open for further statements.

        :return: list of results from pending statements
        """
        return self.post(self.__execute or self.__begin)

    def commit(self):
        """ Send all pending statements to the server for execution and commit
        the transaction.

        :return: list of results from pending statements
        """
        try:
            return self.post(self.__commit or self.__begin_commit)
        finally:
            self.__finished = True

    def rollback(self):
        """ Rollback the current transaction.
        """
        self.__assert_unfinished()
        try:
            if self.__execute:
                self.__execute.delete()
        finally:
            self.__finished = True

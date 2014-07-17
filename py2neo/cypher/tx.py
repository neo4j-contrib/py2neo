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

from py2neo.core import Resource, ServiceRoot
from py2neo.cypher.error import TransactionError, TransactionFinished
from py2neo.cypher.results import RecordProducer
from py2neo.packages.httpstream.packages.urimagic import URI


class Session(object):
    """ A Session is the base object from which Cypher transactions are
    created and is instantiated using a root service URI. If unspecified, this
    defaults to the `DEFAULT_URI`.

    ::

        >>> from py2neo import cypher
        >>> session = cypher.Session("http://arthur:excalibur@camelot:9999")

    """

    def __init__(self, uri=None):
        self._uri = URI(uri or ServiceRoot.DEFAULT_URI)
        if self._uri.user_info:
            service_root_uri = "{0}://{1}@{2}:{3}/".format(self._uri.scheme, self._uri.user_info, self._uri.host, self._uri.port)
        else:
            service_root_uri = "{0}://{1}:{2}/".format(self._uri.scheme, self._uri.host, self._uri.port)
        self._service_root = ServiceRoot(service_root_uri)
        self._graph = self._service_root.graph
        try:
            self._transaction_uri = self._graph.resource.metadata["transaction"]
        except KeyError:
            raise NotImplementedError("Cypher transactions are not supported "
                                      "by this server version")

    def create_transaction(self):
        """ Create a new transaction object.

        ::

            >>> from py2neo import cypher
            >>> session = cypher.Session()
            >>> tx = session.create_transaction()

        :return: new transaction object
        :rtype: Transaction
        """
        return Transaction(self._transaction_uri)

    def execute(self, statement, parameters=None):
        """ Execute a single statement and return the results.
        """
        tx = self.create_transaction()
        tx.append(statement, parameters)
        results = tx.commit()
        return results[0]


class Transaction(object):
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

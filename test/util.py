#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright 2011-2017, Nigel Small
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


from unittest import TestCase
from uuid import uuid4

from py2neo.graph import Graph, BoltTransaction, HTTPTransaction
from py2neo.remoting import remote
from py2neo.selection import NodeSelector
from py2neo.types import Node


def unique_string_generator():
    while True:
        yield uuid4().hex


class GraphTestCase(TestCase):

    graph = Graph()
    node_selector = NodeSelector(graph)

    def __init__(self, *args, **kwargs):
        super(GraphTestCase, self).__init__(*args, **kwargs)
        self.http_graph = Graph(bolt=False)
        self.http_graph.driver = None
        self.http_graph.transaction_class = HTTPTransaction
        self.graph_service = self.graph.graph_service
        self.schema = self.graph.schema
        self.unique_string = unique_string_generator()

        version = self.graph_service.kernel_version
        with self.graph.begin() as tx:
            if version >= (3,):
                assert isinstance(tx, BoltTransaction)
            else:
                assert isinstance(tx, HTTPTransaction)
        with self.http_graph.begin() as tx:
            assert isinstance(tx, HTTPTransaction)

    def reset(self):
        graph = self.graph
        schema = self.schema
        for label in graph.node_labels:
            for key in schema.get_uniqueness_constraints(label):
                schema.drop_uniqueness_constraint(label, key)
            for key in schema.get_indexes(label):
                schema.drop_index(label, key)
        graph.delete_all()

    def assert_error(self, error, classes, fullname):
        for cls in classes:
            assert isinstance(error, cls)
        name = fullname.rpartition(".")[-1]
        assert error.__class__.__name__ == name
        assert error.exception == name
        assert error.fullname in [None, fullname]
        assert error.stacktrace

    def assert_new_error(self, error, classes, code):
        for cls in classes:
            assert isinstance(error, cls)
        name = code.rpartition(".")[-1]
        assert error.__class__.__name__ == name
        assert error.code == code
        assert error.message

    def get_non_existent_node_id(self):
        node = Node()
        self.graph.create(node)
        node_id = remote(node)._id
        self.graph.delete(node)
        return node_id

    def get_attached_node_id(self):
        return self.graph.evaluate("CREATE (a)-[:TO]->(b) RETURN id(a)")


class TemporaryTransaction(object):

    def __init__(self, graph):
        self.tx = graph.begin()

    def __del__(self):
        try:
            self.tx.rollback()
        except:
            pass

    def run(self, statement, parameters=None, **kwparameters):
        return self.tx.run(statement, parameters, **kwparameters)

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


from os import getenv
from unittest import TestCase, SkipTest
from uuid import uuid4

from py2neo.graph import Graph
from py2neo.selection import NodeSelector
from py2neo.types.graph import Node


def unique_string_generator():
    while True:
        yield "_" + uuid4().hex


class GraphTestCase(TestCase):

    def __init__(self, *args, **kwargs):
        super(GraphTestCase, self).__init__(*args, **kwargs)
        self.graph = Graph()
        self.node_selector = NodeSelector(self.graph)
        self.db = self.graph.graph_db
        self.schema = self.graph.schema
        self.unique_string = unique_string_generator()

    def reset(self):
        graph = self.graph
        schema = self.schema
        for label in graph.node_labels:
            for property_keys in schema.get_uniqueness_constraints(label):
                schema.drop_uniqueness_constraint(label, *property_keys)
            for property_keys in schema.get_indexes(label):
                schema.drop_index(label, *property_keys)
        graph.delete_all()

    def assert_error(self, error, classes, fullname):
        for cls in classes:
            assert isinstance(error, cls)
        name = fullname.rpartition(".")[-1]
        self.assertEqual(error.__class__.__name__, error.exception, name)
        self.assertIn(error.fullname, [None, fullname])
        self.assertTrue(error.stacktrace)

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
        node_id = node.identity
        self.graph.delete(node)
        return node_id

    def get_attached_node_id(self):
        return self.graph.evaluate("CREATE (a)-[:TO]->(b) RETURN id(a)")


class HTTPGraphTestCase(GraphTestCase):

    def __init__(self, *args, **kwargs):
        super(HTTPGraphTestCase, self).__init__(*args, **kwargs)
        self.http_graph = Graph(bolt=False)

    def setUp(self):
        if getenv("NEO4J_HTTP_DISABLED"):
            raise SkipTest("HTTP disabled")


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

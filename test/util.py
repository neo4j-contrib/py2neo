#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright 2011-2015, Nigel Small
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

from py2neo import Graph, Node
from py2neo.ext.mandex import ManualIndexManager


def unique_string_generator():
    while True:
        yield uuid4().hex


class Py2neoTestCase(TestCase):

    def __init__(self, *args, **kwargs):
        super(Py2neoTestCase, self).__init__(*args, **kwargs)
        self.graph = Graph()
        self.cypher = self.graph.cypher
        self.schema = self.graph.schema
        self.index_manager = ManualIndexManager(self.graph)
        self.unique_string = unique_string_generator()

    def reset(self):
        graph = self.graph
        schema = self.schema
        for label in graph.node_labels:
            for key in schema.get_uniqueness_constraints(label):
                schema.drop_uniqueness_constraint(label, key)
            for key in schema.get_indexes(label):
                schema.drop_index(label, key)
        graph.delete_all()

    def assert_error(self, error, classes, fullname, cause_classes, status_code=None):
        for cls in classes:
            assert isinstance(error, cls)
        name = fullname.rpartition(".")[-1]
        assert error.__class__.__name__ == name
        assert error.exception == name
        assert error.fullname in [None, fullname]
        assert error.stacktrace
        cause = error.__cause__
        for cls in cause_classes:
            assert isinstance(cause, cls)
        if status_code:
            assert cause.status_code == status_code

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
        node_id = node._id
        self.graph.delete(node)
        return node_id

    def get_attached_node_id(self):
        return self.cypher.evaluate("CREATE (a)-[:TO]->(b) RETURN id(a)")


class TemporaryTransaction(object):

    def __init__(self, graph):
        self.tx = graph.cypher.begin()

    def __del__(self):
        self.tx.rollback()

    def run(self, statement, parameters=None, **kwparameters):
        return self.tx.run(statement, parameters, **kwparameters)

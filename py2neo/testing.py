#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright 2011-2018, Nigel Small
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

from pytest import main as test_main

from py2neo.admin import Warehouse, minor_versions
from py2neo.database import Database, Graph
from py2neo.selection import NodeSelector
from py2neo.types import Node


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


class IntegrationTestCase(TestCase):

    @staticmethod
    def unique_string_generator():
        while True:
            yield "_" + uuid4().hex

    def __init__(self, *args, **kwargs):
        super(IntegrationTestCase, self).__init__(*args, **kwargs)
        self.graph = Graph()
        self.node_selector = NodeSelector(self.graph)
        self.db = self.graph.database
        self.schema = self.graph.schema
        self.unique_string = self.unique_string_generator()

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
        self.assertEqual(error.__class__.__name__, name)
        self.assertEqual(error.code, code)
        self.assertTrue(error.message)

    def get_non_existent_node_id(self):
        node = Node()
        self.graph.create(node)
        node_id = node.identity
        self.graph.delete(node)
        return node_id

    def get_attached_node_id(self):
        return self.graph.evaluate("CREATE (a)-[:TO]->(b) RETURN id(a)")


def run_tests(versions, user, password):
    warehouse = Warehouse()
    for version in versions:
        name = uuid4().hex
        installation = warehouse.install(name, "community", version)
        print("Installed Neo4j community %s to %s" % (version, installation.home))
        installation.auth.update(user, password)
        pid = installation.server.start()
        print("Started Neo4j server with PID %d" % pid)
        try:
            status = test_main()
            if status != 0:
                raise RuntimeError("Tests failed with status %d" % status)
        finally:
            installation.server.stop()
            warehouse.uninstall(name)
            Database.forget_all()


def main():
    versions = list(reversed(minor_versions))
    user = "neo4j"
    password = "password"
    print("Running tests")
    run_tests(versions, user, password)


if __name__ == "__main__":
    main()

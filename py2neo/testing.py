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


from os import getenv
from unittest import TestCase
from uuid import uuid4

from _pytest.assertion.rewrite import AssertionRewritingHook
from neobolt.exceptions import ServiceUnavailable
from pytest import main as test_main

from py2neo import Graph
from py2neo.admin.dist import minor_versions
from py2neo.admin.install import Warehouse
from py2neo.data import Node
from py2neo.database import Database
from py2neo.internal.connectors import Connector
from py2neo.matching import NodeMatcher


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
        self.node_matcher = NodeMatcher(self.graph)
        self.db = self.graph.database
        self.schema = self.graph.schema
        self.unique_string = self.unique_string_generator()

    def reset(self):
        graph = self.graph
        schema = self.schema
        for label in schema.node_labels:
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


class ClusterIntegrationTestCase(TestCase):

    cluster = None

    server_version = "3.4.10"

    @classmethod
    def setUpClass(cls):
        cls.bolt_uri = getenv("PY2NEO_BOLT_URI", "bolt+routing://:17100")
        cls.bolt_routing_uri = getenv("PY2NEO_BOLT_ROUTING_URI", "bolt+routing://:17100")
        cx = Connector(cls.bolt_routing_uri, auth=("neo4j", "password"))
        try:
            cls.server_agent = cx.server_agent
        except ServiceUnavailable:
            from py2neo.admin.install import Warehouse
            from py2neo.experimental.clustering import LocalCluster
            cls.cluster = LocalCluster.install(Warehouse(), "test", cls.server_version, alpha=3)
            cls.cluster.update_auth("neo4j", "password")
            cls.cluster.start()
            cls.server_agent = cx.server_agent

    @classmethod
    def tearDownClass(cls):
        if cls.cluster:
            cls.cluster.stop()
            cls.cluster.uninstall()
            cls.cluster = None


def for_each_connector(f):

    def f_(self):
        for uri in ["bolt://:17100", "bolt+routing://:17100", "http://:17200", "https://:17300"]:
            with self.subTest(uri=uri):
                connector = Connector(uri, auth=("neo4j", "password"))
                try:
                    f(self, connector=connector)
                finally:
                    connector.close()

    return f_


class TestRunner(object):

    user = "neo4j"
    password = "password"
    quick = getenv("PY2NEO_QUICK_TEST", "")

    def __init__(self, versions):
        self.warehouse = Warehouse()
        self.versions = versions
        self._state = {}

    def _before_tests(self):
        # As we're running pytest.main repeatedly, plugins are loaded multiple
        # times. This causes an import warning, which is silenced by the code below.
        self._state["warning"] = AssertionRewritingHook._warn_already_imported
        AssertionRewritingHook._warn_already_imported = lambda *args: None
        #
        self._state["name"] = uuid4().hex
        self._state["installation"] = self.warehouse.install(self._state["name"], "community", self._state["version"])
        print("Installed Neo4j community %s to %s" % (self._state["version"], self._state["installation"].home))
        self._state["installation"].auth.update(self.user, self.password)
        pid = self._state["installation"].server.start()
        print("Started Neo4j server with PID %d" % pid)

    def _after_tests(self):
        self._state["installation"].server.stop()
        self.warehouse.uninstall(self._state["name"])
        Database.forget_all()
        # Re-enable the import warning. Pretend nothing dodgy happened.
        AssertionRewritingHook._warn_already_imported = self._state["warning"]

    def run_tests(self):
        print("Running tests")
        for self._state["version"] in self.versions:
            self._before_tests()
            try:
                status = test_main()
                if status != 0:
                    raise RuntimeError("Tests failed with status %d" % status)
            except KeyboardInterrupt:
                break
            finally:
                self._after_tests()
            if self.quick:
                break


def main():
    versions = list(reversed(minor_versions))
    TestRunner(versions).run_tests()


if __name__ == "__main__":
    main()

#!/usr/bin/env python
# -*- encoding: utf-8 -*-

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


from unittest import main, TestCase

from py2neo import Graph, Node, Relationship


class TemporaryTransaction(object):

    def __init__(self, graph):
        self.tx = graph.cypher.begin()

    def __del__(self):
        self.tx.rollback()

    def execute(self, statement, parameters=None, **kwparameters):
        self.tx.append(statement, parameters, **kwparameters)
        result, = self.tx.process()
        return result


class CypherPresubstitutionTestCase(TestCase):

    graph = None

    def setUp(self):
        self.graph = Graph()

    def new_tx(self):
        if self.graph.supports_cypher_transactions:
            return TemporaryTransaction(self.graph)
        else:
            return None

    def test_can_use_parameter_for_property_value(self):
        tx = self.new_tx()
        if tx:
            (created,), = tx.execute("CREATE (a:`Homo Sapiens` {`full name`:{v}}) RETURN a",
                                     v="Alice Smith")
            assert isinstance(created, Node)
            assert created.labels == {"Homo Sapiens"}
            assert created.properties == {"full name": "Alice Smith"}

    def test_can_use_parameter_for_property_set(self):
        tx = self.new_tx()
        if tx:
            (created,), = tx.execute("CREATE (a:`Homo Sapiens`) SET a={p} RETURN a",
                                     p={"full name": "Alice Smith"})
            assert isinstance(created, Node)
            assert created.labels == {"Homo Sapiens"}
            assert created.properties == {"full name": "Alice Smith"}

    def test_can_use_parameter_for_property_key(self):
        tx = self.new_tx()
        if tx:
            (created,), = tx.execute("CREATE (a:`Homo Sapiens` {«k»:'Alice Smith'}) RETURN a",
                                     k="full name")
            assert isinstance(created, Node)
            assert created.labels == {"Homo Sapiens"}
            assert created.properties == {"full name": "Alice Smith"}

    def test_can_use_parameter_for_node_label(self):
        tx = self.new_tx()
        if tx:
            (created,), = tx.execute("CREATE (a:«l» {`full name`:'Alice Smith'}) RETURN a",
                                     l="Homo Sapiens")
            assert isinstance(created, Node)
            assert created.labels == {"Homo Sapiens"}
            assert created.properties == {"full name": "Alice Smith"}

    def test_can_use_parameter_for_multiple_node_labels(self):
        tx = self.new_tx()
        if tx:
            (created,), = tx.execute("CREATE (a:«l» {`full name`:'Alice Smith'}) RETURN a",
                                     l=("Homo Sapiens", "Hunter", "Gatherer"))
            assert isinstance(created, Node)
            assert created.labels == {"Homo Sapiens", "Hunter", "Gatherer"}
            assert created.properties == {"full name": "Alice Smith"}

    def test_can_parameter_mixture(self):
        tx = self.new_tx()
        if tx:
            (created,), = tx.execute("CREATE (a:«l» {«k»:{v}}) RETURN a",
                                     l="Homo Sapiens", k="full name", v="Alice Smith")
            assert isinstance(created, Node)
            assert created.labels == {"Homo Sapiens"}
            assert created.properties == {"full name": "Alice Smith"}

    def test_can_use_parameter_for_relationship_type(self):
        tx = self.new_tx()
        if tx:
            (created,), = tx.execute("CREATE (a)-[ab:«t»]->(b) RETURN ab",
                                     t="REALLY LIKES")
            assert isinstance(created, Relationship)
            assert created.type == "REALLY LIKES"

    def test_fails_properly_if_presubstitution_key_does_not_exist(self):
        tx = self.new_tx()
        if tx:
            try:
                tx.execute("CREATE (a)-[ab:«t»]->(b) RETURN ab")
            except KeyError:
                assert True
            else:
                assert False


if __name__ == "__main__":
    main()

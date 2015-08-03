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

from py2neo import Graph, NEO4J_HTTP_URI
from py2neo.cypher.core import presubstitute


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
        self.graph = Graph(NEO4J_HTTP_URI)

    def new_tx(self):
        if self.graph.supports_cypher_transactions:
            return TemporaryTransaction(self.graph)
        else:
            return None

    def test_can_use_parameter_for_property_value(self):
        tx = self.new_tx()
        if tx:
            result, = tx.execute("CREATE (a:`Homo Sapiens` {`full name`:{v}}) "
                                 "RETURN labels(a), a.`full name`",
                                 v="Alice Smith")
            assert set(result[0]) == {"Homo Sapiens"}
            assert result[1] == "Alice Smith"

    def test_can_use_parameter_for_property_set(self):
        tx = self.new_tx()
        if tx:
            result, = tx.execute("CREATE (a:`Homo Sapiens`) SET a={p} "
                                 "RETURN labels(a), a.`full name`",
                                 p={"full name": "Alice Smith"})
            assert set(result[0]) == {"Homo Sapiens"}
            assert result[1] == "Alice Smith"

    def test_can_use_parameter_for_property_key(self):
        tx = self.new_tx()
        if tx:
            result, = tx.execute("CREATE (a:`Homo Sapiens` {«k»:'Alice Smith'}) "
                                 "RETURN labels(a), a.`full name`",
                                 k="full name")
            assert set(result[0]) == {"Homo Sapiens"}
            assert result[1] == "Alice Smith"

    def test_can_use_parameter_for_node_label(self):
        tx = self.new_tx()
        if tx:
            result, = tx.execute("CREATE (a:«l» {`full name`:'Alice Smith'}) "
                                 "RETURN labels(a), a.`full name`",
                                 l="Homo Sapiens")
            assert set(result[0]) == {"Homo Sapiens"}
            assert result[1] == "Alice Smith"

    def test_can_use_parameter_for_multiple_node_labels(self):
        tx = self.new_tx()
        if tx:
            result, = tx.execute("CREATE (a:«l» {`full name`:'Alice Smith'}) "
                                 "RETURN labels(a), a.`full name`",
                                 l=("Homo Sapiens", "Hunter", "Gatherer"))
            assert set(result[0]) == {"Homo Sapiens", "Hunter", "Gatherer"}
            assert result[1] == "Alice Smith"

    def test_can_use_parameter_mixture(self):
        statement = u"CREATE (a:«l» {«k»:{v}})"
        parameters = {"l": "Homo Sapiens", "k": "full name", "v": "Alice Smith"}
        s, p = presubstitute(statement, parameters)
        assert s == "CREATE (a:`Homo Sapiens` {`full name`:{v}})"
        assert p == {"v": "Alice Smith"}

    def test_can_use_simple_parameter_for_relationship_type(self):
        statement = u"CREATE (a)-[ab:«t»]->(b)"
        parameters = {"t": "REALLY_LIKES"}
        s, p = presubstitute(statement, parameters)
        assert s == "CREATE (a)-[ab:REALLY_LIKES]->(b)"
        assert p == {}

    def test_can_use_parameter_with_special_character_for_relationship_type(self):
        statement = u"CREATE (a)-[ab:«t»]->(b)"
        parameters = {"t": "REALLY LIKES"}
        s, p = presubstitute(statement, parameters)
        assert s == "CREATE (a)-[ab:`REALLY LIKES`]->(b)"
        assert p == {}

    def test_can_use_parameter_with_backtick_for_relationship_type(self):
        statement = u"CREATE (a)-[ab:«t»]->(b)"
        parameters = {"t": "REALLY `LIKES`"}
        s, p = presubstitute(statement, parameters)
        assert s == "CREATE (a)-[ab:`REALLY ``LIKES```]->(b)"
        assert p == {}

    def test_can_use_parameter_for_relationship_count(self):
        statement = u"MATCH (a)-[ab:KNOWS*«x»]->(b)"
        parameters = {"x": 3}
        s, p = presubstitute(statement, parameters)
        assert s == "MATCH (a)-[ab:KNOWS*3]->(b)"
        assert p == {}

    def test_can_use_parameter_for_relationship_count_range(self):
        statement = u"MATCH (a)-[ab:KNOWS*«x»]->(b)"
        parameters = {"x": (3, 5)}
        s, p = presubstitute(statement, parameters)
        assert s == "MATCH (a)-[ab:KNOWS*3..5]->(b)"
        assert p == {}

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

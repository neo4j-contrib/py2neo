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


from io import StringIO

from py2neo.graph import Node, Relationship, Path
from py2neo.cypher import CypherEngine, Transaction, presubstitute, CypherWriter, cypher_repr
from py2neo.status import CypherError
from test.util import Py2neoTestCase, TemporaryTransaction


class CypherTestCase(Py2neoTestCase):

    def setUp(self):
        a = Node(name="Alice", age=66)
        b = Node(name="Bob", age=77)
        ab = Relationship(a, "KNOWS", b)
        self.graph.create(ab)
        self.alice_and_bob = (a, b, ab)

    def test_can_get_list_of_records(self):
        records = list(self.cypher.run("RETURN 1").collect())
        assert len(records) == 1
        first = records[0]
        assert len(first) == 1
        value = first[0]
        assert value == 1

    def test_can_cursor_through_records(self):
        cursor = self.cypher.run("RETURN 1")
        for record in cursor.collect():
            assert record.values() == (1,)

    def test_can_run_cypher_statement(self):
        self.cypher.run("MERGE (a:Person {name:'Alice'})")

    def test_can_run_parametrised_cypher_statement(self):
        self.cypher.run("MERGE (a:Person {name:{N}})", {"N": "Alice"})

    def test_can_run_cypher_statement_1(self):
        value = self.cypher.evaluate("MERGE (a:Person {name:'Alice'}) RETURN a")
        assert isinstance(value, Node)
        assert value.labels() == {"Person"}
        assert dict(value) == {"name": "Alice"}

    def test_can_run_parametrised_cypher_statement_1(self):
        value = self.cypher.evaluate("MERGE (a:Person {name:{N}}) RETURN a", {"N": "Alice"})
        assert isinstance(value, Node)
        assert value.labels() == {"Person"}
        assert dict(value) == {"name": "Alice"}

    def test_can_run_cypher_statement_with_node_parameter(self):
        alice = Node(name="Alice")
        self.graph.create(alice)
        statement = "MATCH (a) WHERE id(a) = {N} RETURN a"
        a = self.cypher.run(statement, {"N": alice}).evaluate("a")
        assert a is alice

    def test_can_evaluate_cypher_statement(self):
        value = self.cypher.evaluate("MERGE (a:Person {name:'Alice'}) RETURN a")
        assert isinstance(value, Node)
        assert value.labels() == {"Person"}
        assert dict(value) == {"name": "Alice"}

    def test_can_evaluate_parametrised_cypher_statement(self):
        value = self.cypher.evaluate("MERGE (a:Person {name:{N}}) RETURN a", {"N": "Alice"})
        assert isinstance(value, Node)
        assert value.labels() == {"Person"}
        assert dict(value) == {"name": "Alice"}

    def test_evaluate_with_no_results_returns_none(self):
        value = self.cypher.evaluate("CREATE (a {name:{N}})", {"N": "Alice"})
        assert value is None

    def test_can_begin_transaction(self):
        uri = "http://localhost:7474/db/data/transaction"
        cypher = CypherEngine(uri)
        tx = cypher.begin()
        assert isinstance(tx, Transaction)

    def test_nonsense_query(self):
        statement = "SELECT z=nude(0) RETURNS x"
        try:
            self.cypher.run(statement)
        except CypherError as error:
            assert error.code == "Neo.ClientError.Statement.InvalidSyntax"
        else:
            assert False

    def test_can_run_statement(self):
        cursor = self.cypher.run("CREATE (a {name:'Alice'}) RETURN a.name AS name")
        records = list(cursor.collect())
        assert len(records) == 1
        assert records[0]["name"] == "Alice"

    def test_can_run_with_parameter(self):
        cursor = self.cypher.run("CREATE (a {name:{N}}) "
                                 "RETURN a.name AS name", {"N": "Alice"})
        records = list(cursor.collect())
        assert len(records) == 1
        assert records[0]["name"] == "Alice"

    def test_can_run_with_entity_parameter(self):
        alice = Node(name="Alice")
        self.graph.create(alice)
        statement = "MATCH (a) WHERE id(a)={N} RETURN a.name AS name"
        cursor = self.cypher.run(statement, {"N": alice})
        records = list(cursor.collect())
        assert len(records) == 1
        assert records[0]["name"] == "Alice"

    def test_can_evaluate(self):
        value = self.cypher.evaluate("CREATE (a {name:'Alice'}) RETURN a.name AS name")
        assert value == "Alice"

    def test_can_evaluate_where_none_returned(self):
        statement = "MATCH (a) WHERE 2 + 2 = 5 RETURN a.name AS name"
        value = self.cypher.evaluate(statement)
        assert value is None

    def test_nonsense_query_with_error_handler(self):
        with self.assertRaises(CypherError):
            self.cypher.run("SELECT z=nude(0) RETURNS x")

    def test_query(self):
        a, b, ab = self.alice_and_bob
        statement = ("MATCH (a) WHERE id(a)={A} "
                     "MATCH (b) WHERE id(b)={B} "
                     "MATCH (a)-[ab:KNOWS]->(b) "
                     "RETURN a, b, ab, a.name AS a_name, b.name AS b_name")
        cursor = self.cypher.run(statement, {"A": a, "B": b})
        records = list(cursor.collect())
        assert len(records) == 1
        for record in records:
            assert isinstance(record["a"], Node)
            assert isinstance(record["b"], Node)
            assert isinstance(record["ab"], Relationship)
            assert record["a_name"] == "Alice"
            assert record["b_name"] == "Bob"

    def test_query_can_return_path(self):
        a, b, ab = self.alice_and_bob
        statement = ("MATCH (a) WHERE id(a)={A} "
                     "MATCH (b) WHERE id(b)={B} "
                     "MATCH p=((a)-[ab:KNOWS]->(b)) "
                     "RETURN p")
        cursor = self.cypher.run(statement, {"A": a, "B": b})
        records = list(cursor.collect())
        assert len(records) == 1
        for record in records:
            assert isinstance(record["p"], Path)
            nodes = record["p"].nodes()
            assert len(nodes) == 2
            assert nodes[0] == a
            assert nodes[1] == b
            assert record["p"][0].type() == "KNOWS"

    def test_query_can_return_collection(self):
        node = Node()
        self.graph.create(node)
        statement = "MATCH (a) WHERE id(a)={N} RETURN collect(a) AS a_collection"
        params = {"N": node}
        cursor = self.cypher.run(statement, params)
        assert cursor.select()["a_collection"] == [node]

    def test_param_used_once(self):
        node = Node()
        self.graph.create(node)
        statement = "MATCH (a) WHERE id(a)={X} RETURN a"
        params = {"X": node}
        cursor = self.cypher.run(statement, params)
        record = cursor.select()
        assert record["a"] == node

    def test_param_used_twice(self):
        node = Node()
        self.graph.create(node)
        statement = "MATCH (a) WHERE id(a)={X} MATCH (b) WHERE id(b)={X} RETURN a, b"
        params = {"X": node}
        cursor = self.cypher.run(statement, params)
        record = cursor.select()
        assert record["a"] == node
        assert record["b"] == node

    def test_param_used_thrice(self):
        node = Node()
        self.graph.create(node)
        statement = "MATCH (a) WHERE id(a)={X} " \
                    "MATCH (b) WHERE id(b)={X} " \
                    "MATCH (c) WHERE id(c)={X} " \
                    "RETURN a, b, c"
        params = {"X": node}
        cursor = self.cypher.run(statement, params)
        record = cursor.select()
        assert record["a"] == node
        assert record["b"] == node
        assert record["c"] == node

    def test_param_reused_once_after_with_statement(self):
        a, b, ab = self.alice_and_bob
        query = ("MATCH (a) WHERE id(a)={A} "
                 "MATCH (a)-[:KNOWS]->(b) "
                 "WHERE a.age > {min_age} "
                 "WITH a "
                 "MATCH (a)-[:KNOWS]->(b) "
                 "WHERE b.age > {min_age} "
                 "RETURN b")
        params = {"A": a, "min_age": 50}
        record = self.cypher.run(query, params).select()
        assert record["b"] == b

    def test_param_reused_twice_after_with_statement(self):
        a, b, ab = self.alice_and_bob
        c = Node(name="Carol", age=88)
        bc = Relationship(b, "KNOWS", c)
        self.graph.create(c | bc)
        query = ("MATCH (a) WHERE id(a)={A} "
                 "MATCH (a)-[:KNOWS]->(b) "
                 "WHERE a.age > {min_age} "
                 "WITH a "
                 "MATCH (a)-[:KNOWS]->(b) "
                 "WHERE b.age > {min_age} "
                 "WITH b "
                 "MATCH (b)-[:KNOWS]->(c) "
                 "WHERE c.age > {min_age} "
                 "RETURN c")
        params = {"A": a, "min_age": 50}
        record = self.cypher.run(query, params).select()
        assert record["c"] == c

    def test_invalid_syntax_raises_cypher_error(self):
        cypher = self.cypher
        try:
            cypher.run("X")
        except CypherError as error:
            assert error.code == "Neo.ClientError.Statement.InvalidSyntax"
        else:
            assert False

    def test_unique_path_not_unique_raises_cypher_error(self):
        cypher = self.cypher
        cursor = cypher.run("CREATE (a), (b) RETURN a, b")
        record = cursor.select()
        parameters = {"A": record["a"], "B": record["b"]}
        statement = ("MATCH (a) WHERE id(a)={A} MATCH (b) WHERE id(b)={B}" +
                     "CREATE (a)-[:KNOWS]->(b)")
        cypher.run(statement, parameters)
        cypher.run(statement, parameters)
        try:
            statement = ("MATCH (a) WHERE id(a)={A} MATCH (b) WHERE id(b)={B}" +
                         "CREATE UNIQUE (a)-[:KNOWS]->(b)")
            cypher.run(statement, parameters)
        except CypherError as error:
            assert error.code == "Neo.ClientError.Statement.ConstraintViolation"
        else:
            assert False


class CypherCreateTestCase(Py2neoTestCase):

    def test_can_create_node(self):
        a = Node("Person", name="Alice")
        self.cypher.create(a)
        assert a.remote

    def test_can_create_relationship(self):
        a = Node("Person", name="Alice")
        b = Node("Person", name="Bob")
        r = Relationship(a, "KNOWS", b, since=1999)
        self.cypher.create(r)
        assert a.remote
        assert b.remote
        assert r.remote
        assert r.start_node() == a
        assert r.end_node() == b

    def test_can_create_nodes_and_relationship(self):
        self.graph.delete_all()
        a = Node()
        b = Node()
        c = Node()
        ab = Relationship(a, "TO", b)
        bc = Relationship(b, "TO", c)
        ca = Relationship(c, "TO", a)
        self.cypher.create(ab | bc | ca)
        assert a.remote
        assert b.remote
        assert c.remote
        assert ab.remote
        assert ab.start_node() == a
        assert ab.end_node() == b
        assert bc.remote
        assert bc.start_node() == b
        assert bc.end_node() == c
        assert ca.remote
        assert ca.start_node() == c
        assert ca.end_node() == a
        assert self.graph.order() == 3
        assert self.graph.size() == 3


class CypherLangTestCase(Py2neoTestCase):

    def test_can_write_simple_identifier(self):
        string = StringIO()
        writer = CypherWriter(string)
        writer.write_identifier("foo")
        written = string.getvalue()
        assert written == "foo"

    def test_can_write_identifier_with_odd_chars(self):
        string = StringIO()
        writer = CypherWriter(string)
        writer.write_identifier("foo bar")
        written = string.getvalue()
        assert written == "`foo bar`"

    def test_can_write_identifier_containing_back_ticks(self):
        string = StringIO()
        writer = CypherWriter(string)
        writer.write_identifier("foo `bar`")
        written = string.getvalue()
        assert written == "`foo ``bar```"

    def test_cannot_write_empty_identifier(self):
        string = StringIO()
        writer = CypherWriter(string)
        with self.assertRaises(ValueError):
            writer.write_identifier("")

    def test_cannot_write_none_identifier(self):
        string = StringIO()
        writer = CypherWriter(string)
        with self.assertRaises(ValueError):
            writer.write_identifier(None)

    def test_can_write_simple_node(self):
        string = StringIO()
        writer = CypherWriter(string)
        writer.write(Node())
        written = string.getvalue()
        assert written == "()"

    def test_can_write_node_with_labels(self):
        string = StringIO()
        writer = CypherWriter(string)
        writer.write(Node("Dark Brown", "Chicken"))
        written = string.getvalue()
        assert written == '(:Chicken:`Dark Brown`)'

    def test_can_write_node_with_properties(self):
        string = StringIO()
        writer = CypherWriter(string)
        writer.write(Node(name="Gertrude", age=3))
        written = string.getvalue()
        assert written == '({age:3,name:"Gertrude"})'

    def test_can_write_node_with_labels_and_properties(self):
        string = StringIO()
        writer = CypherWriter(string)
        writer.write(Node("Dark Brown", "Chicken", name="Gertrude", age=3))
        written = string.getvalue()
        assert written == '(:Chicken:`Dark Brown` {age:3,name:"Gertrude"})'

    def test_can_write_simple_relationship(self):
        string = StringIO()
        writer = CypherWriter(string)
        writer.write(Relationship({}, "KNOWS", {}))
        written = string.getvalue()
        assert written == "()-[:KNOWS]->()"

    def test_can_write_relationship_with_properties(self):
        string = StringIO()
        writer = CypherWriter(string)
        writer.write(Relationship(
            {"name": "Fred"}, ("LIVES WITH", {"place": "Bedrock"}), {"name": "Wilma"}))
        written = string.getvalue()
        assert written == '({name:"Fred"})-[:`LIVES WITH` {place:"Bedrock"}]->({name:"Wilma"})'

    def test_can_write_simple_path(self):
        alice, bob, carol, dave = Node(), Node(), Node(), Node()
        path = Path(alice, "LOVES", bob, Relationship(carol, "HATES", bob), carol, "KNOWS", dave)
        string = StringIO()
        writer = CypherWriter(string)
        writer.write(path)
        written = string.getvalue()
        assert written == "()-[:LOVES]->()<-[:HATES]-()-[:KNOWS]->()"

    def test_can_write_array(self):
        string = StringIO()
        writer = CypherWriter(string)
        writer.write([1, 1, 2, 3, 5, 8, 13])
        written = string.getvalue()
        assert written == "[1,1,2,3,5,8,13]"

    def test_can_write_mapping(self):
        string = StringIO()
        writer = CypherWriter(string)
        writer.write({"one": "eins", "two": "zwei", "three": "drei"})
        written = string.getvalue()
        assert written == '{one:"eins",three:"drei",two:"zwei"}'

    def test_writing_none_writes_nothing(self):
        string = StringIO()
        writer = CypherWriter(string)
        writer.write(None)
        written = string.getvalue()
        assert written == ""

    def test_can_write_with_wrapper_function(self):
        alice, bob, carol, dave = Node(), Node(), Node(), Node()
        path = Path(alice, "LOVES", bob, Relationship(carol, "HATES", bob), carol, "KNOWS", dave)
        written = cypher_repr(path)
        assert written == "()-[:LOVES]->()<-[:HATES]-()-[:KNOWS]->()"


class CypherPresubstitutionTestCase(Py2neoTestCase):

    def new_tx(self):
        return TemporaryTransaction(self.graph)

    def test_can_use_parameter_for_property_value(self):
        tx = self.new_tx()
        if tx:
            labels, full_name = tx.run("CREATE (a:`Homo Sapiens` {`full name`:{v}}) "
                                       "RETURN labels(a), a.`full name`",
                                       v="Alice Smith").select()
            assert set(labels) == {"Homo Sapiens"}
            assert full_name == "Alice Smith"

    def test_can_use_parameter_for_property_set(self):
        tx = self.new_tx()
        if tx:
            labels, full_name = tx.run("CREATE (a:`Homo Sapiens`) SET a={p} "
                                       "RETURN labels(a), a.`full name`",
                                       p={"full name": "Alice Smith"}).select()
            assert set(labels) == {"Homo Sapiens"}
            assert full_name == "Alice Smith"

    def test_can_use_parameter_for_property_key(self):
        tx = self.new_tx()
        if tx:
            labels, full_name = tx.run("CREATE (a:`Homo Sapiens` {«k»:'Alice Smith'}) "
                                       "RETURN labels(a), a.`full name`",
                                       k="full name").select()
            assert set(labels) == {"Homo Sapiens"}
            assert full_name == "Alice Smith"

    def test_can_use_parameter_for_node_label(self):
        tx = self.new_tx()
        if tx:
            labels, full_name = tx.run("CREATE (a:«l» {`full name`:'Alice Smith'}) "
                                       "RETURN labels(a), a.`full name`",
                                       l="Homo Sapiens").select()
            assert set(labels) == {"Homo Sapiens"}
            assert full_name == "Alice Smith"

    def test_can_use_parameter_for_multiple_node_labels(self):
        tx = self.new_tx()
        if tx:
            labels, full_name = tx.run("CREATE (a:«l» {`full name`:'Alice Smith'}) "
                                       "RETURN labels(a), a.`full name`",
                                       l=("Homo Sapiens", "Hunter", "Gatherer")).select()
            assert set(labels) == {"Homo Sapiens", "Hunter", "Gatherer"}
            assert full_name == "Alice Smith"

    def test_can_use_parameter_mixture(self):
        statement = u"CREATE (a:«l» {«k»:{v}})"
        parameters = {"l": "Homo Sapiens", "k": "full name", "v": "Alice Smith"}
        s, p = presubstitute(statement, parameters)
        assert s == "CREATE (a:`Homo Sapiens` {`full name`:{v}})"
        assert p == {"v": "Alice Smith"}

    def test_can_use_multiple_parameters(self):
        statement = u"CREATE (a:«l» {«k»:{v}})-->(a:«l» {«k»:{v}})"
        parameters = {"l": "Homo Sapiens", "k": "full name", "v": "Alice Smith"}
        s, p = presubstitute(statement, parameters)
        assert s == "CREATE (a:`Homo Sapiens` {`full name`:{v}})-->" \
                    "(a:`Homo Sapiens` {`full name`:{v}})"
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
            with self.assertRaises(KeyError):
                tx.run("CREATE (a)-[ab:«t»]->(b) RETURN ab")

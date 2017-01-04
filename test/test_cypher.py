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


from io import StringIO

from py2neo.cypher import CypherWriter, cypher_repr
from py2neo.graph import Transaction, HTTPTransaction
from py2neo.status import CypherSyntaxError, ConstraintError
from py2neo.types import Node, Relationship, Path, order, size
from py2neo import remote
from test.util import GraphTestCase


class CypherTestCase(GraphTestCase):

    def setUp(self):
        a = Node(name="Alice", age=66)
        b = Node(name="Bob", age=77)
        ab = Relationship(a, "KNOWS", b)
        self.graph.create(ab)
        self.alice_and_bob = (a, b, ab)

    def test_can_get_list_of_records(self):
        records = list(self.graph.run("RETURN 1"))
        assert len(records) == 1
        first = records[0]
        assert len(first) == 1
        value = first[0]
        assert value == 1

    def test_can_navigate_through_records_using_cursor(self):
        records = self.graph.run("RETURN 1")
        for record in records:
            assert record.values() == (1,)

    def test_can_run_cypher_statement(self):
        self.graph.run("MERGE (a:Person {name:'Alice'})")

    def test_can_run_parametrised_cypher_statement(self):
        self.graph.run("MERGE (a:Person {name:{N}})", {"N": "Alice"})

    def test_can_run_cypher_statement_1(self):
        value = self.graph.evaluate("MERGE (a:Person {name:'Alice'}) RETURN a")
        assert isinstance(value, Node)
        assert set(value.labels()) == {"Person"}
        assert dict(value) == {"name": "Alice"}

    def test_can_run_parametrised_cypher_statement_1(self):
        value = self.graph.evaluate("MERGE (a:Person {name:{N}}) RETURN a", {"N": "Alice"})
        assert isinstance(value, Node)
        assert set(value.labels()) == {"Person"}
        assert dict(value) == {"name": "Alice"}

    def test_can_evaluate_cypher_statement(self):
        value = self.graph.evaluate("MERGE (a:Person {name:'Alice'}) RETURN a")
        assert isinstance(value, Node)
        assert set(value.labels()) == {"Person"}
        assert dict(value) == {"name": "Alice"}

    def test_can_evaluate_parametrised_cypher_statement(self):
        value = self.graph.evaluate("MERGE (a:Person {name:{N}}) RETURN a", {"N": "Alice"})
        assert isinstance(value, Node)
        assert set(value.labels()) == {"Person"}
        assert dict(value) == {"name": "Alice"}

    def test_evaluate_with_no_results_returns_none(self):
        value = self.graph.evaluate("CREATE (a {name:{N}})", {"N": "Alice"})
        assert value is None

    def test_can_begin_transaction(self):
        tx = self.graph.begin()
        assert isinstance(tx, Transaction)

    def test_nonsense_query(self):
        statement = "SELECT z=nude(0) RETURNS x"
        with self.assertRaises(CypherSyntaxError):
            self.graph.run(statement)

    def test_can_run_statement(self):
        records = list(self.graph.run("CREATE (a {name:'Alice'}) RETURN a.name AS name"))
        assert len(records) == 1
        assert records[0]["name"] == "Alice"

    def test_can_run_with_parameter(self):
        records = list(self.graph.run("CREATE (a {name:{x}}) RETURN a.name AS name", x="Alice"))
        assert len(records) == 1
        assert records[0]["name"] == "Alice"

    def test_can_evaluate(self):
        value = self.graph.evaluate("CREATE (a {name:'Alice'}) RETURN a.name AS name")
        assert value == "Alice"

    def test_can_evaluate_where_none_returned(self):
        statement = "MATCH (a) WHERE 2 + 2 = 5 RETURN a.name AS name"
        value = self.graph.evaluate(statement)
        assert value is None

    def test_query(self):
        a, b, ab = self.alice_and_bob
        statement = ("MATCH (a) WHERE id(a)={A} "
                     "MATCH (b) WHERE id(b)={B} "
                     "MATCH (a)-[ab:KNOWS]->(b) "
                     "RETURN a, b, ab, a.name AS a_name, b.name AS b_name")
        records = list(self.graph.run(statement, {"A": remote(a)._id, "B": remote(b)._id}))
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
        records = list(self.graph.run(statement, {"A": remote(a)._id, "B": remote(b)._id}))
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
        params = {"N": remote(node)._id}
        cursor = self.graph.run(statement, params)
        record = cursor.next()
        assert record["a_collection"] == [node]

    def test_param_used_once(self):
        node = Node()
        self.graph.create(node)
        statement = "MATCH (a) WHERE id(a)={X} RETURN a"
        params = {"X": remote(node)._id}
        cursor = self.graph.run(statement, params)
        record = cursor.next()
        assert record["a"] == node

    def test_param_used_twice(self):
        node = Node()
        self.graph.create(node)
        statement = "MATCH (a) WHERE id(a)={X} MATCH (b) WHERE id(b)={X} RETURN a, b"
        params = {"X": remote(node)._id}
        record = self.graph.run(statement, params).next()
        assert record["a"] == node
        assert record["b"] == node

    def test_param_used_thrice(self):
        node = Node()
        self.graph.create(node)
        statement = "MATCH (a) WHERE id(a)={X} " \
                    "MATCH (b) WHERE id(b)={X} " \
                    "MATCH (c) WHERE id(c)={X} " \
                    "RETURN a, b, c"
        params = {"X": remote(node)._id}
        cursor = self.graph.run(statement, params)
        record = cursor.next()
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
        params = {"A": remote(a)._id, "min_age": 50}
        record = self.graph.run(query, params).next()
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
        params = {"A": remote(a)._id, "min_age": 50}
        record = self.graph.run(query, params).next()
        assert record["c"] == c

    def test_unique_path_not_unique_raises_cypher_error(self):
        graph = self.graph
        record = graph.run("CREATE (a), (b) RETURN a, b").next()
        parameters = {"A": remote(record["a"])._id, "B": remote(record["b"])._id}
        statement = ("MATCH (a) WHERE id(a)={A} MATCH (b) WHERE id(b)={B}"
                     "CREATE (a)-[:KNOWS]->(b)")
        graph.run(statement, parameters)
        graph.run(statement, parameters)
        statement = ("MATCH (a) WHERE id(a)={A} MATCH (b) WHERE id(b)={B}"
                     "CREATE UNIQUE (a)-[:KNOWS]->(b)")
        with self.assertRaises(ConstraintError):
            graph.run(statement, parameters)


class CypherCreateTestCase(GraphTestCase):

    def test_can_create_node(self):
        a = Node("Person", name="Alice")
        self.graph.create(a)
        assert remote(a)

    def test_can_create_relationship(self):
        a = Node("Person", name="Alice")
        b = Node("Person", name="Bob")
        r = Relationship(a, "KNOWS", b, since=1999)
        self.graph.create(r)
        assert remote(a)
        assert remote(b)
        assert remote(r)
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
        self.graph.create(ab | bc | ca)
        assert remote(a)
        assert remote(b)
        assert remote(c)
        assert remote(ab)
        assert ab.start_node() == a
        assert ab.end_node() == b
        assert remote(bc)
        assert bc.start_node() == b
        assert bc.end_node() == c
        assert remote(ca)
        assert ca.start_node() == c
        assert ca.end_node() == a
        assert order(self.graph) == 3
        assert size(self.graph) == 3

    def test_cannot_create_non_graphy_thing(self):
        with self.assertRaises(TypeError):
            self.graph.create("this string is definitely not graphy")


class CypherWriterTestCase(GraphTestCase):

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
        node = Node()
        node.__name__ = "a"
        string = StringIO()
        writer = CypherWriter(string)
        writer.write(node)
        written = string.getvalue()
        assert written == "(a)"

    def test_can_write_node_with_labels(self):
        node = Node("Dark Brown", "Chicken")
        node.__name__ = "a"
        string = StringIO()
        writer = CypherWriter(string)
        writer.write(node)
        written = string.getvalue()
        assert written == '(a:Chicken:`Dark Brown`)'

    def test_can_write_node_with_properties(self):
        node = Node(name="Gertrude", age=3)
        string = StringIO()
        writer = CypherWriter(string)
        writer.write(node)
        written = string.getvalue()
        assert written == '(gertrude {age:3,name:"Gertrude"})'

    def test_can_write_node_with_labels_and_properties(self):
        node = Node("Dark Brown", "Chicken", name="Gertrude", age=3)
        string = StringIO()
        writer = CypherWriter(string)
        writer.write(node)
        written = string.getvalue()
        assert written == '(gertrude:Chicken:`Dark Brown` {age:3,name:"Gertrude"})'

    def test_can_write_simple_relationship(self):
        a = Node()
        b = Node()
        r = Relationship(a, "KNOWS", b)
        a.__name__ = "a"
        b.__name__ = "b"
        string = StringIO()
        writer = CypherWriter(string)
        writer.write(r)
        written = string.getvalue()
        assert written == "(a)-[:KNOWS]->(b)"

    def test_can_write_relationship_with_name(self):
        r = Relationship(Node(name="Fred"), "LIVES WITH", Node(name="Wilma"))
        string = StringIO()
        writer = CypherWriter(string)
        writer.write_relationship(r, name="fred_wilma")
        written = string.getvalue()
        assert written == '(fred)-[fred_wilma:`LIVES WITH`]->(wilma)'

    def test_can_write_relationship_with_properties(self):
        r = Relationship(Node(name="Fred"), ("LIVES WITH", {"place": "Bedrock"}),
                         Node(name="Wilma"))
        string = StringIO()
        writer = CypherWriter(string)
        writer.write(r)
        written = string.getvalue()
        assert written == '(fred)-[:`LIVES WITH` {place:"Bedrock"}]->(wilma)'

    def test_can_write_simple_path(self):
        alice, bob, carol, dave = Node(name="Alice"), Node(name="Bob"), \
                                  Node(name="Carol"), Node(name="Dave")
        path = Path(alice, "LOVES", bob, Relationship(carol, "HATES", bob), carol, "KNOWS", dave)
        string = StringIO()
        writer = CypherWriter(string)
        writer.write(path)
        written = string.getvalue()
        assert written == "(alice)-[:LOVES]->(bob)<-[:HATES]-(carol)-[:KNOWS]->(dave)"

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

    def test_maps_do_not_contain_private_entries(self):
        string = StringIO()
        writer = CypherWriter(string)
        writer.write({"visible": True, "_visible": False})
        written = string.getvalue()
        assert written == '{visible:true}'

    def test_maps_can_contain_private_entries_if_enabled(self):
        string = StringIO()
        writer = CypherWriter(string)
        writer.write_map({"visible": True, "_visible": True}, private=True)
        written = string.getvalue()
        assert written == '{_visible:true,visible:true}'

    def test_writing_none_writes_nothing(self):
        string = StringIO()
        writer = CypherWriter(string)
        writer.write(None)
        written = string.getvalue()
        assert written == ""

    def test_can_write_with_wrapper_function(self):
        alice, bob, carol, dave = Node(name="Alice"), Node(name="Bob"), \
                                  Node(name="Carol"), Node(name="Dave")
        path = Path(alice, "LOVES", bob, Relationship(carol, "HATES", bob), carol, "KNOWS", dave)
        written = cypher_repr(path)
        assert written == "(alice)-[:LOVES]->(bob)<-[:HATES]-(carol)-[:KNOWS]->(dave)"


class CypherOverHTTPTestCase(GraphTestCase):

    def test_can_run_statement(self):
        tx = HTTPTransaction(self.graph)
        result = tx.run("CREATE (a:Person {name:'Alice'}) RETURN a")
        tx.commit()
        assert list(result.keys()) == ["a"]
        records = list(result)
        assert len(records) == 1
        assert len(records[0]) == 1
        a = records[0][0]
        assert isinstance(a, Node)
        assert set(a.labels()) == {"Person"}
        assert dict(a) == {"name": "Alice"}

    def test_can_process_mid_transaction(self):
        tx = HTTPTransaction(self.graph)
        result1 = tx.run("RETURN 1")
        tx.process()
        assert result1.evaluate() == 1
        result2 = tx.run("RETURN 2")
        assert result2.evaluate() == 2
        tx.commit()

    def test_can_force_process_to_get_keys(self):
        tx = HTTPTransaction(self.graph)
        result = tx.run("RETURN 1 AS n")
        assert list(result.keys()) == ["n"]
        tx.commit()

    def test_rollback(self):
        tx = HTTPTransaction(self.graph)
        tx.run("RETURN 1")
        tx.rollback()
        assert tx.finished()

    def test_rollback_after_process(self):
        tx = HTTPTransaction(self.graph)
        tx.run("RETURN 1")
        tx.process()
        tx.run("RETURN 2")
        tx.rollback()
        assert tx.finished()

    def test_error(self):
        with self.assertRaises(CypherSyntaxError):
            tx = HTTPTransaction(self.graph)
            tx.run("X")
            tx.commit()

    def test_autocommit(self):
        tx = HTTPTransaction(self.graph, autocommit=True)
        cursor = tx.run("RETURN 1")
        assert cursor.evaluate() == 1

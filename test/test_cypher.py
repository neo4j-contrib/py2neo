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


from py2neo.database import Transaction, CypherSyntaxError, ConstraintError
from py2neo.types import Node, Relationship, Path, graph_order, graph_size

from test.util import GraphTestCase, HTTPGraphTestCase


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
        assert set(value.labels) == {"Person"}
        assert dict(value) == {"name": "Alice"}

    def test_can_run_parametrised_cypher_statement_1(self):
        value = self.graph.evaluate("MERGE (a:Person {name:{N}}) RETURN a", {"N": "Alice"})
        assert isinstance(value, Node)
        assert set(value.labels) == {"Person"}
        assert dict(value) == {"name": "Alice"}

    def test_can_evaluate_cypher_statement(self):
        value = self.graph.evaluate("MERGE (a:Person {name:'Alice'}) RETURN a")
        assert isinstance(value, Node)
        assert set(value.labels) == {"Person"}
        assert dict(value) == {"name": "Alice"}

    def test_can_evaluate_parametrised_cypher_statement(self):
        value = self.graph.evaluate("MERGE (a:Person {name:{N}}) RETURN a", {"N": "Alice"})
        assert isinstance(value, Node)
        assert set(value.labels) == {"Person"}
        assert dict(value) == {"name": "Alice"}

    def test_evaluate_with_no_results_returns_none(self):
        value = self.graph.evaluate("CREATE (a {name:{N}})", {"N": "Alice"})
        assert value is None

    def test_can_begin_transaction(self):
        tx = self.graph.begin()
        assert isinstance(tx, Transaction)

    def test_nonsense_query(self):
        statement = "X"
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
        records = list(self.graph.run(statement, {"A": a.identity, "B": b.identity}))
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
        result = self.graph.run(statement, {"A": a.identity, "B": b.identity})
        records = list(result)
        assert len(records) == 1
        for record in records:
            assert isinstance(record["p"], Path)
            nodes = record["p"].nodes
            assert len(nodes) == 2
            assert nodes[0] == a
            assert nodes[1] == b
            assert record["p"][0].type == "KNOWS"

    def test_query_can_return_collection(self):
        node = Node()
        self.graph.create(node)
        statement = "MATCH (a) WHERE id(a)={N} RETURN collect(a) AS a_collection"
        params = {"N": node.identity}
        cursor = self.graph.run(statement, params)
        record = cursor.next()
        assert record["a_collection"] == [node]

    def test_param_used_once(self):
        node = Node()
        self.graph.create(node)
        statement = "MATCH (a) WHERE id(a)={X} RETURN a"
        params = {"X": node.identity}
        cursor = self.graph.run(statement, params)
        record = cursor.next()
        assert record["a"] == node

    def test_param_used_twice(self):
        node = Node()
        self.graph.create(node)
        statement = "MATCH (a) WHERE id(a)={X} MATCH (b) WHERE id(b)={X} RETURN a, b"
        params = {"X": node.identity}
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
        params = {"X": node.identity}
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
        params = {"A": a.identity, "min_age": 50}
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
        params = {"A": a.identity, "min_age": 50}
        record = self.graph.run(query, params).next()
        assert record["c"] == c

    def test_unique_path_not_unique_raises_cypher_error(self):
        graph = self.graph
        record = graph.run("CREATE (a), (b) RETURN a, b").next()
        parameters = {"A": record["a"].identity, "B": record["b"].identity}
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
        self.assertEqual(a.graph, self.graph)
        self.assertIsNotNone(a.identity)

    def test_can_create_relationship(self):
        a = Node("Person", name="Alice")
        b = Node("Person", name="Bob")
        r = Relationship(a, "KNOWS", b, since=1999)
        self.graph.create(r)
        self.assertEqual(a.graph, self.graph)
        self.assertIsNotNone(a.identity)
        self.assertEqual(b.graph, self.graph)
        self.assertIsNotNone(b.identity)
        self.assertEqual(r.graph, self.graph)
        self.assertIsNotNone(r.identity)
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
        s = ab | bc | ca
        assert graph_order(s) == 3
        assert graph_size(s) == 3
        self.graph.create(s)
        self.assertEqual(a.graph, self.graph)
        self.assertIsNotNone(a.identity)
        self.assertEqual(b.graph, self.graph)
        self.assertIsNotNone(b.identity)
        self.assertEqual(c.graph, self.graph)
        self.assertIsNotNone(c.identity)
        self.assertEqual(ab.graph, self.graph)
        self.assertIsNotNone(ab.identity)
        assert ab.start_node() == a
        assert ab.end_node() == b
        self.assertEqual(bc.graph, self.graph)
        self.assertIsNotNone(bc.identity)
        assert bc.start_node() == b
        assert bc.end_node() == c
        self.assertEqual(ca.graph, self.graph)
        self.assertIsNotNone(ca.identity)
        assert ca.start_node() == c
        assert ca.end_node() == a
        assert graph_order(self.graph) == 3
        assert graph_size(self.graph) == 3

    def test_cannot_create_non_graphy_thing(self):
        with self.assertRaises(TypeError):
            self.graph.create("this string is definitely not graphy")


class CypherOverHTTPTestCase(HTTPGraphTestCase):

    def test_can_run_statement(self):
        tx = Transaction(self.http_graph)
        result = tx.run("CREATE (a:Person {name:'Alice'}) RETURN a")
        tx.commit()
        assert list(result.keys()) == ["a"]
        records = list(result)
        assert len(records) == 1
        assert len(records[0]) == 1
        a = records[0][0]
        assert isinstance(a, Node)
        assert set(a.labels) == {"Person"}
        assert dict(a) == {"name": "Alice"}

    def test_can_process_mid_transaction(self):
        tx = Transaction(self.http_graph)
        result1 = tx.run("RETURN 1")
        tx.process()
        assert result1.evaluate() == 1
        result2 = tx.run("RETURN 2")
        assert result2.evaluate() == 2
        tx.commit()

    def test_can_force_process_to_get_keys(self):
        tx = Transaction(self.http_graph)
        result = tx.run("RETURN 1 AS n")
        assert list(result.keys()) == ["n"]
        tx.commit()

    def test_rollback(self):
        tx = Transaction(self.http_graph)
        tx.run("RETURN 1")
        tx.rollback()
        assert tx.finished()

    def test_rollback_after_process(self):
        tx = Transaction(self.http_graph)
        tx.run("RETURN 1")
        tx.process()
        tx.run("RETURN 2")
        tx.rollback()
        assert tx.finished()

    def test_error(self):
        with self.assertRaises(CypherSyntaxError):
            tx = Transaction(self.http_graph)
            tx.run("X")
            tx.commit()

    def test_autocommit(self):
        tx = Transaction(self.http_graph, autocommit=True)
        cursor = tx.run("RETURN 1")
        assert cursor.evaluate() == 1

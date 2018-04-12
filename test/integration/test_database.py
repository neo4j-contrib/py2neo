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


from __future__ import absolute_import

from unittest import TestCase

from neo4j.v1 import Record
from neo4j.exceptions import ConstraintError, CypherSyntaxError

from py2neo.data import Node, Relationship, Path, order, size
from py2neo.database import Database, Graph, GraphError, TransactionFinished
from py2neo.internal.json import JSONHydrator
from py2neo.testing import IntegrationTestCase


alice = Node("Person", "Employee", name="Alice", age=33)
bob = Node("Person")
carol = Node("Person")
dave = Node("Person")

alice_knows_bob = Relationship(alice, "KNOWS", bob, since=1999)
alice_likes_carol = Relationship(alice, "LIKES", carol)
carol_dislikes_bob = Relationship(carol, "DISLIKES", bob)
carol_married_to_dave = Relationship(carol, "MARRIED_TO", dave)
dave_works_for_dave = Relationship(dave, "WORKS_FOR", dave)

record_keys = ["employee_id", "Person"]
record_a = Record(zip(record_keys, [1001, alice]))
record_b = Record(zip(record_keys, [1002, bob]))
record_c = Record(zip(record_keys, [1003, carol]))
record_d = Record(zip(record_keys, [1004, dave]))


class DatabaseSquareOneTestCase(TestCase):

    def test_can_generate_graph(self):
        Database.forget_all()
        db = Database()
        graph = db["data"]
        self.assertIsInstance(graph, Graph)


class DatabaseTestCase(IntegrationTestCase):

    def test_can_forget_all(self):
        _ = Database()
        self.assertTrue(Database._instances)
        Database.forget_all()
        self.assertFalse(Database._instances)

    def test_repr(self):
        db = Database()
        self.assertTrue(repr(db).startswith("<Database uri="))

    def test_same_uri_gives_same_instance(self):
        uri = "bolt://localhost:7687/"
        dbms_1 = Database(uri)
        dbms_2 = Database(uri)
        assert dbms_1 is dbms_2

    def test_dbms_equality(self):
        uri = "bolt://localhost:7687/"
        dbms_1 = Database(uri)
        dbms_2 = Database(uri)
        assert dbms_1 == dbms_2
        assert hash(dbms_1) == hash(dbms_2)

    def test_dbms_is_not_equal_to_non_dbms(self):
        uri = "bolt://localhost:7687/"
        db = Database(uri)
        assert db != object()

    def test_dbms_metadata(self):
        assert self.db.kernel_start_time
        assert self.db.kernel_version
        assert self.db.store_creation_time
        assert self.db.store_id
        assert self.db.primitive_counts
        assert self.db.store_file_sizes
        assert self.db.config

    def test_database_name(self):
        _ = self.db.database_name

    def test_store_directory(self):
        _ = self.db.store_directory

    def test_kernel_version(self):
        version = self.db.kernel_version
        assert isinstance(version, tuple)
        assert 3 <= len(version) <= 4
        assert isinstance(version[0], int)
        assert isinstance(version[1], int)
        assert isinstance(version[2], int)

    def test_can_get_list_of_databases(self):
        databases = list(self.db)
        assert databases == ["data"]

    def test_can_get_dictionary_of_databases(self):
        databases = dict(self.db)
        assert len(databases) == 1
        assert databases["data"] is self.graph


class GraphObjectTestCase(IntegrationTestCase):

    def test_same_uri_gives_same_instance(self):
        uri = "bolt://localhost:7687"
        graph_1 = Graph(uri)
        graph_2 = Graph(uri)
        assert graph_1 is graph_2

    def test_graph_len_returns_number_of_rels(self):
        size = len(self.graph)
        statement = "MATCH ()-[r]->() RETURN COUNT(r)"
        num_rels = self.graph.evaluate(statement)
        assert size == num_rels

    def test_graph_bool_returns_true(self):
        assert self.graph.__bool__()
        assert self.graph.__nonzero__()

    def test_graph_contains(self):
        node = Node()
        self.graph.create(node)
        self.assertIs(node.graph, self.graph)

    def test_can_hydrate_map_from_json_result(self):
        # TODO: check that a warning is raised
        data = {"foo": "bar"}
        value_system = JSONHydrator(self.graph, ["a"])
        hydrated = value_system.hydrate([data])
        assert hydrated[0] == data

    def test_graph_is_not_equal_to_non_graph(self):
        graph = Graph()
        assert graph != object()

    def test_can_create_and_delete_node(self):
        a = Node()
        self.graph.create(a)
        assert isinstance(a, Node)
        self.assertEqual(a.graph, self.graph)
        self.assertIsNotNone(a.identity)
        assert self.graph.exists(a)
        self.graph.delete(a)
        assert not self.graph.exists(a)

    def test_can_create_and_delete_relationship(self):
        ab = Relationship(Node(), "KNOWS", Node())
        self.graph.create(ab)
        assert isinstance(ab, Relationship)
        self.assertEqual(ab.graph, self.graph)
        self.assertIsNotNone(ab.identity)
        assert self.graph.exists(ab)
        self.graph.delete(ab | ab.start_node | ab.end_node)
        assert not self.graph.exists(ab)

    def test_can_get_node_by_id_when_cached(self):
        node = Node()
        self.graph.create(node)
        assert node.identity in self.graph.node_cache
        got = self.graph.node(node.identity)
        assert got is node

    def test_can_get_node_by_id_when_not_cached(self):
        node = Node()
        self.graph.create(node)
        self.graph.node_cache.clear()
        assert node.identity not in self.graph.node_cache
        got = self.graph.node(node.identity)
        assert got.identity == node.identity

    def test_get_non_existent_node_by_id(self):
        node = Node()
        self.graph.create(node)
        node_id = node.identity
        self.graph.delete(node)
        self.graph.node_cache.clear()
        with self.assertRaises(IndexError):
            _ = self.graph.node(node_id)

    def test_node_cache_is_thread_local(self):
        from threading import Thread
        node = Node()
        self.graph.create(node)
        assert node.identity in self.graph.node_cache
        other_cache_keys = []

        def check_cache():
            other_cache_keys.extend(self.graph.node_cache.keys())

        thread = Thread(target=check_cache)
        thread.start()
        thread.join()

        assert node.identity in self.graph.node_cache
        assert node.identity not in other_cache_keys

    def test_graph_repr(self):
        assert repr(self.graph).startswith("<Graph")

    def test_can_get_same_instance(self):
        graph_1 = Graph()
        graph_2 = Graph()
        assert graph_1 is graph_2

    def test_create_single_empty_node(self):
        a = Node()
        self.graph.create(a)
        self.assertEqual(a.graph, self.graph)
        self.assertIsNotNone(a.identity)

    def test_get_node_by_id(self):
        a1 = Node(foo="bar")
        self.graph.create(a1)
        a2 = self.graph.node(a1.identity)
        assert a1 == a2

    def test_create_node_with_mixed_property_types(self):
        a = Node.cast({"number": 13, "foo": "bar", "true": False, "fish": "chips"})
        self.graph.create(a)
        assert len(a) == 4
        assert a["fish"] == "chips"
        assert a["foo"] == "bar"
        assert a["number"] == 13
        assert not a["true"]

    def test_create_node_with_null_properties(self):
        a = Node.cast({"foo": "bar", "no-foo": None})
        self.graph.create(a)
        assert a["foo"] == "bar"
        assert a["no-foo"] is None


class GraphSchemaTestCase(IntegrationTestCase):

    def setUp(self):
        self.reset()

    def test_schema_index(self):
        label_1 = next(self.unique_string)
        label_2 = next(self.unique_string)
        munich = Node.cast({'name': "München", 'key': "09162000"})
        self.graph.create(munich)
        munich.clear_labels()
        munich.update_labels({label_1, label_2})
        self.schema.create_index(label_1, "name")
        self.schema.create_index(label_1, "key")
        self.schema.create_index(label_2, "name")
        self.schema.create_index(label_2, "key")
        found_borough_via_name = self.node_selector.select(label_1, name="München")
        found_borough_via_key = self.node_selector.select(label_1, key="09162000")
        found_county_via_name = self.node_selector.select(label_2, name="München")
        found_county_via_key = self.node_selector.select(label_2, key="09162000")
        assert list(found_borough_via_name) == list(found_borough_via_key)
        assert list(found_county_via_name) == list(found_county_via_key)
        assert list(found_borough_via_name) == list(found_county_via_name)
        keys = self.schema.get_indexes(label_1)
        assert (u"name",) in keys
        assert (u"key",) in keys
        self.schema.drop_index(label_1, "name")
        self.schema.drop_index(label_1, "key")
        self.schema.drop_index(label_2, "name")
        self.schema.drop_index(label_2, "key")
        with self.assertRaises(GraphError):
            self.schema.drop_index(label_2, "key")
        self.graph.delete(munich)

    def test_unique_constraint(self):
        label_1 = next(self.unique_string)
        borough = Node(label_1, name="Taufkirchen")
        self.graph.create(borough)
        self.schema.create_uniqueness_constraint(label_1, "name")
        constraints = self.schema.get_uniqueness_constraints(label_1)
        assert (u"name",) in constraints
        with self.assertRaises(ConstraintError):
            self.graph.create(Node(label_1, name="Taufkirchen"))
        self.graph.delete(borough)

    def test_labels_constraints(self):
        label_1 = next(self.unique_string)
        a = Node(label_1, name="Alice")
        b = Node(label_1, name="Alice")
        self.graph.create(a | b)
        with self.assertRaises(GraphError):
            self.graph.schema.create_uniqueness_constraint(label_1, "name")
        b.remove_label(label_1)
        self.graph.push(b)
        self.schema.create_uniqueness_constraint(label_1, "name")
        a.remove_label(label_1)
        self.graph.push(a)
        b.add_label(label_1)
        self.graph.push(b)
        b.remove_label(label_1)
        self.graph.push(b)
        self.schema.drop_uniqueness_constraint(label_1, "name")
        with self.assertRaises(GraphError):
            self.schema.drop_uniqueness_constraint(label_1, "name")
        self.graph.delete(a | b)


class GraphMatchTestCase(IntegrationTestCase):

    def setUp(self):
        self.alice = Node(name="Alice")
        self.bob = Node(name="Bob")
        self.carol = Node(name="Carol")
        s = (Relationship(self.alice, "LOVES", self.bob) |
             Relationship(self.bob, "LOVES", self.alice) |
             Relationship(self.alice, "KNOWS", self.bob) |
             Relationship(self.bob, "KNOWS", self.alice) |
             Relationship(self.bob, "KNOWS", self.carol) |
             Relationship(self.carol, "KNOWS", self.bob))
        self.graph.create(s)

    def test_can_match_start_node(self):
        relationships = list(self.graph.match(start_node=self.alice))
        assert len(relationships) == 2
        assert "KNOWS" in [type(rel).__name__ for rel in relationships]
        assert "LOVES" in [type(rel).__name__ for rel in relationships]
        assert self.bob in [rel.end_node for rel in relationships]

    def test_can_match_start_node_and_type(self):
        relationships = list(self.graph.match(start_node=self.alice, rel_type="KNOWS"))
        assert len(relationships) == 1
        assert self.bob in [rel.end_node for rel in relationships]

    def test_can_match_start_node_and_end_node(self):
        relationships = list(self.graph.match(start_node=self.alice, end_node=self.bob))
        assert len(relationships) == 2
        assert "KNOWS" in [type(rel).__name__ for rel in relationships]
        assert "LOVES" in [type(rel).__name__ for rel in relationships]

    def test_can_match_type_and_end_node(self):
        relationships = list(self.graph.match(rel_type="KNOWS", end_node=self.bob))
        assert len(relationships) == 2
        assert self.alice in [rel.start_node for rel in relationships]
        assert self.carol in [rel.start_node for rel in relationships]

    def test_can_bidi_match_start_node(self):
        relationships = list(self.graph.match(start_node=self.bob, bidirectional=True))
        assert len(relationships) == 6
        assert "KNOWS" in [type(rel).__name__ for rel in relationships]
        assert "LOVES" in [type(rel).__name__ for rel in relationships]
        assert self.alice in [rel.start_node for rel in relationships]
        assert self.bob in [rel.start_node for rel in relationships]
        assert self.carol in [rel.start_node for rel in relationships]
        assert self.alice in [rel.end_node for rel in relationships]
        assert self.bob in [rel.end_node for rel in relationships]
        assert self.carol in [rel.end_node for rel in relationships]

    def test_can_bidi_match_start_node_and_type(self):
        relationships = list(self.graph.match(start_node=self.bob, rel_type="KNOWS", bidirectional=True))
        assert len(relationships) == 4
        assert self.alice in [rel.start_node for rel in relationships]
        assert self.bob in [rel.start_node for rel in relationships]
        assert self.carol in [rel.start_node for rel in relationships]
        assert self.alice in [rel.end_node for rel in relationships]
        assert self.bob in [rel.end_node for rel in relationships]
        assert self.carol in [rel.end_node for rel in relationships]

    def test_can_bidi_match_start_node_and_end_node(self):
        relationships = list(self.graph.match(start_node=self.alice, end_node=self.bob, bidirectional=True))
        assert len(relationships) == 4
        assert "KNOWS" in [type(rel).__name__ for rel in relationships]
        assert "LOVES" in [type(rel).__name__ for rel in relationships]
        assert self.alice in [rel.start_node for rel in relationships]
        assert self.bob in [rel.start_node for rel in relationships]
        assert self.alice in [rel.end_node for rel in relationships]
        assert self.bob in [rel.end_node for rel in relationships]

    def test_can_bidi_match_type_and_end_node(self):
        relationships = list(self.graph.match(rel_type="KNOWS", end_node=self.bob, bidirectional=True))
        assert len(relationships) == 4
        assert self.alice in [rel.start_node for rel in relationships]
        assert self.carol in [rel.start_node for rel in relationships]
        assert self.alice in [rel.start_node for rel in relationships]
        assert self.bob in [rel.start_node for rel in relationships]
        assert self.carol in [rel.start_node for rel in relationships]
        assert self.alice in [rel.end_node for rel in relationships]
        assert self.bob in [rel.end_node for rel in relationships]
        assert self.carol in [rel.end_node for rel in relationships]

    def test_can_match_with_limit(self):
        relationships = list(self.graph.match(limit=3))
        assert len(relationships) == 3

    def test_can_match_one_when_some_exist(self):
        rel = self.graph.match_one()
        assert isinstance(rel, Relationship)

    def test_can_match_one_when_none_exist(self):
        rel = self.graph.match_one(rel_type=next(self.unique_string))
        assert rel is None

    def test_can_match_none(self):
        relationships = list(self.graph.match(rel_type="X", limit=1))
        assert len(relationships) == 0

    def test_can_match_start_node_and_multiple_types(self):
        relationships = list(self.graph.match(start_node=self.alice, rel_type=("LOVES", "KNOWS")))
        assert len(relationships) == 2

    def test_relationship_start_node_must_be_bound(self):
        with self.assertRaises(ValueError):
            list(self.graph.match(start_node=Node()))

    def test_relationship_end_node_must_be_bound(self):
        with self.assertRaises(ValueError):
            list(self.graph.match(end_node=Node()))

    def test_relationship_start_and_end_node_must_be_bound(self):
        with self.assertRaises(ValueError):
            list(self.graph.match(start_node=Node(), end_node=Node()))


class GraphDeleteTestCase(IntegrationTestCase):

    def test_can_delete_node(self):
        alice = Node("Person", name="Alice")
        self.graph.create(alice)
        assert self.graph.exists(alice)
        self.graph.delete(alice)
        assert not self.graph.exists(alice)

    def test_can_delete_nodes_and_relationship_rel_first(self):
        alice = Node("Person", name="Alice")
        bob = Node("Person", name="Bob")
        ab = Relationship(alice, "KNOWS", bob)
        self.graph.create(alice | bob | ab)
        assert self.graph.exists(alice | bob | ab)
        self.graph.delete(ab | alice | bob)
        assert not self.graph.exists(alice | bob | ab)

    def test_can_delete_nodes_and_relationship_nodes_first(self):
        alice = Node("Person", name="Alice")
        bob = Node("Person", name="Bob")
        ab = Relationship(alice, "KNOWS", bob)
        self.graph.create(alice | bob | ab)
        assert self.graph.exists(alice | bob | ab)
        self.graph.delete(alice | bob | ab)
        assert not self.graph.exists(alice | bob | ab)

    def test_can_delete_path(self):
        alice, bob, carol, dave = Node(), Node(), Node(), Node()
        path = Path(alice, "LOVES", bob, Relationship(carol, "HATES", bob), carol, "KNOWS", dave)
        self.graph.create(path)
        assert self.graph.exists(path)
        self.graph.delete(path)
        assert not self.graph.exists(path)

    def test_cannot_delete_other_types(self):
        with self.assertRaises(TypeError):
            self.graph.delete("not a node or a relationship")


class TransactionRunTestCase(IntegrationTestCase):

    def test_can_run_single_statement_transaction(self):
        tx = self.graph.begin()
        assert not tx.finished()
        cursor = tx.run("CREATE (a) RETURN a")
        tx.commit()
        records = list(cursor)
        assert len(records) == 1
        for record in records:
            assert isinstance(record["a"], Node)
        assert tx.finished()

    def test_can_run_query_that_returns_map_literal(self):
        tx = self.graph.begin()
        cursor = tx.run("RETURN {foo:'bar'}")
        tx.commit()
        value = cursor.evaluate()
        assert value == {"foo": "bar"}

    def test_can_run_transaction_as_with_statement(self):
        with self.graph.begin() as tx:
            assert not tx.finished()
            tx.run("CREATE (a) RETURN a")
        assert tx.finished()

    def test_can_run_multi_statement_transaction(self):
        tx = self.graph.begin()
        assert not tx.finished()
        cursor_1 = tx.run("CREATE (a) RETURN a")
        cursor_2 = tx.run("CREATE (a) RETURN a")
        cursor_3 = tx.run("CREATE (a) RETURN a")
        tx.commit()
        for cursor in (cursor_1, cursor_2, cursor_3):
            records = list(cursor)
            assert len(records) == 1
            for record in records:
                assert isinstance(record["a"], Node)
        assert tx.finished()

    def test_can_run_multi_execute_transaction(self):
        tx = self.graph.begin()
        for i in range(10):
            assert not tx.finished()
            cursor_1 = tx.run("CREATE (a) RETURN a")
            cursor_2 = tx.run("CREATE (a) RETURN a")
            cursor_3 = tx.run("CREATE (a) RETURN a")
            tx.process()
            for cursor in (cursor_1, cursor_2, cursor_3):
                records = list(cursor)
                assert len(records) == 1
                for record in records:
                    assert isinstance(record["a"], Node)
        tx.commit()
        assert tx.finished()

    def test_can_rollback_transaction(self):
        tx = self.graph.begin()
        for i in range(10):
            assert not tx.finished()
            cursor_1 = tx.run("CREATE (a) RETURN a")
            cursor_2 = tx.run("CREATE (a) RETURN a")
            cursor_3 = tx.run("CREATE (a) RETURN a")
            tx.process()
            for cursor in (cursor_1, cursor_2, cursor_3):
                records = list(cursor)
                assert len(records) == 1
                for record in records:
                    assert isinstance(record["a"], Node)
        tx.rollback()
        assert tx.finished()

    def test_cannot_append_after_transaction_finished(self):
        tx = self.graph.begin()
        tx.rollback()
        try:
            tx.run("CREATE (a) RETURN a")
        except TransactionFinished as error:
            assert error.args[0] is tx
        else:
            assert False


class TransactionCreateTestCase(IntegrationTestCase):

    def test_can_create_node(self):
        a = Node("Person", name="Alice")
        with self.graph.begin() as tx:
            tx.create(a)
        self.assertEqual(a.graph, self.graph)
        self.assertIsNotNone(a.identity)

    def test_can_create_relationship(self):
        a = Node("Person", name="Alice")
        b = Node("Person", name="Bob")
        r = Relationship(a, "KNOWS", b, since=1999)
        with self.graph.begin() as tx:
            tx.create(r)
        self.assertEqual(a.graph, self.graph)
        self.assertIsNotNone(a.identity)
        self.assertEqual(b.graph, self.graph)
        self.assertIsNotNone(b.identity)
        self.assertEqual(r.graph, self.graph)
        self.assertIsNotNone(r.identity)
        assert r.start_node == a
        assert r.end_node == b

    def test_can_create_nodes_and_relationship_1(self):
        self.graph.delete_all()
        with self.graph.begin() as tx:
            a = Node("Person", name="Alice")
            b = Node("Person", name="Bob")
            tx.create(a)
            tx.create(b)
            tx.process()
            r = Relationship(a, "KNOWS", b, since=1999)
            tx.create(r)
        self.assertEqual(a.graph, self.graph)
        self.assertIsNotNone(a.identity)
        self.assertEqual(b.graph, self.graph)
        self.assertIsNotNone(b.identity)
        self.assertEqual(r.graph, self.graph)
        self.assertIsNotNone(r.identity)
        assert r.start_node == a
        assert r.end_node == b
        assert order(self.graph) == 2
        assert size(self.graph) == 1

    def test_can_create_nodes_and_relationship_2(self):
        self.graph.delete_all()
        with self.graph.begin() as tx:
            a = Node("Person", name="Alice")
            b = Node("Person", name="Bob")
            tx.create(a)
            tx.create(b)
            r = Relationship(a, "KNOWS", b, since=1999)
            tx.create(r)
        self.assertEqual(a.graph, self.graph)
        self.assertIsNotNone(a.identity)
        self.assertEqual(b.graph, self.graph)
        self.assertIsNotNone(b.identity)
        self.assertEqual(r.graph, self.graph)
        self.assertIsNotNone(r.identity)
        assert r.start_node == a
        assert r.end_node == b
        assert order(self.graph) == 2
        assert size(self.graph) == 1

    def test_can_create_nodes_and_relationship_3(self):
        self.graph.delete_all()
        with self.graph.begin() as tx:
            a = Node("Person", name="Alice")
            b = Node("Person", name="Bob")
            r = Relationship(a, "KNOWS", b, since=1999)
            tx.create(a)
            tx.create(b)
            tx.create(r)
        self.assertEqual(a.graph, self.graph)
        self.assertIsNotNone(a.identity)
        self.assertEqual(b.graph, self.graph)
        self.assertIsNotNone(b.identity)
        self.assertEqual(r.graph, self.graph)
        self.assertIsNotNone(r.identity)
        assert r.start_node == a
        assert r.end_node == b
        assert order(self.graph) == 2
        assert size(self.graph) == 1

    def test_can_create_nodes_and_relationship_4(self):
        self.graph.delete_all()
        with self.graph.begin() as tx:
            a = Node()
            b = Node()
            c = Node()
            ab = Relationship(a, "TO", b)
            bc = Relationship(b, "TO", c)
            ca = Relationship(c, "TO", a)
            tx.create(ab | bc | ca)
        self.assertEqual(a.graph, self.graph)
        self.assertIsNotNone(a.identity)
        self.assertEqual(b.graph, self.graph)
        self.assertIsNotNone(b.identity)
        self.assertEqual(c.graph, self.graph)
        self.assertIsNotNone(c.identity)
        self.assertEqual(ab.graph, self.graph)
        self.assertIsNotNone(ab.identity)
        assert ab.start_node == a
        assert ab.end_node == b
        self.assertEqual(bc.graph, self.graph)
        self.assertIsNotNone(bc.identity)
        assert bc.start_node == b
        assert bc.end_node == c
        self.assertEqual(ca.graph, self.graph)
        self.assertIsNotNone(ca.identity)
        assert ca.start_node == c
        assert ca.end_node == a
        assert order(self.graph) == 3
        assert size(self.graph) == 3

    def test_create_is_idempotent(self):
        self.graph.delete_all()
        a = Node()
        b = Node()
        r = Relationship(a, "TO", b)
        with self.graph.begin() as tx:
            tx.create(r)
        self.assertEqual(a.graph, self.graph)
        self.assertIsNotNone(a.identity)
        self.assertEqual(b.graph, self.graph)
        self.assertIsNotNone(b.identity)
        self.assertEqual(r.graph, self.graph)
        self.assertIsNotNone(r.identity)
        assert order(self.graph) == 2
        assert size(self.graph) == 1
        with self.graph.begin() as tx:
            tx.create(r)
        self.assertEqual(a.graph, self.graph)
        self.assertIsNotNone(a.identity)
        self.assertEqual(b.graph, self.graph)
        self.assertIsNotNone(b.identity)
        self.assertEqual(r.graph, self.graph)
        self.assertIsNotNone(r.identity)
        assert order(self.graph) == 2
        assert size(self.graph) == 1

    def test_cannot_create_non_graphy_thing(self):
        with self.assertRaises(TypeError):
            self.graph.create("this string is definitely not graphy")


class TransactionDeleteTestCase(IntegrationTestCase):

    def test_can_delete_relationship(self):
        a = Node()
        b = Node()
        r = Relationship(a, "TO", b)
        self.graph.create(r)
        assert self.graph.exists(r)
        with self.graph.begin() as tx:
            tx.delete(r)
        assert not self.graph.exists(r)
        assert not self.graph.exists(a)
        assert not self.graph.exists(b)


class TransactionSeparateTestCase(IntegrationTestCase):

    def test_can_delete_relationship_by_separating(self):
        a = Node()
        b = Node()
        r = Relationship(a, "TO", b)
        self.graph.create(r)
        assert self.graph.exists(r)
        with self.graph.begin() as tx:
            tx.separate(r)
        assert not self.graph.exists(r)
        assert self.graph.exists(a)
        assert self.graph.exists(b)

    def test_cannot_separate_non_graphy_thing(self):
        with self.assertRaises(TypeError):
            self.graph.separate("this string is definitely not graphy")


class TransactionDegreeTestCase(IntegrationTestCase):

    def test_degree_of_node(self):
        a = Node()
        b = Node()
        self.graph.create(Relationship(a, "R1", b) | Relationship(a, "R2", b))
        with self.graph.begin() as tx:
            d = tx.degree(a)
        assert d == 2

    def test_degree_of_two_related_nodes(self):
        a = Node()
        b = Node()
        self.graph.create(Relationship(a, "R1", b) | Relationship(a, "R2", b))
        with self.graph.begin() as tx:
            d = tx.degree(a | b)
        assert d == 2

    def test_cannot_get_degree_of_non_graphy_thing(self):
        with self.assertRaises(TypeError):
            with self.graph.begin() as tx:
                tx.degree("this string is definitely not graphy")


class TransactionExistsTestCase(IntegrationTestCase):

    def test_cannot_check_existence_of_non_graphy_thing(self):
        with self.assertRaises(TypeError):
            with self.graph.begin() as tx:
                tx.exists("this string is definitely not graphy")


class TransactionErrorTestCase(IntegrationTestCase):

    def test_can_generate_transaction_error(self):
        tx = self.graph.begin()
        with self.assertRaises(CypherSyntaxError):
            tx.run("X")
            tx.commit()

    def test_unique_path_not_unique_raises_cypher_transaction_error_in_transaction(self):
        tx = self.graph.begin()
        cursor = tx.run("CREATE (a), (b) RETURN a, b")
        tx.process()
        record = cursor.next()
        parameters = {"A": record["a"].identity, "B": record["b"].identity}
        statement = ("MATCH (a) WHERE id(a)={A} MATCH (b) WHERE id(b)={B}" +
                     "CREATE (a)-[:KNOWS]->(b)")
        tx.run(statement, parameters)
        tx.run(statement, parameters)
        statement = ("MATCH (a) WHERE id(a)={A} MATCH (b) WHERE id(b)={B}" +
                     "CREATE UNIQUE (a)-[:KNOWS]->(b)")
        with self.assertRaises(ConstraintError):
            tx.run(statement, parameters)
            tx.commit()


class TransactionAutocommitTestCase(IntegrationTestCase):

    def test_can_autocommit(self):
        tx = self.graph.begin(autocommit=True)
        assert not tx.finished()
        tx.run("RETURN 1")
        assert tx.finished()


class CursorMovementTestCase(IntegrationTestCase):
    """ Tests for move and position
    """

    def test_cannot_move_beyond_end(self):
        cursor = self.graph.run("RETURN 1")
        assert cursor.forward()
        assert not cursor.forward()

    def test_can_only_move_until_end(self):
        cursor = self.graph.run("RETURN 1")
        assert cursor.forward(2) == 1

    def test_moving_by_zero_keeps_same_position(self):
        cursor = self.graph.run("RETURN 1")
        assert cursor.forward(0) == 0


class CursorKeysTestCase(IntegrationTestCase):

    def test_keys_are_populated_before_moving(self):
        cursor = self.graph.run("RETURN 1 AS n")
        assert list(cursor.keys()) == ["n"]

    def test_keys_are_populated_after_moving(self):
        cursor = self.graph.run("UNWIND range(1, 10) AS n RETURN n")
        n = 0
        while cursor.forward():
            n += 1
            assert list(cursor.keys()) == ["n"]

    def test_keys_are_populated_before_moving_within_a_transaction(self):
        with self.graph.begin() as tx:
            cursor = tx.run("RETURN 1 AS n")
            assert list(cursor.keys()) == ["n"]


class CursorStatsTestCase(IntegrationTestCase):

    def test_stats_available(self):
        cursor = self.graph.run("CREATE (a:Banana)")
        stats = cursor.stats()
        assert stats["nodes_created"] == 1
        assert stats["labels_added"] == 1
        assert stats["contains_updates"] == 1


class CursorCurrentTestCase(IntegrationTestCase):

    def test_current_is_none_at_start(self):
        cursor = self.graph.run("RETURN 1")
        assert cursor.current() is None

    def test_current_updates_after_move(self):
        cursor = self.graph.run("UNWIND range(1, 10) AS n RETURN n")
        n = 0
        while cursor.forward():
            n += 1
            assert cursor.current() == Record(zip(["n"], [n]))


class CursorSelectionTestCase(IntegrationTestCase):

    def test_select_picks_next(self):
        cursor = self.graph.run("RETURN 1")
        record = cursor.next()
        assert record == Record(zip(["1"], [1]))

    def test_cannot_select_past_end(self):
        cursor = self.graph.run("RETURN 1")
        cursor.forward()
        with self.assertRaises(StopIteration):
            _ = cursor.next()

    def test_selection_triggers_move(self):
        cursor = self.graph.run("UNWIND range(1, 10) AS n RETURN n, n * n as n_sq")
        for i in range(1, 11):
            n, n_sq = cursor.next()
            assert n == i
            assert n_sq == i * i


class CursorAsIteratorTestCase(IntegrationTestCase):

    def test_can_use_next_function(self):
        cursor = self.graph.run("RETURN 1")
        record = next(cursor)
        assert record == Record(zip(["1"], [1]))

    def test_raises_stop_iteration(self):
        cursor = self.graph.run("RETURN 1")
        cursor.forward()
        with self.assertRaises(StopIteration):
            _ = next(cursor)


class CursorStreamingTestCase(IntegrationTestCase):

    def test_stream_yields_all(self):
        cursor = self.graph.run("UNWIND range(1, 10) AS n RETURN n, n * n as n_sq")
        record_list = list(cursor)
        assert record_list == [Record(zip(["n", "n_sq"], [1, 1])),
                               Record(zip(["n", "n_sq"], [2, 4])),
                               Record(zip(["n", "n_sq"], [3, 9])),
                               Record(zip(["n", "n_sq"], [4, 16])),
                               Record(zip(["n", "n_sq"], [5, 25])),
                               Record(zip(["n", "n_sq"], [6, 36])),
                               Record(zip(["n", "n_sq"], [7, 49])),
                               Record(zip(["n", "n_sq"], [8, 64])),
                               Record(zip(["n", "n_sq"], [9, 81])),
                               Record(zip(["n", "n_sq"], [10, 100]))]

    def test_stream_yields_remainder(self):
        cursor = self.graph.run("UNWIND range(1, 10) AS n RETURN n, n * n as n_sq")
        cursor.forward(5)
        record_list = list(cursor)
        assert record_list == [Record(zip(["n", "n_sq"], [6, 36])),
                               Record(zip(["n", "n_sq"], [7, 49])),
                               Record(zip(["n", "n_sq"], [8, 64])),
                               Record(zip(["n", "n_sq"], [9, 81])),
                               Record(zip(["n", "n_sq"], [10, 100]))]


class CursorEvaluationTestCase(IntegrationTestCase):

    def test_can_evaluate_single_value(self):
        cursor = self.graph.run("RETURN 1")
        value = cursor.evaluate()
        assert value == 1

    def test_can_evaluate_value_by_index(self):
        cursor = self.graph.run("RETURN 1, 2")
        value = cursor.evaluate(1)
        assert value == 2

    def test_can_evaluate_value_by_key(self):
        cursor = self.graph.run("RETURN 1 AS first, 2 AS second")
        value = cursor.evaluate("second")
        assert value == 2

    def test_evaluate_with_no_records_is_none(self):
        cursor = self.graph.run("RETURN 1")
        cursor.forward()
        value = cursor.evaluate()
        assert value is None

    def test_evaluate_on_non_existent_column_is_none(self):
        cursor = self.graph.run("RETURN 1")
        value = cursor.evaluate(1)
        assert value is None

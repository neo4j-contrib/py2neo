#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright 2011-2016, Nigel Small
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


from py2neo import Node, Relationship, order, size, remote, TransactionFinished, CypherSyntaxError, ConstraintError
from test.util import Py2neoTestCase


class TransactionRunTestCase(Py2neoTestCase):

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


class TransactionCreateTestCase(Py2neoTestCase):

    def test_can_create_node(self):
        a = Node("Person", name="Alice")
        with self.graph.begin() as tx:
            tx.create(a)
        assert remote(a)

    def test_can_create_relationship(self):
        a = Node("Person", name="Alice")
        b = Node("Person", name="Bob")
        r = Relationship(a, "KNOWS", b, since=1999)
        with self.graph.begin() as tx:
            tx.create(r)
        assert remote(a)
        assert remote(b)
        assert remote(r)
        assert r.start_node() == a
        assert r.end_node() == b

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
        assert remote(a)
        assert remote(b)
        assert remote(r)
        assert r.start_node() == a
        assert r.end_node() == b
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
        assert remote(a)
        assert remote(b)
        assert remote(r)
        assert r.start_node() == a
        assert r.end_node() == b
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
        assert remote(a)
        assert remote(b)
        assert remote(r)
        assert r.start_node() == a
        assert r.end_node() == b
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

    def test_create_is_idempotent(self):
        self.graph.delete_all()
        a = Node()
        b = Node()
        r = Relationship(a, "TO", b)
        with self.graph.begin() as tx:
            tx.create(r)
        assert remote(a)
        assert remote(b)
        assert remote(r)
        assert order(self.graph) == 2
        assert size(self.graph) == 1
        with self.graph.begin() as tx:
            tx.create(r)
        assert remote(a)
        assert remote(b)
        assert remote(r)
        assert order(self.graph) == 2
        assert size(self.graph) == 1


class TransactionDeleteTestCase(Py2neoTestCase):

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


class TransactionSeparateTestCase(Py2neoTestCase):

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


class TransactionDegreeTestCase(Py2neoTestCase):

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


class TransactionExistsTestCase(Py2neoTestCase):

    def test_cannot_check_existence_of_non_graphy_thing(self):
        with self.assertRaises(TypeError):
            with self.graph.begin() as tx:
                tx.exists("this string is definitely not graphy")


class TransactionErrorTestCase(Py2neoTestCase):

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
        parameters = {"A": record["a"], "B": record["b"]}
        statement = ("MATCH (a) WHERE id(a)={A} MATCH (b) WHERE id(b)={B}" +
                     "CREATE (a)-[:KNOWS]->(b)")
        tx.run(statement, parameters)
        tx.run(statement, parameters)
        statement = ("MATCH (a) WHERE id(a)={A} MATCH (b) WHERE id(b)={B}" +
                     "CREATE UNIQUE (a)-[:KNOWS]->(b)")
        tx.run(statement, parameters)
        with self.assertRaises(ConstraintError):
            tx.commit()


class TransactionAutocommitTestCase(Py2neoTestCase):

    def test_can_autocommit(self):
        tx = self.graph.begin(autocommit=True)
        assert not tx.finished()
        tx.run("RETURN 1")
        assert tx.finished()


class TransactionCoverageTestCase(Py2neoTestCase):
    """ These tests exist purely to make the coverage counter happy.
    """

    def test_base_class_rollback_does_nothing(self):
        from py2neo.database import Transaction
        tx = Transaction(self.graph)
        tx.rollback()

    def test_base_class_post_does_nothing(self):
        from py2neo.database import Transaction
        tx = Transaction(self.graph)
        tx._post()

    def test_base_class_run_does_nothing(self):
        from py2neo.database import Transaction
        tx = Transaction(self.graph)
        tx.run("")

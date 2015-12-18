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


from py2neo import Node, Relationship, Finished, GraphError
from py2neo.status import ClientError, CypherError
from py2neo.status.statement import InvalidSyntax, ConstraintViolation
from test.util import Py2neoTestCase


class TransactionRunTestCase(Py2neoTestCase):

    def test_can_run_single_statement_transaction(self):
        tx = self.cypher.begin()
        assert not tx.finished()
        cursor = tx.run("CREATE (a) RETURN a")
        tx.commit()
        records = list(cursor.collect())
        assert len(records) == 1
        for record in records:
            assert isinstance(record["a"], Node)
        assert tx.finished()

    def test_can_run_transaction_as_with_statement(self):
        with self.cypher.begin() as tx:
            assert not tx.finished()
            tx.run("CREATE (a) RETURN a")
        assert tx.finished()

    def test_can_run_multi_statement_transaction(self):
        tx = self.cypher.begin()
        assert not tx.finished()
        cursor_1 = tx.run("CREATE (a) RETURN a")
        cursor_2 = tx.run("CREATE (a) RETURN a")
        cursor_3 = tx.run("CREATE (a) RETURN a")
        tx.commit()
        for cursor in (cursor_1, cursor_2, cursor_3):
            records = list(cursor.collect())
            assert len(records) == 1
            for record in records:
                assert isinstance(record["a"], Node)
        assert tx.finished()

    def test_can_run_multi_execute_transaction(self):
        tx = self.cypher.begin()
        assert tx._id is None
        for i in range(10):
            assert not tx.finished()
            cursor_1 = tx.run("CREATE (a) RETURN a")
            cursor_2 = tx.run("CREATE (a) RETURN a")
            cursor_3 = tx.run("CREATE (a) RETURN a")
            tx.process()
            assert tx._id is not None
            for cursor in (cursor_1, cursor_2, cursor_3):
                records = list(cursor.collect())
                assert len(records) == 1
                for record in records:
                    assert isinstance(record["a"], Node)
        tx.commit()
        assert tx.finished()

    def test_can_rollback_transaction(self):
        tx = self.cypher.begin()
        for i in range(10):
            assert not tx.finished()
            cursor_1 = tx.run("CREATE (a) RETURN a")
            cursor_2 = tx.run("CREATE (a) RETURN a")
            cursor_3 = tx.run("CREATE (a) RETURN a")
            tx.process()
            assert tx._id is not None
            for cursor in (cursor_1, cursor_2, cursor_3):
                records = list(cursor.collect())
                assert len(records) == 1
                for record in records:
                    assert isinstance(record["a"], Node)
        tx.rollback()
        assert tx.finished()

    def test_cannot_append_after_transaction_finished(self):
        tx = self.cypher.begin()
        tx.rollback()
        try:
            tx.run("CREATE (a) RETURN a")
        except Finished as error:
            assert error.obj is tx
            assert repr(error) == "Transaction finished"
        else:
            assert False


class TransactionCreateTestCase(Py2neoTestCase):

    def test_can_create_node(self):
        a = Node("Person", name="Alice")
        with self.cypher.begin() as tx:
            tx.create(a)
        assert a.bound()

    def test_can_create_relationship(self):
        a = Node("Person", name="Alice")
        b = Node("Person", name="Bob")
        r = Relationship(a, "KNOWS", b, since=1999)
        with self.cypher.begin() as tx:
            tx.create(r)
        assert a.bound()
        assert b.bound()
        assert r.bound()
        assert r.start_node() == a
        assert r.end_node() == b

    def test_can_create_nodes_and_relationship_1(self):
        self.graph.delete_all()
        with self.cypher.begin() as tx:
            a = Node("Person", name="Alice")
            b = Node("Person", name="Bob")
            tx.create(a)
            tx.create(b)
            tx.process()
            r = Relationship(a, "KNOWS", b, since=1999)
            tx.create(r)
        assert a.bound()
        assert b.bound()
        assert r.bound()
        assert r.start_node() == a
        assert r.end_node() == b
        assert self.graph.order() == 2
        assert self.graph.size() == 1

    def test_can_create_nodes_and_relationship_2(self):
        self.graph.delete_all()
        with self.cypher.begin() as tx:
            a = Node("Person", name="Alice")
            b = Node("Person", name="Bob")
            tx.create(a)
            tx.create(b)
            r = Relationship(a, "KNOWS", b, since=1999)
            tx.create(r)
        assert a.bound()
        assert b.bound()
        assert r.bound()
        assert r.start_node() == a
        assert r.end_node() == b
        assert self.graph.order() == 2
        assert self.graph.size() == 1

    def test_can_create_nodes_and_relationship_3(self):
        self.graph.delete_all()
        with self.cypher.begin() as tx:
            a = Node("Person", name="Alice")
            b = Node("Person", name="Bob")
            r = Relationship(a, "KNOWS", b, since=1999)
            tx.create(a)
            tx.create(b)
            tx.create(r)
        assert a.bound()
        assert b.bound()
        assert r.bound()
        assert r.start_node() == a
        assert r.end_node() == b
        assert self.graph.order() == 2
        assert self.graph.size() == 1

    def test_can_create_nodes_and_relationship_4(self):
        self.graph.delete_all()
        with self.cypher.begin() as tx:
            a = Node()
            b = Node()
            c = Node()
            ab = Relationship(a, "TO", b)
            bc = Relationship(b, "TO", c)
            ca = Relationship(c, "TO", a)
            tx.create(ab | bc | ca)
        assert a.bound()
        assert b.bound()
        assert c.bound()
        assert ab.bound()
        assert ab.start_node() == a
        assert ab.end_node() == b
        assert bc.bound()
        assert bc.start_node() == b
        assert bc.end_node() == c
        assert ca.bound()
        assert ca.start_node() == c
        assert ca.end_node() == a
        assert self.graph.order() == 3
        assert self.graph.size() == 3

    def test_create_is_idempotent(self):
        self.graph.delete_all()
        a = Node()
        b = Node()
        r = Relationship(a, "TO", b)
        with self.cypher.begin() as tx:
            tx.create(r)
        assert a.bound()
        assert b.bound()
        assert r.bound()
        assert self.graph.order() == 2
        assert self.graph.size() == 1
        with self.cypher.begin() as tx:
            tx.create(r)
        assert a.bound()
        assert b.bound()
        assert r.bound()
        assert self.graph.order() == 2
        assert self.graph.size() == 1


class TransactionCreateUniqueTestCase(Py2neoTestCase):

    def setUp(self):
        self.graph.delete_all()
        self.a = Node("Person", name="Alice")
        self.b = Node("Person", name="Bob")
        self.ab = Relationship(self.a, "KNOWS", self.b, since=1999)
        with self.cypher.begin() as tx:
            tx.create(self.ab)

    def test_can_create_path_when_none_exists(self):
        c = Node("Person", name="Carol")
        ac = Relationship(self.a, "KNOWS", c)
        with self.cypher.begin() as tx:
            tx.create_unique(ac)
        assert c != self.b
        assert c.bound()
        assert ac.bound()
        assert ac.start_node() == self.a
        assert ac.end_node() == c
        assert self.graph.order() == 3
        assert self.graph.size() == 2

    def test_can_create_path_when_one_exists(self):
        b = Node("Person", name="Bob")
        ab = Relationship(self.a, "KNOWS", b)
        with self.cypher.begin() as tx:
            tx.create_unique(ab)
        assert b._id == self.b._id
        assert ab._id == self.ab._id
        assert b.bound()
        assert ab.bound()
        assert ab.start_node() == self.a
        assert ab.end_node() == b
        assert self.graph.order() == 2
        assert self.graph.size() == 1

    def test_can_create_path_beyond_existing_path(self):
        c = Node("Person", name="Carol")
        bc = Relationship(self.b, "KNOWS", c)
        with self.cypher.begin() as tx:
            tx.create_unique(self.ab + bc)
        assert c.bound()
        assert bc.bound()
        assert bc.start_node() == self.b
        assert bc.end_node() == c
        assert self.graph.order() == 3
        assert self.graph.size() == 2

    def test_can_create_path_with_reversed_relationship(self):
        c = Node("Person", name="Carol")
        cb = Relationship(c, "KNOWS", self.b)
        with self.cypher.begin() as tx:
            tx.create_unique(self.ab + cb)
        assert c.bound()
        assert cb.bound()
        assert cb.start_node() == c
        assert cb.end_node() == self.b
        assert self.graph.order() == 3
        assert self.graph.size() == 2

    def test_cannot_create_unique_without_bound_nodes(self):
        a = Node("Person", name="Alice")
        b = Node("Person", name="Bob")
        r = Relationship(a, "KNOWS", b, since=1999)
        with self.assertRaises(ValueError):
            with self.cypher.begin() as tx:
                tx.create_unique(r)


class TransactionDeleteTestCase(Py2neoTestCase):

    def test_can_delete_relationship(self):
        a = Node()
        b = Node()
        r = Relationship(a, "TO", b)
        self.graph.create(r)
        assert self.graph.exists(r)
        with self.cypher.begin() as tx:
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
        with self.cypher.begin() as tx:
            tx.separate(r)
        assert not self.graph.related(r)
        assert self.graph.exists(a)
        assert self.graph.exists(b)


class TransactionErrorTestCase(Py2neoTestCase):

    def test_can_generate_transaction_error(self):
        tx = self.cypher.begin()
        try:
            tx.run("CRAETE (a) RETURN a")
            tx.commit()
        except InvalidSyntax as err:
            assert repr(err)
        else:
            assert False

    def test_unique_path_not_unique_raises_cypher_transaction_error_in_transaction(self):
        tx = self.cypher.begin()
        cursor = tx.run("CREATE (a), (b) RETURN a, b")
        tx.process()
        record = cursor.select()
        parameters = {"A": record["a"]._id, "B": record["b"]._id}
        statement = ("MATCH (a) WHERE id(a)={A} MATCH (b) WHERE id(b)={B}" +
                     "CREATE (a)-[:KNOWS]->(b)")
        tx.run(statement, parameters)
        tx.run(statement, parameters)
        statement = ("MATCH (a) WHERE id(a)={A} MATCH (b) WHERE id(b)={B}" +
                     "CREATE UNIQUE (a)-[:KNOWS]->(b)")
        tx.run(statement, parameters)
        try:
            tx.commit()
        except CypherError as error:
            self.assert_new_error(
                error, (ConstraintViolation, ClientError, CypherError,
                        GraphError), "Neo.ClientError.Statement.ConstraintViolation")
        else:
            assert False

    def test_can_hydrate_error_for_all_known_codes(self):
        codes = [
            "Neo.ClientError.General.ReadOnly",
            "Neo.ClientError.Request.Invalid",
            "Neo.ClientError.Request.InvalidFormat",
            "Neo.ClientError.Schema.ConstraintAlreadyExists",
            "Neo.ClientError.Schema.ConstraintVerificationFailure",
            "Neo.ClientError.Schema.ConstraintViolation",
            "Neo.ClientError.Schema.IllegalTokenName",
            "Neo.ClientError.Schema.IndexAlreadyExists",
            "Neo.ClientError.Schema.IndexBelongsToConstraint",
            "Neo.ClientError.Schema.LabelLimitReached",
            "Neo.ClientError.Schema.NoSuchConstraint",
            "Neo.ClientError.Schema.NoSuchIndex",
            "Neo.ClientError.Statement.ArithmeticError",
            "Neo.ClientError.Statement.ConstraintViolation",
            "Neo.ClientError.Statement.EntityNotFound",
            "Neo.ClientError.Statement.InvalidArguments",
            "Neo.ClientError.Statement.InvalidSemantics",
            "Neo.ClientError.Statement.InvalidSyntax",
            "Neo.ClientError.Statement.InvalidType",
            "Neo.ClientError.Statement.NoSuchLabel",
            "Neo.ClientError.Statement.NoSuchProperty",
            "Neo.ClientError.Statement.ParameterMissing",
            "Neo.ClientError.Transaction.ConcurrentRequest",
            "Neo.ClientError.Transaction.EventHandlerThrewException",
            "Neo.ClientError.Transaction.InvalidType",
            "Neo.ClientError.Transaction.UnknownId",
            "Neo.DatabaseError.General.CorruptSchemaRule",
            "Neo.DatabaseError.General.FailedIndex",
            "Neo.DatabaseError.General.UnknownFailure",
            "Neo.DatabaseError.Schema.ConstraintCreationFailure",
            "Neo.DatabaseError.Schema.ConstraintDropFailure",
            "Neo.DatabaseError.Schema.IndexCreationFailure",
            "Neo.DatabaseError.Schema.IndexDropFailure",
            "Neo.DatabaseError.Schema.NoSuchLabel",
            "Neo.DatabaseError.Schema.NoSuchPropertyKey",
            "Neo.DatabaseError.Schema.NoSuchRelationshipType",
            "Neo.DatabaseError.Schema.NoSuchSchemaRule",
            "Neo.DatabaseError.Statement.ExecutionFailure",
            "Neo.DatabaseError.Transaction.CouldNotBegin",
            "Neo.DatabaseError.Transaction.CouldNotCommit",
            "Neo.DatabaseError.Transaction.CouldNotRollback",
            "Neo.DatabaseError.Transaction.ReleaseLocksFailed",
            "Neo.TransientError.Network.UnknownFailure",
            "Neo.TransientError.Statement.ExternalResourceFailure",
            "Neo.TransientError.Transaction.AcquireLockTimeout",
            "Neo.TransientError.Transaction.DeadlockDetected",
        ]
        for code in codes:
            data = {"code": code, "message": "X"}
            _, classification, category, title = code.split(".")
            error = CypherError.hydrate(data)
            assert error.code == code
            assert error.message == "X"
            assert error.__class__.__name__ == title
            assert error.__class__.__mro__[1].__name__ == classification
            assert error.__class__.__module__ == "py2neo.status.%s" % category.lower()
            assert isinstance(error, CypherError)
            assert isinstance(error, GraphError)

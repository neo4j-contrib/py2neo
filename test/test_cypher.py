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

from py2neo import Node, NodePointer, Relationship, Path, Finished, GraphError
from py2neo.cypher import CypherEngine, Transaction, \
    CreateStatement, DeleteStatement
from py2neo.cypher.core import presubstitute
from py2neo.cypher.error.core import CypherError, TransactionError, ClientError
from py2neo.cypher.error.statement import ConstraintViolation, InvalidSyntax
from py2neo.lang import CypherWriter, cypher_repr
from py2neo.cypher.task import CypherTask, CreateNode, MergeNode, CreateRelationship
from py2neo.lang import Writer
from py2neo.packages.httpstream import ClientError as _ClientError, Response as _Response
from test.util import Py2neoTestCase


class TemporaryTransaction(object):

    def __init__(self, graph):
        self.tx = graph.cypher.begin()

    def __del__(self):
        self.tx.rollback()

    def run(self, statement, parameters=None, **kwparameters):
        return self.tx.run(statement, parameters, **kwparameters)


class WriterTestCase(Py2neoTestCase):

    def test_base_writer_cannot_write(self):
        writer = Writer()
        with self.assertRaises(NotImplementedError):
            writer.write(None)


class CypherTestCase(Py2neoTestCase):

    def setUp(self):
        self.alice_and_bob = self.graph.create(
            {"name": "Alice", "age": 66},
            {"name": "Bob", "age": 77},
            (0, "KNOWS", 1),
        )

    def test_can_run_cypher(self):
        result = self.cypher.run("RETURN 1")
        assert len(result) == 1
        first = result[0]
        assert len(first) == 1
        value = first[0]
        assert value == 1

    def test_can_create_cypher_engine(self):
        uri = "http://localhost:7474/db/data/transaction"
        cypher = CypherEngine(uri)
        assert cypher.uri == uri

    def test_cypher_engines_with_identical_arguments_are_same_objects(self):
        uri = "http://localhost:7474/db/data/cypher"
        cypher_1 = CypherEngine(uri)
        cypher_2 = CypherEngine(uri)
        assert cypher_1 is cypher_2

    def test_can_run_cypher_statement(self):
        self.cypher.run("MERGE (a:Person {name:'Alice'})")

    def test_can_run_parametrised_cypher_statement(self):
        self.cypher.run("MERGE (a:Person {name:{N}})", {"N": "Alice"})

    def test_can_run_cypher_statement(self):
        value = self.cypher.evaluate("MERGE (a:Person {name:'Alice'}) RETURN a")
        assert isinstance(value, Node)
        assert value.labels() == {"Person"}
        assert dict(value) == {"name": "Alice"}

    def test_can_run_parametrised_cypher_statement(self):
        value = self.cypher.evaluate("MERGE (a:Person {name:{N}}) RETURN a", {"N": "Alice"})
        assert isinstance(value, Node)
        assert value.labels() == {"Person"}
        assert dict(value) == {"name": "Alice"}

    def test_can_run_cypher_statement_with_node_parameter(self):
        alice = Node(name="Alice")
        self.graph.create(alice)
        statement = "MATCH (a) WHERE id(a) = {N} RETURN a"
        result = self.cypher.run(statement, {"N": alice})
        assert result[0]["a"] is alice

    def test_can_evaluate_cypher_statement(self):
        result = self.cypher.evaluate("MERGE (a:Person {name:'Alice'}) RETURN a")
        assert isinstance(result, Node)
        assert result.labels() == {"Person"}
        assert dict(result) == {"name": "Alice"}

    def test_can_evaluate_parametrised_cypher_statement(self):
        result = self.cypher.evaluate("MERGE (a:Person {name:{N}}) RETURN a", {"N": "Alice"})
        assert isinstance(result, Node)
        assert result.labels() == {"Person"}
        assert dict(result) == {"name": "Alice"}

    def test_evaluate_with_no_results_returns_none(self):
        result = self.cypher.evaluate("CREATE (a {name:{N}})", {"N": "Alice"})
        assert result is None

    def test_can_begin_transaction(self):
        uri = "http://localhost:7474/db/data/transaction"
        cypher = CypherEngine(uri)
        tx = cypher.begin()
        assert isinstance(tx, Transaction)

    def test_nonsense_query(self):
        statement = "SELECT z=nude(0) RETURNS x"
        try:
            self.cypher.run(statement)
        except TransactionError as error:
            assert error.code == "Neo.ClientError.Statement.InvalidSyntax"
        except CypherError as error:
            assert error.exception == "SyntaxException"
            assert error.fullname in [None, "org.neo4j.cypher.SyntaxException"]
            assert error.statement == statement
            assert not error.parameters
        else:
            assert False

    def test_can_run_statement(self):
        results = self.cypher.run("CREATE (a {name:'Alice'}) RETURN a.name AS name")
        assert len(results) == 1
        assert results[0]["name"] == "Alice"

    def test_can_run_with_parameter(self):
        results = self.cypher.run("CREATE (a {name:{N}}) "
                                  "RETURN a.name AS name", {"N": "Alice"})
        assert len(results) == 1
        assert results[0]["name"] == "Alice"

    def test_can_run_with_entity_parameter(self):
        alice, = self.graph.create({"name": "Alice"})
        statement = "MATCH (a) WHERE id(a)={N} RETURN a.name AS name"
        results = self.cypher.run(statement, {"N": alice})
        assert len(results) == 1
        assert results[0]["name"] == "Alice"

    def test_can_evaluate(self):
        result = self.cypher.evaluate("CREATE (a {name:'Alice'}) RETURN a.name AS name")
        assert result == "Alice"

    def test_can_evaluate_where_none_returned(self):
        statement = "MATCH (a) WHERE 2 + 2 = 5 RETURN a.name AS name"
        result = self.cypher.evaluate(statement)
        assert result is None

    def test_can_convert_to_subgraph(self):
        results = self.cypher.run("CREATE (a)-[ab:KNOWS]->(b) RETURN a, ab, b")
        subgraph = results.to_subgraph()
        assert subgraph.order() == 2
        assert subgraph.size() == 1

    def test_nonsense_query_with_error_handler(self):
        with self.assertRaises(CypherError):
            self.cypher.run("SELECT z=nude(0) RETURNS x")

    def test_query(self):
        a, b, ab = self.alice_and_bob
        statement = ("MATCH (a) WHERE id(a)={A} "
                     "MATCH (b) WHERE id(b)={B} "
                     "MATCH (a)-[ab:KNOWS]->(b) "
                     "RETURN a, b, ab, a.name AS a_name, b.name AS b_name")
        results = self.cypher.run(statement, {"A": a._id, "B": b._id})
        assert len(results) == 1
        for record in results:
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
        results = self.cypher.run(statement, {"A": a._id, "B": b._id})
        assert len(results) == 1
        for record in results:
            assert isinstance(record["p"], Path)
            nodes = record["p"].nodes()
            assert len(nodes) == 2
            assert nodes[0] == a
            assert nodes[1] == b
            assert record["p"][0].type() == "KNOWS"

    def test_query_can_return_collection(self):
        node, = self.graph.create({})
        statement = "MATCH (a) WHERE id(a)={N} RETURN collect(a) AS a_collection"
        params = {"N": node._id}
        results = self.cypher.run(statement, params)
        assert results[0]["a_collection"] == [node]

    def test_param_used_once(self):
        node, = self.graph.create({})
        statement = "MATCH (a) WHERE id(a)={X} RETURN a"
        params = {"X": node._id}
        results = self.cypher.run(statement, params)
        record = results[0]
        assert record["a"] == node

    def test_param_used_twice(self):
        node, = self.graph.create({})
        statement = "MATCH (a) WHERE id(a)={X} MATCH (b) WHERE id(b)={X} RETURN a, b"
        params = {"X": node._id}
        results = self.cypher.run(statement, params)
        record = results[0]
        assert record["a"] == node
        assert record["b"] == node

    def test_param_used_thrice(self):
        node, = self.graph.create({})
        statement = "MATCH (a) WHERE id(a)={X} " \
                    "MATCH (b) WHERE id(b)={X} " \
                    "MATCH (c) WHERE id(c)={X} " \
                    "RETURN a, b, c"
        params = {"X": node._id}
        results = self.cypher.run(statement, params)
        record = results[0]
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
        params = {"A": a._id, "min_age": 50}
        results = self.cypher.run(query, params)
        record = results[0]
        assert record["b"] == b

    def test_param_reused_twice_after_with_statement(self):
        a, b, ab = self.alice_and_bob
        c, bc = self.graph.create(
            {"name": "Carol", "age": 88},
            (b, "KNOWS", 0),
        )
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
        params = {"A": a._id, "min_age": 50}
        results = self.cypher.run(query, params)
        record = results[0]
        assert record["c"] == c

    def test_invalid_syntax_raises_cypher_error(self):
        cypher = self.cypher
        try:
            cypher.run("X")
        except TransactionError as error:
            assert error.code == "Neo.ClientError.Statement.InvalidSyntax"
        except CypherError as error:
            self.assert_error(
                error, (CypherError, GraphError), "org.neo4j.cypher.SyntaxException",
                (_ClientError, _Response), 400)
        else:
            assert False

    def test_unique_path_not_unique_raises_cypher_error(self):
        cypher = self.cypher
        results = cypher.run("CREATE (a), (b) RETURN a, b")
        parameters = {"A": results[0]["a"], "B": results[0]["b"]}
        statement = ("MATCH (a) WHERE id(a)={A} MATCH (b) WHERE id(b)={B}" +
                     "CREATE (a)-[:KNOWS]->(b)")
        cypher.run(statement, parameters)
        cypher.run(statement, parameters)
        try:
            statement = ("MATCH (a) WHERE id(a)={A} MATCH (b) WHERE id(b)={B}" +
                         "CREATE UNIQUE (a)-[:KNOWS]->(b)")
            cypher.run(statement, parameters)
        except TransactionError as error:
            assert error.code == "Neo.ClientError.Statement.ConstraintViolation"
        except CypherError as error:
            self.assert_error(
                error, (CypherError, GraphError), "org.neo4j.cypher.UniquePathNotUniqueException",
                (_ClientError, _Response), 400)
        else:
            assert False


class CypherTransactionTestCase(Py2neoTestCase):

    def test_can_run_single_statement_transaction(self):
        tx = self.cypher.begin()
        assert not tx.finished()
        result = tx.run("CREATE (a) RETURN a")
        tx.commit()
        assert len(result) == 1
        for record in result:
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
        result_1 = tx.run("CREATE (a) RETURN a")
        result_2 = tx.run("CREATE (a) RETURN a")
        result_3 = tx.run("CREATE (a) RETURN a")
        tx.commit()
        for result in (result_1, result_2, result_3):
            assert len(result) == 1
            for record in result:
                assert isinstance(record["a"], Node)
        assert tx.finished()

    def test_can_run_multi_execute_transaction(self):
        tx = self.cypher.begin()
        assert tx._id is None
        for i in range(10):
            assert not tx.finished()
            result_1 = tx.run("CREATE (a) RETURN a")
            result_2 = tx.run("CREATE (a) RETURN a")
            result_3 = tx.run("CREATE (a) RETURN a")
            tx.process()
            assert tx._id is not None
            for result in (result_1, result_2, result_3):
                assert len(result) == 1
                for record in result:
                    assert isinstance(record["a"], Node)
        tx.commit()
        assert tx.finished()

    def test_can_rollback_transaction(self):
        tx = self.cypher.begin()
        for i in range(10):
            assert not tx.finished()
            result_1 = tx.run("CREATE (a) RETURN a")
            result_2 = tx.run("CREATE (a) RETURN a")
            result_3 = tx.run("CREATE (a) RETURN a")
            tx.process()
            assert tx._id is not None
            for result in (result_1, result_2, result_3):
                assert len(result) == 1
                for record in result:
                    assert isinstance(record["a"], Node)
        tx.rollback()
        assert tx.finished()

    def test_can_generate_transaction_error(self):
        tx = self.cypher.begin()
        try:
            tx.run("CRAETE (a) RETURN a")
            tx.commit()
        except InvalidSyntax as err:
            assert repr(err)
        else:
            assert False

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

    def test_unique_path_not_unique_raises_cypher_transaction_error_in_transaction(self):
        tx = self.cypher.begin()
        result = tx.run("CREATE (a), (b) RETURN a, b")
        tx.process()
        record = result[0]
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
        except TransactionError as error:
            self.assert_new_error(
                error, (ConstraintViolation, ClientError, TransactionError,
                        CypherError, GraphError), "Neo.ClientError.Statement.ConstraintViolation")
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
            error = TransactionError.hydrate(data)
            assert error.code == code
            assert error.message == "X"
            assert error.__class__.__name__ == title
            assert error.__class__.__mro__[1].__name__ == classification
            assert error.__class__.__module__ == "py2neo.cypher.error.%s" % category.lower()
            assert isinstance(error, TransactionError)
            assert isinstance(error, CypherError)
            assert isinstance(error, GraphError)


class CypherSpecialistExecutionTestCase(Py2neoTestCase):

    def test_can_create(self):
        tx = self.cypher.begin()
        
        tx.commit()


class CypherCreateTestCase(Py2neoTestCase):

    def test_statement_representations_return_cypher(self):
        node = Node()
        statement = CreateStatement(self.graph)
        statement.create(node)
        assert statement.__repr__() == 'CREATE (_0)\nRETURN _0'
        assert statement.__str__() == 'CREATE (_0)\nRETURN _0'
        assert statement.__unicode__() == 'CREATE (_0)\nRETURN _0'

    def test_empty_statement_returns_empty_tuple(self):
        statement = CreateStatement(self.graph)
        created = statement.execute()
        assert created == ()

    def test_cannot_create_uncastable_type(self):
        statement = CreateStatement(self.graph)
        with self.assertRaises(TypeError):
            statement.create("this is not a valid thing to create")

    def test_cannot_create_none(self):
        statement = CreateStatement(self.graph)
        with self.assertRaises(TypeError):
            statement.create(None)

    def test_can_create_naked_node(self):
        node = Node()
        statement = CreateStatement(self.graph)
        statement.create(node)
        created = statement.execute()
        assert created == (node,)
        assert node.bound

    def test_can_create_node_with_properties(self):
        node = Node(name="Alice")
        statement = CreateStatement(self.graph)
        statement.create(node)
        created = statement.execute()
        assert created == (node,)
        assert node.bound

    def test_can_create_node_with_label(self):
        node = Node("Person", name="Alice")
        statement = CreateStatement(self.graph)
        statement.create(node)
        created = statement.execute()
        assert created == (node,)
        assert node.bound

    def test_cannot_create_unique_node(self):
        node = Node(name="Alice")
        statement = CreateStatement(self.graph)
        with self.assertRaises(TypeError):
            statement.create_unique(node)

    def test_can_create_two_nodes_and_a_relationship(self):
        alice = Node(name="Alice")
        bob = Node(name="Bob")
        alice_knows_bob = Relationship(alice, "KNOWS", bob)
        statement = CreateStatement(self.graph)
        statement.create(alice)
        statement.create(bob)
        statement.create(alice_knows_bob)
        created = statement.execute()
        assert created == (alice, bob, alice_knows_bob)
        assert alice.bound
        assert bob.bound
        assert alice_knows_bob.bound
        assert alice_knows_bob.start_node() is alice
        assert alice_knows_bob.end_node() is bob

    def test_can_create_two_nodes_and_a_unique_relationship(self):
        alice = Node(name="Alice")
        bob = Node(name="Bob")
        alice_knows_bob = Relationship(alice, "KNOWS", bob)
        statement = CreateStatement(self.graph)
        statement.create(alice)
        statement.create(bob)
        statement.create_unique(alice_knows_bob)
        created = statement.execute()
        assert created == (alice, bob, alice_knows_bob)
        assert alice.bound
        assert bob.bound
        assert alice_knows_bob.bound
        assert alice_knows_bob.start_node() is alice
        assert alice_knows_bob.end_node() is bob

    def test_can_create_two_nodes_and_a_relationship_with_properties(self):
        alice = Node(name="Alice")
        bob = Node(name="Bob")
        alice_knows_bob = Relationship(alice, "KNOWS", bob, since=1999)
        statement = CreateStatement(self.graph)
        statement.create(alice)
        statement.create(bob)
        statement.create(alice_knows_bob)
        created = statement.execute()
        assert created == (alice, bob, alice_knows_bob)
        assert alice.bound
        assert bob.bound
        assert alice_knows_bob.bound
        assert alice_knows_bob.start_node() is alice
        assert alice_knows_bob.end_node() is bob

    def test_can_create_two_nodes_and_a_relationship_using_pointers(self):
        alice = Node(name="Alice")
        bob = Node(name="Bob")
        alice_knows_bob = Relationship(0, "KNOWS", 1)
        statement = CreateStatement(self.graph)
        statement.create(alice)
        statement.create(bob)
        statement.create(alice_knows_bob)
        created = statement.execute()
        assert created == (alice, bob, alice_knows_bob)
        assert alice.bound
        assert bob.bound
        assert alice_knows_bob.bound
        assert alice_knows_bob.start_node() is alice
        assert alice_knows_bob.end_node() is bob

    def test_cannot_use_a_pointer_that_does_not_refer_to_a_node(self):
        alice = Node(name="Alice")
        bob = Node(name="Bob")
        alice_knows_bob = Relationship(0, "KNOWS", 1)
        broken_relationship = Relationship(0, "KNOWS", 2)
        statement = CreateStatement(self.graph)
        statement.create(alice)
        statement.create(bob)
        statement.create(alice_knows_bob)
        with self.assertRaises(ValueError):
            statement.create(broken_relationship)

    def test_cannot_use_a_pointer_that_is_out_of_range(self):
        broken_relationship = Relationship(10, "KNOWS", 11)
        statement = CreateStatement(self.graph)
        with self.assertRaises(IndexError):
            statement.create(broken_relationship)

    def test_can_create_one_node_and_a_relationship_to_an_existing_node(self):
        alice = self.cypher.evaluate("CREATE (a {name:'Alice'}) RETURN a")
        bob = Node(name="Bob")
        alice_knows_bob = Relationship(alice, "KNOWS", bob)
        statement = CreateStatement(self.graph)
        statement.create(bob)
        statement.create(alice_knows_bob)
        created = statement.execute()
        assert created == (bob, alice_knows_bob)
        assert bob.bound
        assert alice_knows_bob.bound
        assert alice_knows_bob.start_node() is alice
        assert alice_knows_bob.end_node() is bob

    def test_can_create_a_relationship_to_two_existing_nodes(self):
        alice = self.cypher.evaluate("CREATE (a {name:'Alice'}) RETURN a")
        bob = self.cypher.evaluate("CREATE (b {name:'Bob'}) RETURN b")
        alice_knows_bob = Relationship(alice, "KNOWS", bob)
        statement = CreateStatement(self.graph)
        statement.create(alice_knows_bob)
        created = statement.execute()
        assert created == (alice_knows_bob,)
        assert alice_knows_bob.bound
        assert alice_knows_bob.start_node() is alice
        assert alice_knows_bob.end_node() is bob

    def test_can_pass_entities_that_already_exist(self):
        results = self.cypher.run("CREATE (a)-[ab:KNOWS]->(b) RETURN a, ab, b")
        alice, alice_knows_bob, bob = results[0]
        statement = CreateStatement(self.graph)
        statement.create(alice)
        statement.create(bob)
        statement.create(alice_knows_bob)
        created = statement.execute()
        assert created == (alice, bob, alice_knows_bob)

    def test_a_unique_relationship_is_really_unique(self):
        results = self.cypher.run("CREATE (a)-[ab:KNOWS]->(b) RETURN a, ab, b")
        alice, alice_knows_bob, bob = results[0]
        assert alice.degree() == 1
        assert bob.degree() == 1
        statement = CreateStatement(self.graph)
        statement.create_unique(Relationship(alice, "KNOWS", bob))
        statement.execute()
        assert alice.degree() == 1
        assert bob.degree() == 1

    def test_unique_path_creation_can_pick_up_existing_entities(self):
        results = self.cypher.run("CREATE (a)-[ab:KNOWS]->(b) RETURN a, ab, b")
        alice, alice_knows_bob, bob = results[0]
        statement = CreateStatement(self.graph)
        statement.create_unique(Relationship(alice, "KNOWS", Node()))
        created = statement.execute()
        assert created == (alice_knows_bob,)
        assert alice_knows_bob.start_node() == alice
        assert alice_knows_bob.end_node() == bob

    def test_unique_path_not_unique_exception(self):
        results = self.cypher.run("CREATE (a)-[ab:KNOWS]->(b), "
                                      "(a)-[:KNOWS]->(b) RETURN a, ab, b")
        alice, alice_knows_bob, bob = results[0]
        assert alice.degree() == 2
        assert bob.degree() == 2
        statement = CreateStatement(self.graph)
        statement.create_unique(Relationship(alice, "KNOWS", bob))
        with self.assertRaises(ConstraintViolation):
            statement.execute()

    def test_can_create_an_entirely_new_path(self):
        alice = Node(name="Alice")
        bob = Node(name="Bob")
        carol = Node(name="Carol")
        dave = Node(name="Dave")
        path = Path(alice, "LOVES", bob, Relationship(carol, "HATES", bob), carol, "KNOWS", dave)
        statement = CreateStatement(self.graph)
        statement.create(path)
        created = statement.execute()
        assert created == (path,)
        assert alice.bound
        assert bob.bound
        assert carol.bound
        assert dave.bound
        ab, cb, cd = path.relationships()
        assert ab.start_node() is alice
        assert ab.end_node() is bob
        assert cb.start_node() is carol
        assert cb.end_node() is bob
        assert cd.start_node() is carol
        assert cd.end_node() is dave

    def test_can_create_a_path_with_existing_nodes(self):
        alice = self.cypher.evaluate("CREATE (a {name:'Alice'}) RETURN a")
        alice_id = alice._id
        bob = Node(name="Bob")
        carol = self.cypher.evaluate("CREATE (c {name:'Carol'}) RETURN c")
        carol_id = carol._id
        dave = Node(name="Dave")
        path = Path(alice, "LOVES", bob, Relationship(carol, "HATES", bob), carol, "KNOWS", dave)
        statement = CreateStatement(self.graph)
        statement.create(path)
        created = statement.execute()
        assert created == (path,)
        assert path.nodes()[0]._id == alice_id
        assert path.nodes()[2]._id == carol_id
        assert bob.bound
        assert dave.bound
        ab, cb, cd = path.relationships()
        assert ab.start_node() is alice
        assert ab.end_node() is bob
        assert cb.start_node() is carol
        assert cb.end_node() is bob
        assert cd.start_node() is carol
        assert cd.end_node() is dave

    def test_cannot_create_unique_zero_length_path(self):
        path = Path(Node())
        statement = CreateStatement(self.graph)
        with self.assertRaises(ValueError):
            statement.create_unique(path)

    def test_cannot_create_unique_path_with_no_bound_nodes(self):
        path = Path(Node(), "KNOWS", Node())
        statement = CreateStatement(self.graph)
        with self.assertRaises(ValueError):
            statement.create_unique(path)


class CypherDeleteTestCase(Py2neoTestCase):

    def test_statement_representations_return_cypher(self):
        node = Node()
        self.graph.create(node)
        statement = DeleteStatement(self.graph)
        statement.delete(node)
        assert node in statement
        s = 'MATCH (_0) WHERE id(_0)={_0}\nDELETE _0'
        assert statement.__repr__() == s
        assert statement.__str__() == s
        assert statement.__unicode__() == s


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
        try:
            writer.write_identifier("")
        except ValueError:
            assert True
        else:
            assert False

    def test_cannot_write_none_identifier(self):
        string = StringIO()
        writer = CypherWriter(string)
        try:
            writer.write_identifier(None)
        except ValueError:
            assert True
        else:
            assert False

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

    def test_can_write_node_pointer(self):
        string = StringIO()
        writer = CypherWriter(string)
        writer.write(NodePointer(42))
        written = string.getvalue()
        assert written == "(*42)"

    def test_can_write_relationship_containing_node_pointer(self):
        string = StringIO()
        writer = CypherWriter(string)
        writer.write(Relationship(NodePointer(42), "KNOWS", {}))
        written = string.getvalue()
        assert written == "(*42)-[:KNOWS]->()"

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


class CypherTaskTestCase(Py2neoTestCase):

    def test_bare_task(self):
        statement = "spam"
        parameters = {"foo": "bar"}
        snip = CypherTask(statement, parameters)
        assert snip.__repr__() == "<CypherTask statement=%r parameters=%r>" % \
                                  (statement, parameters)
        assert snip.__str__() == statement
        assert snip.__unicode__() == statement
        assert snip.statement == statement
        assert snip.parameters == parameters

    def test_create_empty_node(self):
        snip = CreateNode()
        assert snip.statement == "CREATE (a)"
        assert snip.parameters == {}

    def test_create_empty_node_with_return(self):
        snip = CreateNode().with_return()
        assert snip.statement == "CREATE (a) RETURN a"
        assert snip.parameters == {}

    def test_create_node_with_label(self):
        snip = CreateNode("Person")
        assert snip.statement == "CREATE (a:Person)"
        assert snip.parameters == {}

    def test_create_node_with_label_and_return(self):
        snip = CreateNode("Person").with_return()
        assert snip.statement == "CREATE (a:Person) RETURN a"
        assert snip.parameters == {}

    def test_create_node_with_labels(self):
        snip = CreateNode("Homo Sapiens", "Female")
        assert snip.labels == {"Homo Sapiens", "Female"}
        assert snip.statement == "CREATE (a:Female:`Homo Sapiens`)"
        assert snip.parameters == {}

    def test_create_node_with_labels_and_return(self):
        snip = CreateNode("Homo Sapiens", "Female").with_return()
        assert snip.statement == "CREATE (a:Female:`Homo Sapiens`) RETURN a"
        assert snip.parameters == {}

    def test_create_node_with_labels_and_properties(self):
        snip = CreateNode("Homo Sapiens", "Female", name="Alice", age=33, active=True)
        assert snip.statement == "CREATE (a:Female:`Homo Sapiens` {A})"
        assert snip.parameters == {"A": {"name": "Alice", "age": 33, "active": True}}

    def test_create_node_with_labels_and_properties_and_return(self):
        snip = CreateNode("Homo Sapiens", "Female", name="Alice", age=33, active=True).with_return()
        assert snip.statement == "CREATE (a:Female:`Homo Sapiens` {A}) RETURN a"
        assert snip.parameters == {"A": {"name": "Alice", "age": 33, "active": True}}

    def test_create_node_with_set(self):
        snip = CreateNode().set("Person", name="Alice")
        assert snip.statement == "CREATE (a:Person {A})"
        assert snip.parameters == {"A": {"name": "Alice"}}

    def test_create_node_with_set_and_return(self):
        snip = CreateNode().set("Person", name="Alice").with_return()
        assert snip.statement == "CREATE (a:Person {A}) RETURN a"
        assert snip.parameters == {"A": {"name": "Alice"}}

    def test_merge_node(self):
        snip = MergeNode("Person", "name", "Alice")
        assert snip.statement == "MERGE (a:Person {name:{A1}})"
        assert snip.parameters == {"A1": "Alice"}

    def test_merge_node_with_return(self):
        snip = MergeNode("Person", "name", "Alice").with_return()
        assert snip.statement == "MERGE (a:Person {name:{A1}}) RETURN a"
        assert snip.parameters == {"A1": "Alice"}

    def test_merge_node_without_property(self):
        snip = MergeNode("Person")
        assert snip.primary_label == "Person"
        assert snip.primary_key is None
        assert snip.primary_value is None
        assert snip.statement == "MERGE (a:Person)"
        assert snip.parameters == {}

    def test_merge_node_without_property_with_return(self):
        snip = MergeNode("Person").with_return()
        assert snip.primary_label == "Person"
        assert snip.primary_key is None
        assert snip.primary_value is None
        assert snip.statement == "MERGE (a:Person) RETURN a"
        assert snip.parameters == {}

    def test_merge_node_with_extra_values(self):
        snip = MergeNode("Person", "name", "Alice").set("Employee", employee_id=1234)
        assert snip.labels == {"Person", "Employee"}
        assert snip.statement == "MERGE (a:Person {name:{A1}}) SET a:Employee SET a={A}"
        assert snip.parameters == {"A1": "Alice", "A": {"employee_id": 1234}}

    def test_merge_node_with_extra_values_and_return(self):
        snip = MergeNode("Person", "name", "Alice").set("Employee", employee_id=1234).with_return()
        assert snip.statement == "MERGE (a:Person {name:{A1}}) SET a:Employee SET a={A} RETURN a"
        assert snip.parameters == {"A1": "Alice", "A": {"employee_id": 1234}}

    def test_create_relationship_and_both_nodes(self):
        t = CreateRelationship(Node("Person", name="Alice"), "KNOWS", Node("Person", name="Bob"))
        assert t.statement == "CREATE (a:Person {A}) " \
                              "CREATE (b:Person {B}) " \
                              "CREATE (a)-[r:KNOWS]->(b)"
        assert t.parameters == {"A": {"name": "Alice"}, "B": {"name": "Bob"}}

    def test_create_relationship_with_properties_and_both_nodes(self):
        t = CreateRelationship(Node("Person", name="Alice"), "KNOWS", Node("Person", name="Bob"),
                               since=1999)
        assert t.statement == "CREATE (a:Person {A}) " \
                              "CREATE (b:Person {B}) " \
                              "CREATE (a)-[r:KNOWS {R}]->(b)"
        assert t.parameters == {"A": {"name": "Alice"}, "B": {"name": "Bob"}, "R": {"since": 1999}}

    def test_create_relationship_and_start_node(self):
        alice = Node("Person", name="Alice")
        alice.bind("http://localhost:7474/db/data/node/1")
        t = CreateRelationship(alice, "KNOWS", Node("Person", name="Bob"))
        assert t.statement == "MATCH (a) WHERE id(a)={A} " \
                              "CREATE (b:Person {B}) " \
                              "CREATE (a)-[r:KNOWS]->(b)"
        assert t.parameters == {"A": 1, "B": {"name": "Bob"}}

    def test_create_relationship_and_end_node(self):
        bob = Node("Person", name="Bob")
        bob.bind("http://localhost:7474/db/data/node/2")
        t = CreateRelationship(Node("Person", name="Alice"), "KNOWS", bob)
        assert t.statement == "CREATE (a:Person {A}) " \
                              "MATCH (b) WHERE id(b)={B} " \
                              "CREATE (a)-[r:KNOWS]->(b)"
        assert t.parameters == {"A": {"name": "Alice"}, "B": 2}

    def test_create_relationship_only(self):
        alice = Node("Person", name="Alice")
        alice.bind("http://localhost:7474/db/data/node/1")
        bob = Node("Person", name="Bob")
        bob.bind("http://localhost:7474/db/data/node/2")
        t = CreateRelationship(alice, "KNOWS", bob)
        assert t.statement == "MATCH (a) WHERE id(a)={A} " \
                              "MATCH (b) WHERE id(b)={B} " \
                              "CREATE (a)-[r:KNOWS]->(b)"
        assert t.parameters == {"A": 1, "B": 2}


class CypherPresubstitutionTestCase(Py2neoTestCase):

    def new_tx(self):
        return TemporaryTransaction(self.graph)

    def test_can_use_parameter_for_property_value(self):
        tx = self.new_tx()
        if tx:
            result, = tx.run("CREATE (a:`Homo Sapiens` {`full name`:{v}}) "
                                 "RETURN labels(a), a.`full name`",
                                 v="Alice Smith")
            assert set(result[0]) == {"Homo Sapiens"}
            assert result[1] == "Alice Smith"

    def test_can_use_parameter_for_property_set(self):
        tx = self.new_tx()
        if tx:
            result, = tx.run("CREATE (a:`Homo Sapiens`) SET a={p} "
                                 "RETURN labels(a), a.`full name`",
                                 p={"full name": "Alice Smith"})
            assert set(result[0]) == {"Homo Sapiens"}
            assert result[1] == "Alice Smith"

    def test_can_use_parameter_for_property_key(self):
        tx = self.new_tx()
        if tx:
            result, = tx.run("CREATE (a:`Homo Sapiens` {«k»:'Alice Smith'}) "
                                 "RETURN labels(a), a.`full name`",
                                 k="full name")
            assert set(result[0]) == {"Homo Sapiens"}
            assert result[1] == "Alice Smith"

    def test_can_use_parameter_for_node_label(self):
        tx = self.new_tx()
        if tx:
            result, = tx.run("CREATE (a:«l» {`full name`:'Alice Smith'}) "
                                 "RETURN labels(a), a.`full name`",
                                 l="Homo Sapiens")
            assert set(result[0]) == {"Homo Sapiens"}
            assert result[1] == "Alice Smith"

    def test_can_use_parameter_for_multiple_node_labels(self):
        tx = self.new_tx()
        if tx:
            result, = tx.run("CREATE (a:«l» {`full name`:'Alice Smith'}) "
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

    def test_can_use_multiple_parameters(self):
        statement = u"CREATE (a:«l» {«k»:{v}})-->(a:«l» {«k»:{v}})"
        parameters = {"l": "Homo Sapiens", "k": "full name", "v": "Alice Smith"}
        s, p = presubstitute(statement, parameters)
        assert s == "CREATE (a:`Homo Sapiens` {`full name`:{v}})-->(a:`Homo Sapiens` {`full name`:{v}})"
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

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


import pytest

from py2neo import Finished
from py2neo.core import Node, Relationship
from py2neo.batch import BatchError, WriteBatch, CypherJob, Batch
from py2neo.ext.mandex.batch import ManualIndexWriteBatch


class TestNodeCreation(object):

    @pytest.fixture(autouse=True)
    def setup(self, graph):
        self.batch = WriteBatch(graph)
        self.graph = graph

    def test_can_create_single_empty_node(self):
        self.batch.create(Node())
        a, = self.batch.submit()
        assert isinstance(a, Node)
        assert a.properties == {}

    def test_can_create_single_node_with_streaming(self):
        self.batch.create(Node(name="Alice"))
        for result in self.batch.stream():
            assert isinstance(result, Node)
            assert result.properties == {"name": "Alice"}

    def test_can_create_multiple_nodes(self):
        self.batch.create({"name": "Alice"})
        self.batch.create(Node.cast({"name": "Bob"}))
        self.batch.create(Node(name="Carol"))
        alice, bob, carol = self.batch.submit()
        assert isinstance(alice, Node)
        assert isinstance(bob, Node)
        assert isinstance(carol, Node)
        assert alice["name"] == "Alice"
        assert bob["name"] == "Bob"
        assert carol["name"] == "Carol"


class TestRelationshipCreation(object):

    @pytest.fixture(autouse=True)
    def setup(self, graph):
        self.batch = ManualIndexWriteBatch(graph)

    def test_can_create_relationship_with_new_nodes(self):
        self.batch.create({"name": "Alice"})
        self.batch.create({"name": "Bob"})
        self.batch.create((0, "KNOWS", 1))
        alice, bob, knows = self.batch.submit()
        assert isinstance(knows, Relationship)
        assert knows.start_node == alice
        assert knows.type == "KNOWS"
        assert knows.end_node == bob
        assert knows.properties == {}
        self.recycling = [knows, alice, bob]

    def test_can_create_relationship_with_existing_nodes(self):
        self.batch.create({"name": "Alice"})
        self.batch.create({"name": "Bob"})
        alice, bob = self.batch.submit()
        self.batch.jobs = []
        self.batch.create((alice, "KNOWS", bob))
        knows, = self.batch.submit()
        assert isinstance(knows, Relationship)
        assert knows.start_node == alice
        assert knows.type == "KNOWS"
        assert knows.end_node == bob
        assert knows.properties == {}
        self.recycling = [knows, alice, bob]

    def test_can_create_relationship_with_existing_start_node(self):
        self.batch.create({"name": "Alice"})
        alice, = self.batch.submit()
        self.batch.jobs = []
        self.batch.create({"name": "Bob"})
        self.batch.create((alice, "KNOWS", 0))
        bob, knows = self.batch.submit()
        assert isinstance(knows, Relationship)
        assert knows.start_node == alice
        assert knows.type == "KNOWS"
        assert knows.end_node == bob
        assert knows.properties == {}
        self.recycling = [knows, alice, bob]

    def test_can_create_relationship_with_existing_end_node(self):
        self.batch.create({"name": "Bob"})
        bob, = self.batch.submit()
        self.batch.jobs = []
        self.batch.create({"name": "Alice"})
        self.batch.create((0, "KNOWS", bob))
        alice, knows = self.batch.submit()
        assert isinstance(knows, Relationship)
        assert knows.start_node == alice
        assert knows.type == "KNOWS"
        assert knows.end_node == bob
        assert knows.properties == {}
        self.recycling = [knows, alice, bob]

    def test_can_create_multiple_relationships(self):
        self.batch.create({"name": "Alice"})
        self.batch.create({"name": "Bob"})
        self.batch.create({"name": "Carol"})
        self.batch.create((0, "KNOWS", 1))
        self.batch.create((1, "KNOWS", 2))
        self.batch.create((2, "KNOWS", 0))
        alice, bob, carol, ab, bc, ca = self.batch.submit()
        for relationship in [ab, bc, ca]:
            assert isinstance(relationship, Relationship)
            assert relationship.type == "KNOWS"

    def test_can_create_overlapping_relationships(self):
        self.batch.create({"name": "Alice"})
        self.batch.create({"name": "Bob"})
        self.batch.create((0, "KNOWS", 1))
        self.batch.create((0, "KNOWS", 1))
        alice, bob, knows1, knows2 = self.batch.submit()
        assert isinstance(knows1, Relationship)
        assert knows1.start_node == alice
        assert knows1.type == "KNOWS"
        assert knows1.end_node == bob
        assert knows1.properties == {}
        assert isinstance(knows2, Relationship)
        assert knows2.start_node == alice
        assert knows2.type == "KNOWS"
        assert knows2.end_node == bob
        assert knows2.properties == {}
        self.recycling = [knows1, knows2, alice, bob]

    def test_can_create_relationship_with_properties(self):
        self.batch.create({"name": "Alice"})
        self.batch.create({"name": "Bob"})
        self.batch.create((0, "KNOWS", 1, {"since": 2000}))
        alice, bob, knows = self.batch.submit()
        assert isinstance(knows, Relationship)
        assert knows.start_node == alice
        assert knows.type == "KNOWS"
        assert knows.end_node == bob
        assert knows["since"] == 2000
        self.recycling = [knows, alice, bob]

    def test_create_function(self):
        self.batch.create(Node(name="Alice"))
        self.batch.create(Node(name="Bob"))
        self.batch.create(Relationship(0, "KNOWS", 1))
        alice, bob, ab = self.batch.submit()
        assert isinstance(alice, Node)
        assert alice["name"] == "Alice"
        assert isinstance(bob, Node)
        assert bob["name"] == "Bob"
        assert isinstance(ab, Relationship)
        assert ab.start_node == alice
        assert ab.type == "KNOWS"
        assert ab.end_node == bob
        self.recycling = [ab, alice, bob]


class TestUniqueRelationshipCreation(object):
    @pytest.fixture(autouse=True)
    def setup(self, graph):
        self.batch = WriteBatch(graph)

    def test_can_create_relationship_if_none_exists(self):
        self.batch.create({"name": "Alice"})
        self.batch.create({"name": "Bob"})
        alice, bob = self.batch.submit()
        self.batch.jobs = []
        self.batch.get_or_create_path(
            alice, ("KNOWS", {"since": 2000}), bob)
        path, = self.batch.submit()
        knows = path.relationships[0]
        assert isinstance(knows, Relationship)
        assert knows.start_node == alice
        assert knows.type == "KNOWS"
        assert knows.end_node == bob
        assert knows["since"] == 2000
        self.recycling = [knows, alice, bob]

    def test_will_get_relationship_if_one_exists(self):
        self.batch.create({"name": "Alice"})
        self.batch.create({"name": "Bob"})
        alice, bob = self.batch.submit()
        self.batch.jobs = []
        self.batch.get_or_create_path(
            alice, ("KNOWS", {"since": 2000}), bob)
        self.batch.get_or_create_path(
            alice, ("KNOWS", {"since": 2000}), bob)
        path1, path2 = self.batch.submit()
        assert path1 == path2

    def test_will_fail_batch_if_more_than_one_exists(self):
        self.batch.create({"name": "Alice"})
        self.batch.create({"name": "Bob"})
        self.batch.create((0, "KNOWS", 1))
        self.batch.create((0, "KNOWS", 1))
        alice, bob, k1, k2 = self.batch.submit()
        self.batch.jobs = []
        self.batch.get_or_create_path(alice, "KNOWS", bob)
        try:
            self.batch.submit()
        except BatchError as error:
            cause = error.__cause__
            assert cause.__class__.__name__ == "UniquePathNotUniqueException"
        else:
            assert False

    def test_can_create_relationship_and_start_node(self):
        self.batch.create({"name": "Bob"})
        bob, = self.batch.submit()
        self.batch.jobs = []
        self.batch.get_or_create_path(None, "KNOWS", bob)
        path, = self.batch.submit()
        knows = path.relationships[0]
        alice = knows.start_node
        assert isinstance(knows, Relationship)
        assert isinstance(alice, Node)
        assert knows.type == "KNOWS"
        assert knows.end_node == bob
        self.recycling = [knows, alice, bob]

    def test_can_create_relationship_and_end_node(self):
        self.batch.create({"name": "Alice"})
        alice, = self.batch.submit()
        self.batch.jobs = []
        self.batch.get_or_create_path(alice, "KNOWS", None)
        path, = self.batch.submit()
        knows = path.relationships[0]
        bob = knows.end_node
        assert isinstance(knows, Relationship)
        assert knows.start_node == alice
        assert knows.type == "KNOWS"
        assert isinstance(bob, Node)
        self.recycling = [knows, alice, bob]


class TestDeletion(object):
    @pytest.fixture(autouse=True)
    def setup(self, graph):
        self.batch = WriteBatch(graph)
        self.graph = graph

    def test_can_delete_relationship_and_related_nodes(self):
        self.batch.create({"name": "Alice"})
        self.batch.create({"name": "Bob"})
        self.batch.create((0, "KNOWS", 1))
        alice, bob, ab = self.batch.submit()
        assert alice.exists
        assert bob.exists
        assert ab.exists
        self.batch.jobs = []
        self.batch.delete(ab)
        self.batch.delete(alice)
        self.batch.delete(bob)
        self.batch.run()
        assert not alice.exists
        assert not bob.exists
        assert not ab.exists


class TestPropertyManagement(object):
    @pytest.fixture(autouse=True)
    def setup(self, graph):
        self.batch = WriteBatch(graph)
        self.alice, self.bob, self.friends = graph.create(
            {"name": "Alice", "surname": "Allison"},
            {"name": "Bob", "surname": "Robertson"},
            (0, "KNOWS", 1, {"since": 2000}),
        )
        self.graph = graph

    def _check_properties(self, entity, expected_properties):
        entity.pull()
        actual_properties = entity.properties
        assert len(actual_properties) == len(expected_properties)
        for key, value in expected_properties.items():
            assert key in actual_properties
            assert str(actual_properties[key]) == str(value)

    def test_can_add_new_node_property(self):
        self.batch.set_property(self.alice, "age", 33)
        self.batch.run()
        self._check_properties(self.alice, {"name": "Alice", "surname": "Allison", "age": 33})

    def test_can_overwrite_existing_node_property(self):
        self.batch.set_property(self.alice, "name", "Alison")
        self.batch.run()
        self._check_properties(self.alice, {"name": "Alison", "surname": "Allison"})

    def test_can_replace_all_node_properties(self):
        props = {"full_name": "Alice Allison", "age": 33}
        self.batch.set_properties(self.alice, props)
        self.batch.run()
        self._check_properties(self.alice, props)

    def test_can_add_delete_node_property(self):
        self.batch.delete_property(self.alice, "surname")
        self.batch.run()
        self._check_properties(self.alice, {"name": "Alice"})

    def test_can_add_delete_all_node_properties(self):
        self.batch.delete_properties(self.alice)
        self.batch.run()
        self._check_properties(self.alice, {})

    def test_can_add_new_relationship_property(self):
        self.batch.set_property(self.friends, "foo", "bar")
        self.batch.run()
        self._check_properties(self.friends, {"since": 2000, "foo": "bar"})


def test_can_use_return_values_as_references(graph):
    batch = WriteBatch(graph)
    a = batch.create(Node(name="Alice"))
    b = batch.create(Node(name="Bob"))
    batch.create(Relationship(a, "KNOWS", b))
    results = batch.submit()
    ab = results[2]
    assert isinstance(ab, Relationship)
    assert ab.start_node["name"] == "Alice"
    assert ab.end_node["name"] == "Bob"


def test_can_handle_json_response_with_no_content(graph):
    # This example might fail if the server bug is fixed that returns
    # a 200 response with application/json content-type and no content.
    batch = WriteBatch(graph)
    batch.create((0, "KNOWS", 1))
    results = batch.submit()
    assert results == []


def test_cypher_job_with_bad_syntax(graph):
    batch = WriteBatch(graph)
    batch.append(CypherJob("X"))
    try:
        batch.submit()
    except BatchError as error:
        assert error.batch is batch
        assert error.job_id == 0
        assert error.status_code == 400
        assert error.uri == "cypher"
    else:
        assert False


def test_cypher_job_with_other_error(graph):
    batch = WriteBatch(graph)
    statement = "MATCH (n) RETURN n LIMIT -1"
    batch.append(CypherJob(statement))
    try:
        batch.submit()
    except BatchError as error:
        assert error.batch is batch
        assert error.job_id == 0
        assert error.status_code == 400
        assert error.uri == "cypher"
    else:
        assert False


def test_cannot_resubmit_finished_job(graph):
    batch = Batch(graph)
    batch.append(CypherJob("CREATE (a)"))
    graph.batch.submit(batch)
    try:
        graph.batch.submit(batch)
    except Finished:
        assert True
    else:
        assert False

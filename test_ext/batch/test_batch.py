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


from py2neo import Node, Relationship, Finished, node, relationship
from py2neo.ext.batch import BatchRunner, WriteBatch, CypherJob, \
    BatchError, Job, Target, NodePointer
from py2neo.status.statement import InvalidSyntax, ConstraintViolation
from py2neo.ext.mandex import ManualIndexWriteBatch
from test.util import Py2neoTestCase


class BatchTestCase(Py2neoTestCase):
    def __init__(self, *args, **kwargs):
        super(BatchTestCase, self).__init__(*args, **kwargs)
        self.runner = BatchRunner(self.graph)


class NodeCreationTestCase(Py2neoTestCase):

    def setUp(self):
        self.batch = ManualIndexWriteBatch(self.graph)

    def test_can_create_single_empty_node(self):
        self.batch.create(Node())
        a, = self.batch.run()
        assert isinstance(a, Node)
        assert not a

    def test_can_create_multiple_nodes(self):
        self.batch.create({"name": "Alice"})
        self.batch.create(node({"name": "Bob"}))
        self.batch.create(Node(name="Carol"))
        alice, bob, carol = self.batch.run()
        assert isinstance(alice, Node)
        assert isinstance(bob, Node)
        assert isinstance(carol, Node)
        assert alice["name"] == "Alice"
        assert bob["name"] == "Bob"
        assert carol["name"] == "Carol"


class RelationshipCreationTestCase(Py2neoTestCase):

    def setUp(self):
        self.batch = ManualIndexWriteBatch(self.graph)

    def test_can_create_relationship_with_new_nodes(self):
        self.batch.create({"name": "Alice"})
        self.batch.create({"name": "Bob"})
        self.batch.create((0, "KNOWS", 1))
        alice, bob, knows = self.batch.run()
        assert isinstance(knows, Relationship)
        assert knows.start_node() == alice
        assert knows.type() == "KNOWS"
        assert knows.end_node() == bob
        assert dict(knows) == {}
        self.recycling = [knows, alice, bob]

    def test_can_create_relationship_with_existing_nodes(self):
        self.batch.create({"name": "Alice"})
        self.batch.create({"name": "Bob"})
        alice, bob = self.batch.run()
        self.batch.jobs = []
        self.batch.create((alice, "KNOWS", bob))
        knows, = self.batch.run()
        assert isinstance(knows, Relationship)
        assert knows.start_node() == alice
        assert knows.type() == "KNOWS"
        assert knows.end_node() == bob
        assert dict(knows) == {}
        self.recycling = [knows, alice, bob]

    def test_can_create_relationship_with_existing_start_node(self):
        self.batch.create({"name": "Alice"})
        alice, = self.batch.run()
        self.batch.jobs = []
        self.batch.create({"name": "Bob"})
        self.batch.create((alice, "KNOWS", 0))
        bob, knows = self.batch.run()
        assert isinstance(knows, Relationship)
        assert knows.start_node() == alice
        assert knows.type() == "KNOWS"
        assert knows.end_node() == bob
        assert dict(knows) == {}
        self.recycling = [knows, alice, bob]

    def test_can_create_relationship_with_existing_end_node(self):
        self.batch.create({"name": "Bob"})
        bob, = self.batch.run()
        self.batch.jobs = []
        self.batch.create({"name": "Alice"})
        self.batch.create((0, "KNOWS", bob))
        alice, knows = self.batch.run()
        assert isinstance(knows, Relationship)
        assert knows.start_node() == alice
        assert knows.type() == "KNOWS"
        assert knows.end_node() == bob
        assert dict(knows) == {}
        self.recycling = [knows, alice, bob]

    def test_can_create_multiple_relationships(self):
        self.batch.create({"name": "Alice"})
        self.batch.create({"name": "Bob"})
        self.batch.create({"name": "Carol"})
        self.batch.create((0, "KNOWS", 1))
        self.batch.create((1, "KNOWS", 2))
        self.batch.create((2, "KNOWS", 0))
        alice, bob, carol, ab, bc, ca = self.batch.run()
        for relationship in [ab, bc, ca]:
            assert isinstance(relationship, Relationship)
            assert relationship.type() == "KNOWS"

    def test_can_create_overlapping_relationships(self):
        self.batch.create({"name": "Alice"})
        self.batch.create({"name": "Bob"})
        self.batch.create((0, "KNOWS", 1))
        self.batch.create((0, "KNOWS", 1))
        alice, bob, knows1, knows2 = self.batch.run()
        assert isinstance(knows1, Relationship)
        assert knows1.start_node() == alice
        assert knows1.type() == "KNOWS"
        assert knows1.end_node() == bob
        assert dict(knows1) == {}
        assert isinstance(knows2, Relationship)
        assert knows2.start_node() == alice
        assert knows2.type() == "KNOWS"
        assert knows2.end_node() == bob
        assert dict(knows2) == {}
        self.recycling = [knows1, knows2, alice, bob]

    def test_can_create_relationship_with_properties(self):
        self.batch.create({"name": "Alice"})
        self.batch.create({"name": "Bob"})
        self.batch.create((0, "KNOWS", 1, {"since": 2000}))
        alice, bob, knows = self.batch.run()
        assert isinstance(knows, Relationship)
        assert knows.start_node() == alice
        assert knows.type() == "KNOWS"
        assert knows.end_node() == bob
        assert knows["since"] == 2000
        self.recycling = [knows, alice, bob]

    def test_create_function(self):
        self.batch.create(Node(name="Alice"))
        self.batch.create(Node(name="Bob"))
        self.batch.create((0, "KNOWS", 1))
        alice, bob, ab = self.batch.run()
        assert isinstance(alice, Node)
        assert alice["name"] == "Alice"
        assert isinstance(bob, Node)
        assert bob["name"] == "Bob"
        assert isinstance(ab, Relationship)
        assert ab.start_node() == alice
        assert ab.type() == "KNOWS"
        assert ab.end_node() == bob
        self.recycling = [ab, alice, bob]


class UniqueRelationshipCreationRestCase(Py2neoTestCase):

    def setUp(self):
        self.batch = ManualIndexWriteBatch(self.graph)

    def test_can_create_relationship_if_none_exists(self):
        self.batch.create({"name": "Alice"})
        self.batch.create({"name": "Bob"})
        alice, bob = self.batch.run()
        self.batch.jobs = []
        self.batch.get_or_create_path(
            alice, ("KNOWS", {"since": 2000}), bob)
        path, = self.batch.run()
        knows = path[0]
        assert isinstance(knows, Relationship)
        assert knows.start_node() == alice
        assert knows.type() == "KNOWS"
        assert knows.end_node() == bob
        assert knows["since"] == 2000
        self.recycling = [knows, alice, bob]

    def test_will_get_relationship_if_one_exists(self):
        self.batch.create({"name": "Alice"})
        self.batch.create({"name": "Bob"})
        alice, bob = self.batch.run()
        self.batch.jobs = []
        self.batch.get_or_create_path(
            alice, ("KNOWS", {"since": 2000}), bob)
        self.batch.get_or_create_path(
            alice, ("KNOWS", {"since": 2000}), bob)
        path1, path2 = self.batch.run()
        assert path1 == path2

    def test_will_fail_batch_if_more_than_one_exists(self):
        self.batch.create({"name": "Alice"})
        self.batch.create({"name": "Bob"})
        self.batch.create((0, "KNOWS", 1))
        self.batch.create((0, "KNOWS", 1))
        alice, bob, k1, k2 = self.batch.run()
        self.batch.jobs = []
        self.batch.get_or_create_path(alice, "KNOWS", bob)
        try:
            self.batch.run()
        except BatchError as error:
            assert isinstance(error.__cause__, ConstraintViolation)
        else:
            assert False

    def test_can_create_relationship_and_start_node(self):
        self.batch.create({"name": "Bob"})
        bob, = self.batch.run()
        self.batch.jobs = []
        self.batch.get_or_create_path(None, "KNOWS", bob)
        path, = self.batch.run()
        knows = path[0]
        alice = knows.start_node()
        assert isinstance(knows, Relationship)
        assert isinstance(alice, Node)
        assert knows.type() == "KNOWS"
        assert knows.end_node() == bob
        self.recycling = [knows, alice, bob]

    def test_can_create_relationship_and_end_node(self):
        self.batch.create({"name": "Alice"})
        alice, = self.batch.run()
        self.batch.jobs = []
        self.batch.get_or_create_path(alice, "KNOWS", None)
        path, = self.batch.run()
        knows = path[0]
        bob = knows.end_node()
        assert isinstance(knows, Relationship)
        assert knows.start_node() == alice
        assert knows.type() == "KNOWS"
        assert isinstance(bob, Node)
        self.recycling = [knows, alice, bob]


class DeletionTestCase(Py2neoTestCase):

    def setUp(self):
        self.batch = ManualIndexWriteBatch(self.graph)

    def test_can_delete_relationship_and_related_nodes(self):
        self.batch.create({"name": "Alice"})
        self.batch.create({"name": "Bob"})
        self.batch.create((0, "KNOWS", 1))
        alice, bob, ab = self.batch.run()
        assert self.graph.exists(alice, bob, ab)
        self.batch.jobs = []
        self.batch.delete(ab)
        self.batch.delete(alice)
        self.batch.delete(bob)
        self.batch.run()
        assert not self.graph.exists(alice, bob, ab)


class PropertyManagementTestCase(Py2neoTestCase):

    def setUp(self):
        self.batch = ManualIndexWriteBatch(self.graph)
        self.alice = node({"name": "Alice", "surname": "Allison"})
        self.bob = node({"name": "Bob", "surname": "Robertson"})
        self.friends = relationship((self.alice, "KNOWS", self.bob, {"since": 2000}))
        self.graph.create(self.alice | self.bob | self.friends)

    def _check_properties(self, entity, expected_properties):
        self.graph.pull(entity)
        actual_properties = dict(entity)
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

    def test_can_delete_node_property(self):
        self.batch.delete_property(self.alice, "surname")
        self.batch.run()
        self._check_properties(self.alice, {"name": "Alice"})

    def test_can_delete_all_node_properties(self):
        self.batch.delete_properties(self.alice)
        r = self.batch.run()
        self._check_properties(self.alice, {})


class MiscellaneousTestCase(Py2neoTestCase):

    def setUp(self):
        self.batch = ManualIndexWriteBatch(self.graph)
        self.runner = self.batch.runner

    def test_can_use_return_values_as_references(self):
        a = self.batch.create(Node(name="Alice"))
        b = self.batch.create(Node(name="Bob"))
        self.batch.create(Relationship(a, "KNOWS", b))
        results = self.batch.run()
        ab = results[2]
        assert isinstance(ab, Relationship)
        assert ab.start_node()["name"] == "Alice"
        assert ab.end_node()["name"] == "Bob"

    def test_can_handle_json_response_with_no_content(self):
        # This example might fail if the server bug is fixed that returns
        # a 200 response with application/json content-type and no content.
        self.batch.create((0, "KNOWS", 1))
        results = self.batch.run()
        assert results == []
    
    def test_cypher_job_with_invalid_syntax(self):
        self.batch.append(CypherJob("X"))
        try:
            self.batch.run()
        except BatchError as error:
            assert error.batch is self.batch
            assert error.job_id == 0
            assert isinstance(error.__cause__, InvalidSyntax)
        else:
            assert False

    def test_cannot_resubmit_finished_job(self):
        self.batch.append(CypherJob("CREATE (a)"))
        self.runner.run(self.batch)
        with self.assertRaises(Finished):
            self.runner.run(self.batch)


class BatchRequestTestCase(Py2neoTestCase):

    def test_can_create_batch_request(self):
        method = "POST"
        endpoint = "cypher"
        target = Target(endpoint)
        body = {"query": "CREATE (a) RETURN a"}
        request = Job(method, target, body)
        assert request.method == method
        assert request.target.uri_string == endpoint
        assert request.body == body

    def test_batch_requests_are_equal_if_same(self):
        method = "POST"
        endpoint = "cypher"
        target = Target(endpoint)
        body = {"query": "CREATE (a) RETURN a"}
        request_1 = Job(method, target, body)
        request_2 = request_1
        assert request_1 == request_2
        assert hash(request_1) == hash(request_2)

    def test_batch_requests_are_unequal_if_not_same(self):
        method = "POST"
        endpoint = "cypher"
        target = Target(endpoint)
        body = {"query": "CREATE (a) RETURN a"}
        request_1 = Job(method, target, body)
        request_2 = Job(method, target, body)
        assert request_1 != request_2
        assert hash(request_1) != hash(request_2)


class WriteBatchTestCase(Py2neoTestCase):

    def setUp(self):
        self.batch = WriteBatch(self.graph)

    def test_cannot_create_with_bad_type(self):
        try:
            self.batch.create("")
        except TypeError:
            assert True
        else:
            assert False

    def test_cannot_create_with_none(self):
        try:
            self.batch.create(None)
        except TypeError:
            assert True
        else:
            assert False

    def test_can_create_path_with_new_nodes(self):
        self.batch.create_path({"name": "Alice"}, "KNOWS", {"name": "Bob"})
        results = self.batch.run()
        path = results[0]
        assert len(path) == 1
        assert path.nodes()[0]["name"] == "Alice"
        assert path[0].type() == "KNOWS"
        assert path.nodes()[1]["name"] == "Bob"

    def test_can_create_path_with_existing_nodes(self):
        alice = node({"name": "Alice"})
        bob = node({"name": "Bob"})
        self.graph.create(alice | bob)
        self.batch.create_path(alice, "KNOWS", bob)
        results = self.batch.run()
        path = results[0]
        assert len(path) == 1
        assert path.nodes()[0] == alice
        assert path[0].type() == "KNOWS"
        assert path.nodes()[1] == bob

    def test_path_creation_is_not_idempotent(self):
        alice = Node(name="Alice")
        self.graph.create(alice)
        self.batch.create_path(alice, "KNOWS", {"name": "Bob"})
        results = self.batch.run()
        path = results[0]
        bob = path.nodes()[1]
        assert path.nodes()[0] == alice
        assert bob["name"] == "Bob"
        self.batch = WriteBatch(self.graph)
        self.batch.create_path(alice, "KNOWS", {"name": "Bob"})
        results = self.batch.run()
        path = results[0]
        assert path.nodes()[0] == alice
        assert path.nodes()[1] != bob

    def test_can_get_or_create_path_with_existing_nodes(self):
        alice = Node(name="Alice")
        bob = Node(name="Bob")
        self.graph.create(alice | bob)
        self.batch.get_or_create_path(alice, "KNOWS", bob)
        results = self.batch.run()
        path = results[0]
        assert len(path) == 1
        assert path.nodes()[0] == alice
        assert path[0].type() == "KNOWS"
        assert path.nodes()[1] == bob

    def test_path_merging_is_idempotent(self):
        alice = Node(name="Alice")
        self.graph.create(alice)
        self.batch.get_or_create_path(alice, "KNOWS", {"name": "Bob"})
        results = self.batch.run()
        path = results[0]
        bob = path.nodes()[1]
        assert path.nodes()[0] == alice
        assert bob["name"] == "Bob"
        self.batch = WriteBatch(self.graph)
        self.batch.get_or_create_path(alice, "KNOWS", {"name": "Bob"})
        results = self.batch.run()
        path = results[0]
        assert path.nodes()[0] == alice
        assert path.nodes()[1] == bob

    def test_can_set_property_on_preexisting_node(self):
        alice = Node(name="Alice")
        self.graph.create(alice)
        self.batch.set_property(alice, "age", 34)
        self.batch.run()
        self.graph.pull(alice)
        assert alice["age"] == 34

    def test_can_set_property_on_node_in_same_batch(self):
        alice = self.batch.create({"name": "Alice"})
        self.batch.set_property(alice, "age", 34)
        results = self.batch.run()
        alice = results[self.batch.find(alice)]
        self.graph.pull(alice)
        assert alice["age"] == 34

    def test_can_set_properties_on_preexisting_node(self):
        alice = Node()
        self.graph.create(alice)
        self.batch.set_properties(alice, {"name": "Alice", "age": 34})
        self.batch.run()
        self.graph.pull(alice)
        assert alice["name"] == "Alice"
        assert alice["age"] == 34

    def test_can_set_properties_on_node_in_same_batch(self):
        alice = self.batch.create({})
        self.batch.set_properties(alice, {"name": "Alice", "age": 34})
        results = self.batch.run()
        alice = results[self.batch.find(alice)]
        self.graph.pull(alice)
        assert alice["name"] == "Alice"
        assert alice["age"] == 34

    def test_can_delete_property_on_preexisting_node(self):
        alice = node({"name": "Alice", "age": 34})
        self.graph.create(alice)
        self.batch.delete_property(alice, "age")
        self.batch.run()
        self.graph.pull(alice)
        assert alice["name"] == "Alice"
        assert alice["age"] is None

    def test_can_delete_property_on_node_in_same_batch(self):
        alice = self.batch.create({"name": "Alice", "age": 34})
        self.batch.delete_property(alice, "age")
        results = self.batch.run()
        alice = results[self.batch.find(alice)]
        self.graph.pull(alice)
        assert alice["name"] == "Alice"
        assert alice["age"] is None

    def test_can_delete_properties_on_preexisting_node(self):
        alice = node({"name": "Alice", "age": 34})
        self.graph.create(alice)
        self.batch.delete_properties(alice)
        self.batch.run()
        self.graph.pull(alice)
        assert not alice

    def test_can_delete_properties_on_node_in_same_batch(self):
        alice = self.batch.create({"name": "Alice", "age": 34})
        self.batch.delete_properties(alice)
        results = self.batch.run()
        alice = results[self.batch.find(alice)]
        self.graph.pull(alice)
        assert not alice

    def test_can_add_labels_to_preexisting_node(self):
        alice = Node(name="Alice")
        self.graph.create(alice)
        self.batch.add_labels(alice, "human", "female")
        self.batch.run()
        self.graph.pull(alice)
        assert alice.labels() == {"human", "female"}

    def test_can_add_labels_to_node_in_same_batch(self):
        a = self.batch.create({"name": "Alice"})
        self.batch.add_labels(a, "human", "female")
        results = self.batch.run()
        alice = results[self.batch.find(a)]
        self.graph.pull(alice)
        assert alice.labels() == {"human", "female"}

    def test_can_remove_labels_from_preexisting_node(self):
        alice = Node("human", "female", name="Alice")
        self.graph.create(alice)
        self.batch.remove_label(alice, "human")
        self.batch.run()
        self.graph.pull(alice)
        assert alice.labels() == {"female"}

    def test_can_add_and_remove_labels_on_node_in_same_batch(self):
        alice = self.batch.create({"name": "Alice"})
        self.batch.add_labels(alice, "human", "female")
        self.batch.remove_label(alice, "female")
        results = self.batch.run()
        alice = results[self.batch.find(alice)]
        self.graph.pull(alice)
        assert alice.labels() == {"human"}

    def test_can_set_labels_on_preexisting_node(self):
        alice = Node("human", "female", name="Alice")
        self.graph.create(alice)
        self.batch.set_labels(alice, "mystery", "badger")
        self.batch.run()
        self.graph.pull(alice)
        assert alice.labels() == {"mystery", "badger"}

    def test_can_set_labels_on_node_in_same_batch(self):
        self.batch.create({"name": "Alice"})
        self.batch.add_labels(0, "human", "female")
        self.batch.set_labels(0, "mystery", "badger")
        results = self.batch.run()
        alice = results[0]
        self.graph.pull(alice)
        assert alice.labels() == {"mystery", "badger"}


class NodePointerTestCase(Py2neoTestCase):

    def test_node_pointer_equality(self):
        p1 = NodePointer(42)
        p2 = NodePointer(42)
        assert p1 == p2

    def test_node_pointer_inequality(self):
        p1 = NodePointer(42)
        p2 = NodePointer(69)
        assert p1 != p2

    def test_node_pointer_hashes(self):
        assert hash(NodePointer(42)) == hash(NodePointer(42))

    def test_node_pointer_str(self):
        pointer = NodePointer(3456)
        assert str(pointer) == "{3456}"

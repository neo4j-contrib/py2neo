#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright 2011-2019, Nigel Small
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


from unittest import TestCase

from py2neo.storage import FrozenGraphStore, MutableGraphStore


class GraphStoreTestCase(TestCase):

    store = MutableGraphStore()
    a, b, c, d = store.add_nodes((
        (["X"], {"name": "Alice"}),
        (["X", "Y"], {"name": "Bob"}),
        (["X", "Y"], {"name": "Carol"}),
        (["Y"], {"name": "Dave"}),
    ))
    (a_likes_b, b_likes_a, a_knows_b, a_knows_c,
     c_knows_b, c_married_to_d) = store.add_relationships((
        ("LIKES", (a, b), {}),
        ("LIKES", (b, a), {}),
        ("KNOWS", (a, b), {"since": 1999}),
        ("KNOWS", (a, c), {"since": 2000}),
        ("KNOWS", (c, b), {"since": 2001}),
        ("MARRIED_TO", (c, d), {}),
        )
    )

    def test_should_get_counts(self):
        store = FrozenGraphStore(self.store)
        self.assertEqual(store.node_count(), 4)
        self.assertEqual(store.node_count("X"), 3)
        self.assertEqual(store.relationship_count(), 6)
        self.assertEqual(store.relationship_count("KNOWS"), 3)
        self.assertEqual(store.node_labels(), {"X", "Y"})
        self.assertEqual(store.relationship_types(), {"LIKES", "KNOWS", "MARRIED_TO"})

    def test_should_get_node_degree(self):
        store = FrozenGraphStore(self.store)
        self.assertEqual(store.relationship_count(n_ids={self.a}), 4)
        self.assertEqual(store.relationship_count(r_type="LIKES", n_ids={self.a}), 2)
        self.assertEqual(store.relationship_count(n_ids={self.b}), 4)
        self.assertEqual(store.relationship_count(n_ids={self.c}), 3)
        self.assertEqual(store.relationship_count(n_ids={self.d}), 1)

    def test_should_get_nodes(self):
        store = FrozenGraphStore(self.store)
        self.assertEqual(set(store.nodes()), {self.a, self.b, self.c, self.d})

    def test_should_get_nodes_with_a_label(self):
        store = FrozenGraphStore(self.store)
        self.assertEqual(set(store.nodes("X")), {self.a, self.b, self.c})
        self.assertEqual(set(store.nodes("Y")), {self.b, self.c, self.d})
        self.assertFalse(set(store.nodes("Z")))

    def test_should_get_nodes_with_multiple_labels(self):
        store = FrozenGraphStore(self.store)
        self.assertEqual(set(store.nodes("X", "Y")), {self.b, self.c})
        self.assertFalse(set(store.nodes("X", "Z")))

    def test_should_get_node_labels(self):
        store = FrozenGraphStore(self.store)
        self.assertEqual(store.node_labels(), {"X", "Y"})
        self.assertEqual(store.node_labels(self.a), {"X"})
        self.assertEqual(store.node_labels(self.b), {"X", "Y"})
        self.assertEqual(store.node_labels(self.c), {"X", "Y"})
        self.assertEqual(store.node_labels(self.d), {"Y"})
        self.assertIs(store.node_labels(object()), None)

    def test_should_get_node_properties(self):
        store = FrozenGraphStore(self.store)
        self.assertEqual(store.node_properties(self.a), {"name": "Alice"})
        self.assertEqual(store.node_properties(self.b), {"name": "Bob"})
        self.assertEqual(store.node_properties(self.c), {"name": "Carol"})
        self.assertEqual(store.node_properties(self.d), {"name": "Dave"})
        self.assertIs(store.node_properties(object()), None)

    def test_should_get_relationships(self):
        store = FrozenGraphStore(self.store)
        self.assertEqual(set(store.relationships()), {self.a_likes_b, self.b_likes_a, self.a_knows_b,
                                                      self.a_knows_c, self.c_knows_b, self.c_married_to_d})
        self.assertEqual(set(store.relationships("KNOWS")), {self.a_knows_b, self.a_knows_c, self.c_knows_b})
        self.assertEqual(set(store.relationships("MARRIED_TO")), {self.c_married_to_d})
        self.assertEqual(set(store.relationships(n_ids=(self.a, None))), {self.a_likes_b, self.a_knows_b,
                                                                          self.a_knows_c})
        self.assertEqual(set(store.relationships("KNOWS", (self.a, None))), {self.a_knows_b, self.a_knows_c})
        self.assertEqual(set(store.relationships(n_ids=(None, self.b))), {self.a_likes_b, self.a_knows_b,
                                                                          self.c_knows_b})
        self.assertEqual(set(store.relationships("KNOWS", n_ids=(None, self.b))), {self.a_knows_b, self.c_knows_b})
        self.assertEqual(set(store.relationships(n_ids=(self.a, self.b))), {self.a_likes_b, self.a_knows_b})
        self.assertEqual(set(store.relationships("KNOWS", (self.a, self.b))), {self.a_knows_b})
        self.assertEqual(set(store.relationships(n_ids={self.a})), {self.a_likes_b, self.b_likes_a,
                                                                    self.a_knows_b, self.a_knows_c})
        self.assertEqual(set(store.relationships("KNOWS", {self.a})), {self.a_knows_b, self.a_knows_c})
        self.assertEqual(set(store.relationships(n_ids={self.a, self.b})), {self.a_likes_b, self.b_likes_a,
                                                                            self.a_knows_b})
        self.assertEqual(set(store.relationships("KNOWS", n_ids={self.a, self.b})), {self.a_knows_b})

    def test_should_fail_on_bad_node_sequence(self):
        store = FrozenGraphStore(self.store)
        self.assertEqual(list(store.relationships(n_ids=(self.a, self.b, self.c))), [])

    def test_should_fail_on_bad_node_set(self):
        store = FrozenGraphStore(self.store)
        _ = store.relationships(n_ids={self.a, self.b, self.c})

    def test_should_fail_on_bad_node_type(self):
        store = FrozenGraphStore(self.store)
        with self.assertRaises(TypeError):
            _ = store.relationships(n_ids=1)

    def test_should_get_relationship_nodes(self):
        store = FrozenGraphStore(self.store)
        self.assertEqual(store.relationship_nodes(self.a_likes_b), (self.a, self.b))
        self.assertEqual(store.relationship_nodes(self.b_likes_a), (self.b, self.a))
        self.assertEqual(store.relationship_nodes(self.a_knows_b), (self.a, self.b))
        self.assertEqual(store.relationship_nodes(self.a_knows_c), (self.a, self.c))
        self.assertEqual(store.relationship_nodes(self.c_knows_b), (self.c, self.b))
        self.assertEqual(store.relationship_nodes(self.c_married_to_d), (self.c, self.d))
        self.assertIs(store.relationship_nodes(object()), None)

    def test_should_get_relationship_properties(self):
        store = FrozenGraphStore(self.store)
        self.assertEqual(store.relationship_properties(self.a_knows_b), {"since": 1999})
        self.assertEqual(store.relationship_properties(self.a_knows_c), {"since": 2000})
        self.assertEqual(store.relationship_properties(self.c_knows_b), {"since": 2001})
        self.assertIs(store.relationship_properties(object()), None)

    def test_should_get_relationship_type(self):
        store = FrozenGraphStore(self.store)
        self.assertEqual(store.relationship_type(self.a_likes_b), "LIKES")
        self.assertEqual(store.relationship_type(self.b_likes_a), "LIKES")
        self.assertEqual(store.relationship_type(self.a_knows_b), "KNOWS")
        self.assertEqual(store.relationship_type(self.a_knows_c), "KNOWS")
        self.assertEqual(store.relationship_type(self.c_knows_b), "KNOWS")
        self.assertEqual(store.relationship_type(self.c_married_to_d), "MARRIED_TO")
        self.assertIs(store.relationship_type(object()), None)


class FrozenGraphStoreTestCase(TestCase):

    store = MutableGraphStore()
    a, b, c, d = store.add_nodes((
        (["X"], {"name": "Alice"}),
        (["X", "Y"], {"name": "Bob"}),
        (["X", "Y"], {"name": "Carol"}),
        (["Y"], {"name": "Dave"}),
    ))
    store.add_relationships((
        ("KNOWS", (a, b), {}),
        ("KNOWS", (a, c), {}),
        ("KNOWS", (b, c), {}),
        ("KNOWS", (c, d), {}),
    ))
    store = FrozenGraphStore(store)

    def test_should_create_empty_on_none(self):
        store = FrozenGraphStore()
        self.assertEqual(store.node_count(), 0)
        self.assertEqual(store.relationship_count(), 0)
        self.assertFalse(store.node_labels())
        self.assertFalse(store.relationship_types())

    def test_should_not_create_from_non_store(self):
        with self.assertRaises(TypeError):
            _ = FrozenGraphStore(object())

    def test_should_create_copy_of_frozen_store(self):
        store = FrozenGraphStore(FrozenGraphStore(self.store))
        self.assertEqual(store.node_count(), 4)
        self.assertEqual(store.relationship_count(), 4)
        self.assertEqual(store.node_labels(), {"X", "Y"})
        self.assertEqual(store.relationship_types(), {"KNOWS"})

    def test_should_create_copy_of_mutable_store(self):
        store = FrozenGraphStore(self.store)
        self.assertEqual(store.node_count(), 4)
        self.assertEqual(store.relationship_count(), 4)
        self.assertEqual(store.node_labels(), {"X", "Y"})
        self.assertEqual(store.relationship_types(), {"KNOWS"})

    def test_should_allow_construction_arguments(self):
        store = FrozenGraphStore.build({
            "a": (["Person"], {"name": "Alice", "age": 33}),
            "b": (["Person"], {"name": "Bob", "age": 44}),
        }, {
            "ab": ("KNOWS", ("a", "b"), {"since": 1999}),
        })
        self.assertIsInstance(store, FrozenGraphStore)
        self.assertEqual(store.node_count(), 2)
        self.assertEqual(store.relationship_count(), 1)
        self.assertEqual(store.node_labels(), {"Person"})
        self.assertEqual(store.relationship_types(), {"KNOWS"})
        self.assertEqual(set(store.nodes("Person")), {"a", "b"})
        self.assertEqual(store.node_labels("a"), {"Person"})
        self.assertEqual(store.node_labels("b"), {"Person"})
        self.assertEqual(store.node_properties("a"), {"name": "Alice", "age": 33})
        self.assertEqual(store.node_properties("b"), {"name": "Bob", "age": 44})
        self.assertEqual(set(store.relationships("KNOWS")), {"ab"})
        self.assertEqual(store.relationship_type("ab"), "KNOWS")
        self.assertEqual(store.relationship_properties("ab"), {"since": 1999})


class MutableGraphStoreTestCase(TestCase):

    store = MutableGraphStore()
    a, b, c, d = store.add_nodes((
        (["X"], {"name": "Alice"}),
        (["X", "Y"], {"name": "Bob"}),
        (["X", "Y"], {"name": "Carol"}),
        (["Y"], {"name": "Dave"}),
    ))
    store.add_relationships((
        ("KNOWS", (a, b), {}),
        ("KNOWS", (a, c), {}),
        ("KNOWS", (b, c), {}),
        ("KNOWS", (c, d), {}),
    ))

    def test_should_create_empty_on_none(self):
        store = MutableGraphStore()
        self.assertEqual(store.node_count(), 0)
        self.assertEqual(store.relationship_count(), 0)
        self.assertFalse(store.node_labels())
        self.assertFalse(store.relationship_types())

    def test_should_create_copy_of_frozen_store(self):
        store = MutableGraphStore(FrozenGraphStore(self.store))
        self.assertEqual(store.node_count(), 4)
        self.assertEqual(store.relationship_count(), 4)
        self.assertEqual(store.node_labels(), {"X", "Y"})
        self.assertEqual(store.relationship_types(), {"KNOWS"})

    def test_should_create_copy_of_mutable_store(self):
        store = MutableGraphStore(self.store)
        self.assertEqual(store.node_count(), 4)
        self.assertEqual(store.relationship_count(), 4)
        self.assertEqual(store.node_labels(), {"X", "Y"})
        self.assertEqual(store.relationship_types(), {"KNOWS"})

    def test_can_add_new_label(self):
        store = MutableGraphStore(self.store)
        labels = store.node_labels(self.a)
        self.assertEqual(labels, {"X"})
        labels.add("Z")
        self.assertEqual(store.node_labels(self.a), {"X", "Z"})
        assert "Z" in set(store.node_labels())

    def test_can_add_existing_label(self):
        store = MutableGraphStore(self.store)
        labels = store.node_labels(self.a)
        self.assertEqual(labels, {"X"})
        labels.add("X")
        self.assertEqual(store.node_labels(self.a), {"X"})

    def test_can_remove_label(self):
        store = MutableGraphStore(self.store)
        labels = store.node_labels(self.a)
        self.assertEqual(labels, {"X"})
        labels.remove("X")
        self.assertFalse(store.node_labels(self.a))

    def test_can_discard_label(self):
        store = MutableGraphStore(self.store)
        labels = store.node_labels(self.a)
        self.assertEqual(labels, {"X"})
        labels.discard("Z")
        self.assertEqual(store.node_labels(self.a), {"X"})

    def test_can_clear_labels(self):
        store = MutableGraphStore(self.store)
        labels = store.node_labels(self.b)
        self.assertEqual(labels, {"X", "Y"})
        labels.clear()
        self.assertFalse(store.node_labels(self.b))

    def test_can_add_properties(self):
        store = MutableGraphStore(self.store)
        properties = store.node_properties(self.a)
        self.assertEqual(properties, {"name": "Alice"})
        properties["age"] = 33
        self.assertEqual(store.node_properties(self.a), {"name": "Alice", "age": 33})

    def test_can_update_properties(self):
        store = MutableGraphStore(self.store)
        properties = store.node_properties(self.a)
        self.assertEqual(properties, {"name": "Alice"})
        properties["name"] = "Alistair"
        self.assertEqual(store.node_properties(self.a), {"name": "Alistair"})

    def test_can_remove_properties(self):
        store = MutableGraphStore(self.store)
        properties = store.node_properties(self.a)
        self.assertEqual(properties, {"name": "Alice"})
        del properties["name"]
        self.assertEqual(store.node_properties(self.a), {})

    def test_should_allow_construction_arguments(self):
        store = MutableGraphStore.build({
            "a": (["Person"], {"name": "Alice", "age": 33}),
            "b": (["Person"], {"name": "Bob", "age": 44}),
        }, {
            "ab": ("KNOWS", ("a", "b"), {"since": 1999}),
        })
        self.assertIsInstance(store, MutableGraphStore)
        self.assertEqual(store.node_count(), 2)
        self.assertEqual(store.relationship_count(), 1)
        self.assertEqual(store.node_labels(), {"Person"})
        self.assertEqual(store.relationship_types(), {"KNOWS"})
        self.assertEqual(set(store.nodes("Person")), {"a", "b"})
        self.assertEqual(store.node_labels("a"), {"Person"})
        self.assertEqual(store.node_labels("b"), {"Person"})
        self.assertEqual(store.node_properties("a"), {"name": "Alice", "age": 33})
        self.assertEqual(store.node_properties("b"), {"name": "Bob", "age": 44})
        self.assertEqual(set(store.relationships(r_type="KNOWS")), {"ab"})
        self.assertEqual(store.relationship_type("ab"), "KNOWS")
        self.assertEqual(store.relationship_properties("ab"), {"since": 1999})

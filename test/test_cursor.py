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


from io import BytesIO
from unittest import TestCase

from py2neo.graph import Record, Node, Relationship
from py2neo.types import order, size
from test.util import GraphTestCase


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
record_a = Record(record_keys, [1001, alice])
record_b = Record(record_keys, [1002, bob])
record_c = Record(record_keys, [1003, carol])
record_d = Record(record_keys, [1004, dave])


class CursorMovementTestCase(GraphTestCase):
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


class CursorKeysTestCase(GraphTestCase):

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


class CursorStatsTestCase(GraphTestCase):

    def test_stats_available(self):
        cursor = self.graph.run("CREATE (a:Banana)")
        stats = cursor.stats()
        assert stats["nodes_created"] == 1
        assert stats["labels_added"] == 1
        assert stats["contains_updates"] == 1

    def test_stats_available_over_http(self):
        cursor = self.http_graph.run("CREATE (a:Banana)")
        stats = cursor.stats()
        assert stats["nodes_created"] == 1
        assert stats["labels_added"] == 1
        assert stats["contains_updates"] == 1


class CursorCurrentTestCase(GraphTestCase):

    def test_current_is_none_at_start(self):
        cursor = self.graph.run("RETURN 1")
        assert cursor.current() is None

    def test_current_updates_after_move(self):
        cursor = self.graph.run("UNWIND range(1, 10) AS n RETURN n")
        n = 0
        while cursor.forward():
            n += 1
            assert cursor.current() == Record(["n"], [n])


class CursorSelectionTestCase(GraphTestCase):

    def test_select_picks_next(self):
        cursor = self.graph.run("RETURN 1")
        record = cursor.next()
        assert record == Record(["1"], [1])

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


class CursorAsIteratorTestCase(GraphTestCase):

    def test_can_use_next_function(self):
        cursor = self.graph.run("RETURN 1")
        record = next(cursor)
        assert record == Record(["1"], [1])

    def test_raises_stop_iteration(self):
        cursor = self.graph.run("RETURN 1")
        cursor.forward()
        with self.assertRaises(StopIteration):
            _ = next(cursor)


class CursorStreamingTestCase(GraphTestCase):

    def test_stream_yields_all(self):
        cursor = self.graph.run("UNWIND range(1, 10) AS n RETURN n, n * n as n_sq")
        record_list = list(cursor)
        assert record_list == [Record(["n", "n_sq"], [1, 1]),
                               Record(["n", "n_sq"], [2, 4]),
                               Record(["n", "n_sq"], [3, 9]),
                               Record(["n", "n_sq"], [4, 16]),
                               Record(["n", "n_sq"], [5, 25]),
                               Record(["n", "n_sq"], [6, 36]),
                               Record(["n", "n_sq"], [7, 49]),
                               Record(["n", "n_sq"], [8, 64]),
                               Record(["n", "n_sq"], [9, 81]),
                               Record(["n", "n_sq"], [10, 100])]

    def test_stream_yields_remainder(self):
        cursor = self.graph.run("UNWIND range(1, 10) AS n RETURN n, n * n as n_sq")
        cursor.forward(5)
        record_list = list(cursor)
        assert record_list == [Record(["n", "n_sq"], [6, 36]),
                               Record(["n", "n_sq"], [7, 49]),
                               Record(["n", "n_sq"], [8, 64]),
                               Record(["n", "n_sq"], [9, 81]),
                               Record(["n", "n_sq"], [10, 100])]


class CursorEvaluationTestCase(GraphTestCase):

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


class CursorDumpTestCase(GraphTestCase):

    def test_dump(self):
        s = BytesIO()
        cursor = self.graph.run("RETURN 1")
        cursor.dump(out=s)
        assert s.getvalue()


class RecordTestCase(TestCase):

    def test_can_build_record(self):
        record = Record(["name", "age"], ["Alice", 33])
        assert len(record) == 2
        assert record.keys() == ("name", "age")
        assert record.values() == ("Alice", 33)
        assert record.items() == [("name", "Alice"), ("age", 33)]
        assert record.data() == {"name": "Alice", "age": 33}
        r = repr(record)
        assert r.startswith("(") and r.endswith(")")

    def test_cannot_build_record_with_mismatched_keys_and_values(self):
        with self.assertRaises(ValueError):
            Record(["name"], ["Alice", 33])

    def test_can_coerce_record(self):
        record = Record(["name", "age"], ["Alice", 33])
        assert tuple(record) == ("Alice", 33)
        assert list(record) == ["Alice", 33]
        assert dict(record) == {"name": "Alice", "age": 33}

    def test_can_get_record_value_by_name(self):
        record = Record(["one", "two", "three"], ["eins", "zwei", "drei"])
        assert record["one"] == "eins"
        assert record["two"] == "zwei"
        assert record["three"] == "drei"

    def test_cannot_get_record_value_by_missing_name(self):
        record = Record(["one", "two", "three"], ["eins", "zwei", "drei"])
        with self.assertRaises(KeyError):
            _ = record["four"]

    def test_can_get_record_value_by_index(self):
        record = Record(["one", "two", "three"], ["eins", "zwei", "drei"])
        assert record[0] == "eins"
        assert record[1] == "zwei"
        assert record[2] == "drei"
        assert record[-1] == "drei"

    def test_can_get_record_values_by_slice(self):
        record = Record(["one", "two", "three"], ["eins", "zwei", "drei"])
        assert record[0:2] == Record(["one", "two"], ["eins", "zwei"])
        assert record[1:2] == Record(["two"], ["zwei"])
        assert record[1:3] == Record(["two", "three"], ["zwei", "drei"])
        assert record[1:] == Record(["two", "three"], ["zwei", "drei"])

    def test_can_get_record_values_by_slice_using_getitem(self):
        record = Record(["one", "two", "three"], ["eins", "zwei", "drei"])
        assert record.__getitem__(slice(0, 2)) == Record(["one", "two"], ["eins", "zwei"])

    def test_can_get_record_values_by_slice_using_getslice(self):
        record = Record(["one", "two", "three"], ["eins", "zwei", "drei"])
        try:
            s = record.__getslice__(0, 2)
        except AttributeError:
            assert True
        else:
            assert s == Record(["one", "two"], ["eins", "zwei"])

    def test_cannot_get_record_value_by_anything_else(self):
        record = Record(["one", "two", "three"], ["eins", "zwei", "drei"])
        with self.assertRaises(TypeError):
            _ = record[None]

    def test_record_can_be_converted_to_subgraph(self):
        keys = ["a", "b", "ab", "msg"]
        values = [alice, bob, alice_knows_bob, "hello, world"]
        record = Record(keys, values)
        subgraph = record.subgraph()
        assert order(subgraph) == 2
        assert size(subgraph) == 1
        assert subgraph.nodes() == {alice, bob}
        assert subgraph.relationships() == {alice_knows_bob}

    def test_record_with_no_graphy_objects_converts_to_null_subgraph(self):
        keys = ["a", "b", "c"]
        values = [1, 2.0, "three"]
        record = Record(keys, values)
        subgraph = record.subgraph()
        assert subgraph is None

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
from py2neo.types import Record
from test.util import Py2neoTestCase


class CursorMovementTestCase(Py2neoTestCase):
    """ Tests for move and position
    """

    def test_start_position_is_zero(self):
        cursor = self.cypher.run("RETURN 1")
        assert cursor.position() == 0

    def test_position_updates_after_move(self):
        cursor = self.cypher.run("UNWIND range(1, 10) AS n RETURN n")
        expected_position = 0
        while cursor.move():
            expected_position += 1
            assert cursor.position() == expected_position

    def test_cannot_move_beyond_end(self):
        cursor = self.cypher.run("RETURN 1")
        assert cursor.move()
        assert not cursor.move()

    def test_can_only_move_until_end(self):
        cursor = self.cypher.run("RETURN 1")
        assert cursor.move(2) == 1

    def test_moving_by_zero_keeps_same_position(self):
        cursor = self.cypher.run("RETURN 1")
        assert cursor.move(0) == 0
        assert cursor.position() == 0


class CursorKeysTestCase(Py2neoTestCase):

    def test_keys_is_none_at_start(self):
        cursor = self.cypher.run("RETURN 1")
        assert cursor.keys() is None

    def test_keys_updates_after_move(self):
        cursor = self.cypher.run("UNWIND range(1, 10) AS n RETURN n")
        n = 0
        while cursor.move():
            n += 1
            assert list(cursor.keys()) == ["n"]


class CursorCurrentTestCase(Py2neoTestCase):

    def test_current_is_none_at_start(self):
        cursor = self.cypher.run("RETURN 1")
        assert cursor.current() is None

    def test_current_updates_after_move(self):
        cursor = self.cypher.run("UNWIND range(1, 10) AS n RETURN n")
        n = 0
        while cursor.move():
            n += 1
            assert cursor.current() == Record(["n"], [n])

    def test_current_with_specific_fields(self):
        cursor = self.cypher.run("UNWIND range(1, 10) AS n RETURN n, n * n AS n_sq")
        n = 0
        while cursor.move():
            n += 1
            assert cursor.current(0) == Record(["n"], [n])
            assert cursor.current("n") == Record(["n"], [n])
            assert cursor.current(1) == Record(["n_sq"], [n * n])
            assert cursor.current("n_sq") == Record(["n_sq"], [n * n])
            assert cursor.current("n", "n_sq") == Record(["n", "n_sq"], [n, n * n])


class CursorSelectionTestCase(Py2neoTestCase):

    def test_select_picks_next(self):
        cursor = self.cypher.run("RETURN 1")
        record = cursor.select()
        assert record == Record(["1"], [1])

    def test_cannot_select_past_end(self):
        cursor = self.cypher.run("RETURN 1")
        cursor.move()
        record = cursor.select()
        assert record is None

    def test_selection_triggers_move(self):
        cursor = self.cypher.run("UNWIND range(1, 10) AS n RETURN n, n * n as n_sq")
        for i in range(1, 11):
            n, n_sq = cursor.select()
            assert n == i
            assert n_sq == i * i

    def test_selection_with_specific_fields(self):
        cursor = self.cypher.run("UNWIND range(1, 10) AS n RETURN n, n * n as n_sq")
        for i in range(1, 11):
            n_sq, = cursor.select("n_sq")
            assert n_sq == i * i


class CursorCollectionTestCase(Py2neoTestCase):

    def test_collect_yields_all(self):
        cursor = self.cypher.run("UNWIND range(1, 10) AS n RETURN n, n * n as n_sq")
        collected = list(cursor.collect())
        assert collected == [Record(["n", "n_sq"], [1, 1]),
                             Record(["n", "n_sq"], [2, 4]),
                             Record(["n", "n_sq"], [3, 9]),
                             Record(["n", "n_sq"], [4, 16]),
                             Record(["n", "n_sq"], [5, 25]),
                             Record(["n", "n_sq"], [6, 36]),
                             Record(["n", "n_sq"], [7, 49]),
                             Record(["n", "n_sq"], [8, 64]),
                             Record(["n", "n_sq"], [9, 81]),
                             Record(["n", "n_sq"], [10, 100])]

    def test_collect_with_specific_fields(self):
        cursor = self.cypher.run("UNWIND range(1, 10) AS n RETURN n, n * n as n_sq")
        collected = list(cursor.collect("n_sq"))
        assert collected == [Record(["n_sq"], [1]),
                             Record(["n_sq"], [4]),
                             Record(["n_sq"], [9]),
                             Record(["n_sq"], [16]),
                             Record(["n_sq"], [25]),
                             Record(["n_sq"], [36]),
                             Record(["n_sq"], [49]),
                             Record(["n_sq"], [64]),
                             Record(["n_sq"], [81]),
                             Record(["n_sq"], [100])]

    def test_collect_yields_remainder(self):
        cursor = self.cypher.run("UNWIND range(1, 10) AS n RETURN n, n * n as n_sq")
        cursor.move(5)
        collected = list(cursor.collect())
        assert collected == [Record(["n", "n_sq"], [6, 36]),
                             Record(["n", "n_sq"], [7, 49]),
                             Record(["n", "n_sq"], [8, 64]),
                             Record(["n", "n_sq"], [9, 81]),
                             Record(["n", "n_sq"], [10, 100])]


class CursorEvaluationTestCase(Py2neoTestCase):

    def test_can_evaluate_single_value(self):
        cursor = self.cypher.run("RETURN 1")
        value = cursor.evaluate()
        assert value == 1

    def test_can_evaluate_value_by_index(self):
        cursor = self.cypher.run("RETURN 1, 2")
        value = cursor.evaluate(1)
        assert value == 2

    def test_can_evaluate_value_by_key(self):
        cursor = self.cypher.run("RETURN 1 AS first, 2 AS second")
        value = cursor.evaluate("second")
        assert value == 2

    def test_evaluate_with_no_records_is_none(self):
        cursor = self.cypher.run("RETURN 1")
        cursor.move()
        value = cursor.evaluate()
        assert value is None


class CursorMagicTestCase(Py2neoTestCase):

    def test_repr(self):
        cursor = self.cypher.run("RETURN 1, 2, 3")
        r = repr(cursor)
        assert r.startswith("<Cursor")

    def test_len_returns_length_of_record(self):
        cursor = self.cypher.run("RETURN 1, 2, 3")
        cursor.move()
        assert len(cursor) == 3

    def test_len_fails_with_no_record(self):
        cursor = self.cypher.run("RETURN 1, 2, 3")
        with self.assertRaises(TypeError):
            _ = len(cursor)

    def test_getitem_returns_item_in_record(self):
        cursor = self.cypher.run("RETURN 1, 2, 3")
        cursor.move()
        assert cursor[0] == 1
        assert cursor[1] == 2
        assert cursor[2] == 3

    def test_getitem_fails_with_no_record(self):
        cursor = self.cypher.run("RETURN 1, 2, 3")
        with self.assertRaises(TypeError):
            _ = cursor[0]

    def test_iter_iterates_record(self):
        cursor = self.cypher.run("RETURN 1, 2, 3")
        cursor.move()
        assert list(cursor) == [1, 2, 3]

    def test_iter_fails_with_no_record(self):
        cursor = self.cypher.run("RETURN 1, 2, 3")
        with self.assertRaises(TypeError):
            _ = list(cursor)


class CursorDumpTestCase(Py2neoTestCase):

    def test_dump(self):
        s = StringIO()
        cursor = self.cypher.run("RETURN 1")
        cursor.dump(out=s)
        assert s.getvalue()

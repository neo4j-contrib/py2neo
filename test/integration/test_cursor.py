#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright 2011-2021, Nigel Small
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


from pytest import raises

from py2neo import Record, Subgraph


def test_cannot_move_beyond_end(graph):
    cursor = graph.run("RETURN 1")
    assert cursor.forward()
    assert not cursor.forward()


def test_can_only_move_until_end(graph):
    cursor = graph.run("RETURN 1")
    assert cursor.forward(2) == 1


def test_can_only_move_forward(graph):
    cursor = graph.run("RETURN 1")
    with raises(ValueError):
        cursor.forward(-1)


def test_moving_by_zero_keeps_same_position(graph):
    cursor = graph.run("RETURN 1")
    assert cursor.forward(0) == 0


def test_keys_are_populated_before_moving(graph):
    cursor = graph.run("RETURN 1 AS n")
    assert list(cursor.keys()) == ["n"]


def test_keys_are_populated_after_moving(graph):
    cursor = graph.run("UNWIND range(1, 10) AS n RETURN n")
    n = 0
    while cursor.forward():
        n += 1
        assert list(cursor.keys()) == ["n"]


def test_keys_are_populated_before_moving_within_a_transaction(graph):
    tx = graph.begin()
    cursor = tx.run("RETURN 1 AS n")
    assert list(cursor.keys()) == ["n"]
    graph.rollback(tx)


def test_stats_available(graph):
    cursor = graph.run("CREATE (a:Banana)")
    stats = cursor.stats()
    assert stats["nodes_created"] == 1
    assert stats["labels_added"] == 1


def test_current_is_none_at_start(graph):
    cursor = graph.run("RETURN 1")
    assert cursor.current is None


def test_current_updates_after_move(graph):
    cursor = graph.run("UNWIND range(1, 10) AS n RETURN n")
    n = 0
    while cursor.forward():
        n += 1
        assert cursor.current == Record(["n"], [n])


def test_select_picks_next(graph):
    cursor = graph.run("RETURN 1")
    record = next(cursor)
    assert record == Record(["1"], [1])


def test_cannot_select_past_end(graph):
    cursor = graph.run("RETURN 1")
    cursor.forward()
    with raises(StopIteration):
        _ = next(cursor)


def test_selection_triggers_move(graph):
    cursor = graph.run("UNWIND range(1, 10) AS n RETURN n, n * n as n_sq")
    for i in range(1, 11):
        n, n_sq = next(cursor)
        assert n == i
        assert n_sq == i * i


def test_can_use_next_function(graph):
    cursor = graph.run("RETURN 1")
    record = next(cursor)
    assert record == Record(["1"], [1])


def test_raises_stop_iteration(graph):
    cursor = graph.run("RETURN 1")
    cursor.forward()
    with raises(StopIteration):
        _ = next(cursor)


def test_can_get_data(graph):
    cursor = graph.run("UNWIND range(1, 3) AS n RETURN n, n * n AS n_sq")
    data = cursor.data()
    assert data == [{"n": 1, "n_sq": 1}, {"n": 2, "n_sq": 4}, {"n": 3, "n_sq": 9}]


def test_stream_yields_all(graph):
    cursor = graph.run("UNWIND range(1, 10) AS n RETURN n, n * n as n_sq")
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


def test_stream_yields_remainder(graph):
    cursor = graph.run("UNWIND range(1, 10) AS n RETURN n, n * n as n_sq")
    cursor.forward(5)
    record_list = list(cursor)
    assert record_list == [Record(["n", "n_sq"], [6, 36]),
                           Record(["n", "n_sq"], [7, 49]),
                           Record(["n", "n_sq"], [8, 64]),
                           Record(["n", "n_sq"], [9, 81]),
                           Record(["n", "n_sq"], [10, 100])]


def test_can_evaluate_single_value(graph):
    cursor = graph.run("RETURN 1")
    value = cursor.evaluate()
    assert value == 1


def test_can_evaluate_value_by_index(graph):
    cursor = graph.run("RETURN 1, 2")
    value = cursor.evaluate(1)
    assert value == 2


def test_can_evaluate_value_by_key(graph):
    cursor = graph.run("RETURN 1 AS first, 2 AS second")
    value = cursor.evaluate("second")
    assert value == 2


def test_evaluate_with_no_records_is_none(graph):
    cursor = graph.run("RETURN 1")
    cursor.forward()
    value = cursor.evaluate()
    assert value is None


def test_evaluate_on_non_existent_column_is_none(graph):
    cursor = graph.run("RETURN 1")
    value = cursor.evaluate(1)
    assert value is None


def test_to_subgraph(graph):
    s = graph.run("CREATE p=(:Person {name:'Alice'})-[:KNOWS]->(:Person {name:'Bob'}) RETURN p").to_subgraph()
    assert isinstance(s, Subgraph)
    assert len(s.nodes) == 2
    assert len(s.relationships) == 1


def test_preview_limit_must_be_positive(graph):
    cursor = graph.run("RETURN 1")
    with raises(ValueError):
        cursor.preview(-1)


def test_to_ndarray(graph):
    cursor = graph.run("RETURN 1")
    try:
        cursor.to_ndarray()
    except ImportError:
        assert True
    else:
        assert True


def test_to_series(graph):
    cursor = graph.run("RETURN 1")
    try:
        cursor.to_series()
    except ImportError:
        assert True
    else:
        assert True


def test_to_data_frame(graph):
    cursor = graph.run("RETURN 1")
    try:
        cursor.to_data_frame()
    except ImportError:
        assert True
    else:
        assert True


def test_to_matrix(graph):
    cursor = graph.run("RETURN 1")
    try:
        cursor.to_matrix()
    except ImportError:
        assert True
    else:
        assert True

#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright (c) "Neo4j"
# Neo4j Sweden AB [https://neo4j.com]
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

from collections import deque

from pytest import skip, raises

from py2neo import ClientError
from py2neo.client import Connector
from py2neo.cypher import Cursor, Record


def test_keys(connector):
    result = connector.auto_run("RETURN 'Alice' AS name, 33 AS age")
    connector.pull(result, -1)
    cursor = Cursor(result)
    expected = ["name", "age"]
    actual = cursor.keys()
    assert expected == actual


def test_records(connector):
    result = connector.auto_run("UNWIND range(1, $x) AS n RETURN n, n * n AS n_sq", {"x": 3})
    connector.pull(result, -1)
    cursor = Cursor(result)
    expected = deque([(1, 1), (2, 4), (3, 9)])
    for actual_record in cursor:
        expected_record = Record(["n", "n_sq"], expected.popleft())
        assert expected_record == actual_record


def test_stats(connector):
    result = connector.auto_run("CREATE ()")
    connector.pull(result, -1)
    cursor = Cursor(result)
    stats = cursor.stats()
    assert stats["nodes_created"] == 1


def test_auto_run_with_pull_all(service_profile):
    connector = Connector(service_profile)
    result = connector.auto_run("UNWIND range(1, 5) AS n RETURN n")
    connector.pull(result, -1)
    assert result.take() == [1]
    assert result.take() == [2]
    assert result.take() == [3]
    assert result.take() == [4]
    assert result.take() == [5]
    assert result.take() is None
    connector.close()


def test_auto_run_with_pull_3_then_pull_all(service_profile):
    connector = Connector(service_profile)
    try:
        result = connector.auto_run("UNWIND range(1, 5) AS n RETURN n")
        connector.pull(result, 3)
    except IndexError as error:
        skip(str(error))
    else:
        assert result.take() == [1]
        assert result.take() == [2]
        assert result.take() == [3]
        assert result.take() is None
        connector.pull(result)
        assert result.take() == [4]
        assert result.take() == [5]
        assert result.take() is None
    finally:
        connector.close()


def test_bad_cypher(connector):
    with raises(ClientError):
        result = connector.auto_run("X")
        connector.pull(result, -1)

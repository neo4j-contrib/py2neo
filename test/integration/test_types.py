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


from packaging.version import Version

from interchange.geo import CartesianPoint, WGS84Point
from interchange.time import Date, Time, DateTime, Duration

from pytest import skip


from py2neo.data import Node


def test_null(graph):
    i = None
    o = graph.evaluate("RETURN $x", x=i)
    assert o is i


def test_true(graph):
    i = True
    o = graph.evaluate("RETURN $x", x=i)
    assert o is i


def test_false(graph):
    i = False
    o = graph.evaluate("RETURN $x", x=i)
    assert o is i


def test_int(graph):
    for i in range(-128, 128):
        o = graph.evaluate("RETURN $x", x=i)
        assert o == i


def test_float(graph):
    for i in range(-128, 128):
        f = float(i) + 0.5
        o = graph.evaluate("RETURN $x", x=f)
        assert o == f


def test_string(graph):
    i = u"hello, world"
    o = graph.evaluate("RETURN $x", x=i)
    assert o == i


def test_bytes(graph):
    i = bytearray([65, 66, 67])
    o = graph.evaluate("RETURN $x", x=i)
    # The values are coerced to bytearray before comparison
    # as HTTP does not support byte parameters, instead
    # coercing such values to lists of integers.
    assert bytearray(o) == bytearray(i)


def test_list(graph):
    i = [65, 66, 67]
    o = graph.evaluate("RETURN $x", x=i)
    assert o == i


def test_dict(graph):
    i = {"one": 1, "two": 2}
    o = graph.evaluate("RETURN $x", x=i)
    assert o == i


def test_node(graph):
    i = Node("Person", name="Alice")
    o = graph.evaluate("CREATE (a:Person {name: 'Alice'}) RETURN a")
    assert o.labels == i.labels
    assert dict(o) == dict(i)


def test_relationship(graph):
    o = graph.evaluate("CREATE ()-[r:KNOWS {since: 1999}]->() RETURN r")
    assert type(o).__name__ == "KNOWS"
    assert dict(o) == {"since": 1999}


def test_path(graph):
    o = graph.evaluate("CREATE p=(:Person {name: 'Alice'})-[:KNOWS]->(:Person {name: 'Bob'}) RETURN p")
    assert len(o) == 1
    assert o.start_node.labels == {"Person"}
    assert dict(o.start_node) == {"name": "Alice"}
    assert type(o.relationships[0]).__name__ == "KNOWS"
    assert o.end_node.labels == {"Person"}
    assert dict(o.end_node) == {"name": "Bob"}


def skip_if_no_temporal_support(graph):
    connector = graph.service.connector
    if graph.service.kernel_version < Version("3.4"):
        skip("Temporal type tests are only valid for Neo4j 3.4+")
    if connector.profile.protocol != "bolt":
        skip("Temporal type tests are only valid for Bolt connectors")


def test_date(graph):
    skip_if_no_temporal_support(graph)
    i = Date(2014, 8, 6)
    o = graph.evaluate("RETURN $x", x=i)
    assert o == i


def test_time(graph):
    skip_if_no_temporal_support(graph)
    i = Time(12, 34, 56.789)
    o = graph.evaluate("RETURN $x", x=i)
    assert o == i


def test_date_time(graph):
    skip_if_no_temporal_support(graph)
    i = DateTime(2014, 8, 6, 12, 34, 56.789)
    o = graph.evaluate("RETURN $x", x=i)
    assert o == i


def test_duration(graph):
    skip_if_no_temporal_support(graph)
    i = Duration(months=1, days=2, seconds=3)
    o = graph.evaluate("RETURN $x", x=i)
    assert o == i


def test_cartesian_point(graph):
    skip_if_no_temporal_support(graph)
    i = CartesianPoint((12.34, 56.78))
    o = graph.evaluate("RETURN $x", x=i)
    assert o == i


def test_wgs84_point(graph):
    skip_if_no_temporal_support(graph)
    i = WGS84Point((12.34, 56.78))
    o = graph.evaluate("RETURN $x", x=i)
    assert o == i

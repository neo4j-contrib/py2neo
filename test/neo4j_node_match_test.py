#/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright 2011-2013, Nigel Small
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


from py2neo import neo4j


def _create_graph():
    graph_db = neo4j.GraphDatabaseService()
    a, b, c, d, e = graph_db.create(
        {"name": "Alice"},
        {"name": "Bob"},
        {"name": "Carol"},
        {"name": "Dave"},
        {"name": "Eve"},
    )
    rels = graph_db.create(
        (a, "LOVES", b),
        (b, "LOVES", a),
        (b, "KNOWS", c),
        (b, "KNOWS", d),
        (d, "LOVES", e),
    )
    return a, b, c, d, e, rels


def test_can_match_zero_outgoing():
    a, b, c, d, e, rels = _create_graph()
    matches = list(e.match_outgoing())
    assert len(matches) == 0


def test_can_match_one_outgoing():
    a, b, c, d, e, rels = _create_graph()
    matches = list(a.match_outgoing())
    assert len(matches) == 1
    assert rels[0] in matches


def test_can_match_many_outgoing():
    a, b, c, d, e, rels = _create_graph()
    matches = list(b.match_outgoing())
    assert len(matches) == 3
    assert rels[1] in matches
    assert rels[2] in matches
    assert rels[3] in matches


def test_can_match_many_outgoing_with_limit():
    a, b, c, d, e, rels = _create_graph()
    matches = list(b.match_outgoing(limit=2))
    assert len(matches) == 2
    for match in matches:
        assert match in (rels[1], rels[2], rels[3])


def test_can_match_many_outgoing_by_type():
    a, b, c, d, e, rels = _create_graph()
    matches = list(b.match_outgoing("KNOWS"))
    assert len(matches) == 2
    assert rels[2] in matches
    assert rels[3] in matches


def test_can_match_many_outgoing_by_multiple_types():
    a, b, c, d, e, rels = _create_graph()
    matches = list(b.match_outgoing(("KNOWS", "LOVES")))
    assert len(matches) == 3
    assert rels[1] in matches
    assert rels[2] in matches
    assert rels[3] in matches


def test_can_match_many_in_both_directions():
    a, b, c, d, e, rels = _create_graph()
    matches = list(b.match())
    assert len(matches) == 4
    assert rels[0] in matches
    assert rels[1] in matches
    assert rels[2] in matches
    assert rels[3] in matches


def test_can_match_many_in_both_directions_with_limit():
    a, b, c, d, e, rels = _create_graph()
    matches = list(b.match(limit=2))
    assert len(matches) == 2
    for match in matches:
        assert match in (rels[0], rels[1], rels[2], rels[3])


def test_can_match_many_by_type_in_both_directions():
    a, b, c, d, e, rels = _create_graph()
    matches = list(b.match("LOVES"))
    assert len(matches) == 2
    assert rels[0] in matches
    assert rels[1] in matches

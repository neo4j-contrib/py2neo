#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright 2011-2020, Nigel Small
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


from os.path import join as path_join, dirname

from pytest import fixture, raises

from py2neo.data import Node
from py2neo.matching import NodeMatcher, \
    EQ, NE, LT, LE, GT, GE, \
    STARTS_WITH, ENDS_WITH, CONTAINS, LIKE, \
    IN, AND, OR, XOR


@fixture()
def good_node_id(graph):
    return graph.evaluate("MATCH (a) RETURN max(id(a))")


@fixture()
def bad_node_id(graph):
    return graph.evaluate("MATCH (a) RETURN max(id(a)) + 1")


@fixture()
def movie_matcher(graph):
    graph.delete_all()
    with open(path_join(dirname(__file__), "..", "resources", "movies.cypher")) as f:
        cypher = f.read()
    graph.run(cypher)
    return NodeMatcher(graph)


def test_node_matcher_iter(movie_matcher):
    node_ids = list(movie_matcher)
    assert len(node_ids) == 169


def test_node_matcher_len(movie_matcher):
    node_count = len(movie_matcher)
    assert node_count == 169


def test_node_matcher_contains(movie_matcher, good_node_id):
    node_exists = good_node_id in movie_matcher
    assert node_exists


def test_node_matcher_does_not_contain(movie_matcher, bad_node_id):
    node_exists = bad_node_id in movie_matcher
    assert not node_exists


def test_node_matcher_getitem(movie_matcher, good_node_id):
    node = movie_matcher[good_node_id]
    assert isinstance(node, Node)


def test_node_matcher_getitem_fail(movie_matcher, bad_node_id):
    with raises(KeyError):
        _ = movie_matcher[bad_node_id]


def test_node_matcher_get(movie_matcher, good_node_id):
    node = movie_matcher.get(good_node_id)
    assert isinstance(node, Node)


def test_node_matcher_get_fail(movie_matcher, bad_node_id):
    node = movie_matcher.get(bad_node_id)
    assert node is None


def test_can_match_by_label_key_value(movie_matcher):
    found = list(movie_matcher.match("Person", name="Keanu Reeves"))
    assert len(found) == 1
    first = found[0]
    assert isinstance(first, Node)
    assert first["name"] == "Keanu Reeves"
    assert first["born"] == 1964


def test_can_match_by_label_only(movie_matcher):
    found = list(movie_matcher.match("Person"))
    assert len(found) == 131


def test_can_match_all_nodes(movie_matcher):
    found = list(movie_matcher.match())
    assert len(found) == 169


def test_can_count_all_nodes(movie_matcher):
    count = len(movie_matcher.match())
    assert count == 169


def test_can_match_by_label_and_multiple_values(movie_matcher):
    found = list(movie_matcher.match("Person", name="Keanu Reeves", born=1964))
    assert len(found) == 1
    first = found[0]
    assert isinstance(first, Node)
    assert first["name"] == "Keanu Reeves"
    assert first["born"] == 1964


def test_multiple_values_must_intersect(movie_matcher):
    found = list(movie_matcher.match("Person", name="Keanu Reeves", born=1963))
    assert len(found) == 0


def test_custom_conditions(movie_matcher):
    found = list(movie_matcher.match("Person").where("_.name =~ 'K.*'"))
    found_names = {actor["name"] for actor in found}
    assert found_names == {'Keanu Reeves', 'Kelly McGillis', 'Kevin Bacon',
                           'Kevin Pollak', 'Kiefer Sutherland', 'Kelly Preston'}


def test_custom_conditions_with_parameters(movie_matcher):
    found = list(movie_matcher.match("Person").where(("_.name = $1", {"1": "Keanu Reeves"})))
    assert len(found) == 1
    first = found[0]
    assert isinstance(first, Node)
    assert first["name"] == "Keanu Reeves"
    assert first["born"] == 1964


def test_predicate_none(movie_matcher):
    people = list(movie_matcher.match("Person", born=None))
    names = {person["name"] for person in people}
    assert names == {'Naomie Harris', 'Jessica Thompson',
                     'Angela Scope', 'Paul Blythe', 'James Thompson'}


def test_predicate_eq(movie_matcher):
    people = movie_matcher.match("Person", born=EQ(1964))
    names = {person["name"] for person in people}
    assert names == {"Keanu Reeves"}


def test_predicate_ne(movie_matcher):
    people = movie_matcher.match("Person", born=NE(1964))
    names = {person["name"] for person in people}
    assert len(names) == 125
    assert "Keanu Reeves" not in names


def test_predicate_lt(movie_matcher):
    people = movie_matcher.match("Person", born=LT(1930))
    names = {person["name"] for person in people}
    assert names == {"Max von Sydow"}


def test_predicate_le(movie_matcher):
    people = movie_matcher.match("Person", born=LE(1930))
    names = {person["name"] for person in people}
    assert names == {'Max von Sydow', 'Gene Hackman',
                     'Clint Eastwood', 'Richard Harris'}


def test_predicate_gt(movie_matcher):
    people = movie_matcher.match("Person", born=GT(1985))
    names = {person["name"] for person in people}
    assert names == {"Jonathan Lipnicki"}


def test_predicate_ge(movie_matcher):
    people = movie_matcher.match("Person", born=GE(1985))
    names = {person["name"] for person in people}
    assert names == {"Emile Hirsch", "Jonathan Lipnicki"}


def test_predicate_starts_with(movie_matcher):
    people = movie_matcher.match("Person", name=STARTS_WITH("Keanu"))
    names = {person["name"] for person in people}
    assert names == {"Keanu Reeves"}


def test_predicate_ends_with(movie_matcher):
    people = movie_matcher.match("Person", name=ENDS_WITH("Reeves"))
    names = {person["name"] for person in people}
    assert names == {"Keanu Reeves"}


def test_predicate_contains(movie_matcher):
    people = movie_matcher.match("Person", name=CONTAINS("eve"))
    names = {person["name"] for person in people}
    assert names == {"Keanu Reeves", "Steve Zahn"}


def test_predicate_like_from_string(movie_matcher):
    pattern = 'K.*s'
    people = list(movie_matcher.match("Person", name=LIKE(pattern)))
    names = {person["name"] for person in people}
    assert names == {'Kelly McGillis', 'Keanu Reeves'}


def test_predicate_like_from_regex(movie_matcher):
    import re
    pattern = re.compile(r'K.*s')
    people = list(movie_matcher.match("Person", name=LIKE(pattern)))
    names = {person["name"] for person in people}
    assert names == {'Kelly McGillis', 'Keanu Reeves'}


def test_predicate_in(movie_matcher):
    people = list(movie_matcher.match("Person", born=IN([1930, 1940, 1950])))
    names = {person["name"] for person in people}
    assert names == {'Al Pacino', 'Clint Eastwood', 'Ed Harris',
                     'Gene Hackman', 'Howard Deutch', 'James Cromwell',
                     'James L. Brooks', 'John Hurt',
                     'John Patrick Stanley', 'Richard Harris'}


def test_multiple_predicates(movie_matcher):
    found = list(movie_matcher.match("Person").where(name=STARTS_WITH("J"),
                                                     born=AND(GE(1960), LT(1970))))
    found_names = {actor["name"] for actor in found}
    assert found_names == {'James Marshall', 'John Cusack', 'John Goodman',
                           'John C. Reilly', 'Julia Roberts'}


def test_mutually_exclusive_predicates(movie_matcher):
    found = list(movie_matcher.match("Person").where(name=OR(LIKE("K.*d"), LIKE("K.*n"))))
    found_names = {actor["name"] for actor in found}
    assert found_names == {'Kiefer Sutherland', 'Kelly Preston', 'Kevin Bacon'}


def test_order_by(movie_matcher):
    found = list(movie_matcher.match("Person").where("_.name =~ 'K.*'").order_by("_.name"))
    found_names = [actor["name"] for actor in found]
    assert found_names == ['Keanu Reeves', 'Kelly McGillis', 'Kelly Preston',
                           'Kevin Bacon', 'Kevin Pollak', 'Kiefer Sutherland']


def test_skip(movie_matcher):
    found = list(movie_matcher.match("Person").where("_.name =~ 'K.*'").order_by("_.name").skip(2))
    found_names = [actor["name"] for actor in found]
    assert found_names == ['Kelly Preston', 'Kevin Bacon', 'Kevin Pollak', 'Kiefer Sutherland']


def test_limit(movie_matcher):
    found = list(movie_matcher.match("Person").where("_.name =~ 'K.*'").order_by("_.name").skip(2).limit(2))
    found_names = [actor["name"] for actor in found]
    assert found_names == ['Kelly Preston', 'Kevin Bacon']


def test_multiple_custom_conditions(movie_matcher):
    found = list(movie_matcher.match("Person").where("_.name =~ 'J.*'", "_.born >= 1960", "_.born < 1970"))
    found_names = {actor["name"] for actor in found}
    assert found_names == {'James Marshall', 'John Cusack', 'John Goodman', 'John C. Reilly', 'Julia Roberts'}


def test_one(movie_matcher):
    the_one = movie_matcher.match("Person").where("_.name =~ 'K.*'").order_by("_.name").first()
    assert the_one["name"] == 'Keanu Reeves'


def test_tuple_property_value(movie_matcher):
    found = list(movie_matcher.match("Person", name=("Kevin Bacon", "Kiefer Sutherland")))
    found_names = {actor["name"] for actor in found}
    assert found_names == {"Kevin Bacon", "Kiefer Sutherland"}


def test_set_property_value(movie_matcher):
    found = list(movie_matcher.match("Person", name={"Kevin Bacon", "Kiefer Sutherland"}))
    found_names = {actor["name"] for actor in found}
    assert found_names == {"Kevin Bacon", "Kiefer Sutherland"}


def test_frozenset_property_value(movie_matcher):
    found = list(movie_matcher.match("Person", name=frozenset(["Kevin Bacon", "Kiefer Sutherland"])))
    found_names = {actor["name"] for actor in found}
    assert found_names == {"Kevin Bacon", "Kiefer Sutherland"}


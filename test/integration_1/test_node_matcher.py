#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright 2011-2018, Nigel Small
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

from pytest import fixture

from py2neo.data import Node
from py2neo.matching import NodeMatcher


@fixture()
def movie_matcher(graph):
    graph.delete_all()
    with open(path_join(dirname(__file__), "..", "resources", "movies.cypher")) as f:
        cypher = f.read()
    graph.run(cypher)
    return NodeMatcher(graph)


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
    found = list(movie_matcher.match("Person").where(("_.name = {1}", {"1": "Keanu Reeves"})))
    assert len(found) == 1
    first = found[0]
    assert isinstance(first, Node)
    assert first["name"] == "Keanu Reeves"
    assert first["born"] == 1964


def test_special_parameters_gt(movie_matcher):
    year = 1985
    found = list(movie_matcher.match("Person", born__gt=year))
    assert found
    for actor in found:
        assert actor["born"] > year


def test_special_parameters_gte(movie_matcher):
    year = 1985
    found = list(movie_matcher.match("Person", born__gte=year))
    assert found
    for actor in found:
        assert actor["born"] >= year


def test_special_parameters_lt(movie_matcher):
    year = 1985
    found = list(movie_matcher.match("Person", born__lt=year))
    assert found
    for actor in found:
        assert actor["born"] < year


def test_special_parameters_lte(movie_matcher):
    year = 1985
    found = list(movie_matcher.match("Person", born__lte=year))
    assert found
    for actor in found:
        assert actor["born"] <= year


def test_special_parameters_exact(movie_matcher):
    year = 1985
    found = list(movie_matcher.match("Person", born__exact=year))
    assert found
    for actor in found:
        assert actor["born"] == year


def test_special_parameters_not(movie_matcher):
    year = 1985
    found = list(movie_matcher.match("Person", born__not=year))
    assert found
    for actor in found:
        assert actor["born"] != year


def test_special_parameters_regex(movie_matcher):
    found = list(movie_matcher.match("Person", name__regex='K.*'))
    found_names = {actor["name"] for actor in found}
    assert found_names == {'Keanu Reeves', 'Kelly McGillis', 'Kevin Bacon',
                           'Kevin Pollak', 'Kiefer Sutherland', 'Kelly Preston'}


def test_special_parameters_startswith(movie_matcher):
    found = list(movie_matcher.match("Person", name__startswith='K'))
    for actor in found:
        assert actor["name"].startswith("K")


def test_special_parameters_endswith(movie_matcher):
    found = list(movie_matcher.match("Person", name__endswith='eeves'))
    for actor in found:
        assert actor["name"].endswith("eeves")


def test_special_parameters_contains(movie_matcher):
    found = list(movie_matcher.match("Person", name__contains='shall'))
    for actor in found:
        assert "shall" in actor["name"]


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


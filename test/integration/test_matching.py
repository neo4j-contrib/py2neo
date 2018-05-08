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


from functools import reduce
from operator import or_
from os.path import join as path_join, dirname

from py2neo.matching import NodeMatcher
from py2neo.testing import IntegrationTestCase
from py2neo.data import Node, Relationship


class NodeMatcherTestCase(IntegrationTestCase):

    def setUp(self):
        self.graph.delete_all()
        with open(path_join(dirname(__file__), "..", "resources", "movies.cypher")) as f:
            cypher = f.read()
        self.graph.run(cypher)
        self.matcher = NodeMatcher(self.graph)

    def tearDown(self):
        self.graph.delete_all()

    def test_can_match_by_label_key_value(self):
        found = list(self.matcher.match("Person", name="Keanu Reeves"))
        assert len(found) == 1
        first = found[0]
        assert isinstance(first, Node)
        assert first["name"] == "Keanu Reeves"
        assert first["born"] == 1964

    def test_can_match_by_label_only(self):
        found = list(self.matcher.match("Person"))
        assert len(found) == 131

    def test_can_match_all_nodes(self):
        found = list(self.matcher.match())
        assert len(found) == 169

    def test_can_count_all_nodes(self):
        count = len(self.matcher.match())
        self.assertEqual(count, 169)

    def test_can_match_by_label_and_multiple_values(self):
        found = list(self.matcher.match("Person", name="Keanu Reeves", born=1964))
        assert len(found) == 1
        first = found[0]
        assert isinstance(first, Node)
        assert first["name"] == "Keanu Reeves"
        assert first["born"] == 1964

    def test_multiple_values_must_intersect(self):
        found = list(self.matcher.match("Person", name="Keanu Reeves", born=1963))
        assert len(found) == 0

    def test_custom_conditions(self):
        found = list(self.matcher.match("Person").where("_.name =~ 'K.*'"))
        found_names = {actor["name"] for actor in found}
        assert found_names == {'Keanu Reeves', 'Kelly McGillis', 'Kevin Bacon',
                               'Kevin Pollak', 'Kiefer Sutherland', 'Kelly Preston'}

    def test_custom_conditions_with_parameters(self):
        found = list(self.matcher.match("Person").where(("_.name = {1}", {"1": "Keanu Reeves"})))
        assert len(found) == 1
        first = found[0]
        assert isinstance(first, Node)
        assert first["name"] == "Keanu Reeves"
        assert first["born"] == 1964

    def test_order_by(self):
        found = list(self.matcher.match("Person").where("_.name =~ 'K.*'").order_by("_.name"))
        found_names = [actor["name"] for actor in found]
        assert found_names == ['Keanu Reeves', 'Kelly McGillis', 'Kelly Preston',
                               'Kevin Bacon', 'Kevin Pollak', 'Kiefer Sutherland']

    def test_skip(self):
        found = list(self.matcher.match("Person").where("_.name =~ 'K.*'").order_by("_.name").skip(2))
        found_names = [actor["name"] for actor in found]
        assert found_names == ['Kelly Preston', 'Kevin Bacon', 'Kevin Pollak', 'Kiefer Sutherland']

    def test_limit(self):
        found = list(self.matcher.match("Person").where("_.name =~ 'K.*'").order_by("_.name").skip(2).limit(2))
        found_names = [actor["name"] for actor in found]
        assert found_names == ['Kelly Preston', 'Kevin Bacon']

    def test_multiple_custom_conditions(self):
        found = list(self.matcher.match("Person").where("_.name =~ 'J.*'", "_.born >= 1960", "_.born < 1970"))
        found_names = {actor["name"] for actor in found}
        assert found_names == {'James Marshall', 'John Cusack', 'John Goodman', 'John C. Reilly', 'Julia Roberts'}

    def test_one(self):
        the_one = self.matcher.match("Person").where("_.name =~ 'K.*'").order_by("_.name").first()
        assert the_one["name"] == 'Keanu Reeves'

    def test_tuple_property_value(self):
        found = list(self.matcher.match("Person", name=("Kevin Bacon", "Kiefer Sutherland")))
        found_names = {actor["name"] for actor in found}
        assert found_names == {"Kevin Bacon", "Kiefer Sutherland"}

    def test_set_property_value(self):
        found = list(self.matcher.match("Person", name={"Kevin Bacon", "Kiefer Sutherland"}))
        found_names = {actor["name"] for actor in found}
        assert found_names == {"Kevin Bacon", "Kiefer Sutherland"}

    def test_frozenset_property_value(self):
        found = list(self.matcher.match("Person", name=frozenset(["Kevin Bacon", "Kiefer Sutherland"])))
        found_names = {actor["name"] for actor in found}
        assert found_names == {"Kevin Bacon", "Kiefer Sutherland"}


class RelationshipMatchNodeCombinationsTestCase(IntegrationTestCase):

    def setUp(self):
        TO = Relationship.type("TO")
        self.graph.delete_all()
        a = self.a = Node()
        b = self.b = Node()
        c = self.c = Node()
        d = self.d = Node()
        self.r = [TO(a, b), TO(b, a), TO(b, c), TO(b, b), TO(c, d), TO(a, d)]
        self.graph.create(reduce(or_, self.r))

    def test_a_to_b(self):
        match = self.graph.match(nodes=(self.a, self.b))
        self.assertEqual(len(match), 1)
        r = list(match)
        self.assertEqual(len(r), 1)
        self.assertSetEqual(set(r), {self.r[0]})

    def test_a_to_x(self):
        match = self.graph.match(nodes=(self.a, None))
        self.assertEqual(len(match), 2)
        r = list(match)
        self.assertEqual(len(r), 2)
        self.assertSetEqual(set(r), {self.r[0], self.r[5]})

    def test_x_to_b(self):
        match = self.graph.match(nodes=(None, self.b))
        self.assertEqual(len(match), 2)
        r = list(match)
        self.assertEqual(len(r), 2)
        self.assertSetEqual(set(r), {self.r[0], self.r[3]})

    def test_x_to_x(self):
        match = self.graph.match(nodes=(None, None))
        self.assertEqual(len(match), 6)
        r = list(match)
        self.assertEqual(len(r), 6)
        self.assertSetEqual(set(r), {self.r[0], self.r[1], self.r[2], self.r[3], self.r[4], self.r[5]})

    def test_a_and_b(self):
        match = self.graph.match(nodes={self.a, self.b})
        self.assertEqual(len(match), 2)
        r = list(match)
        self.assertEqual(len(r), 2)
        self.assertSetEqual(set(r), {self.r[0], self.r[1]})

    def test_a_only(self):
        match = self.graph.match(nodes={self.a})
        self.assertEqual(len(match), 3)
        r = list(match)
        self.assertEqual(len(r), 3)
        self.assertSetEqual(set(r), {self.r[0], self.r[1], self.r[5]})

    def test_b_only(self):
        match = self.graph.match(nodes={self.b})
        self.assertEqual(len(match), 4)
        r = list(match)
        self.assertEqual(len(r), 4)
        self.assertSetEqual(set(r), {self.r[0], self.r[1], self.r[2], self.r[3]})

    def test_any(self):
        match = self.graph.match(nodes=set())
        self.assertEqual(len(match), 6)
        r = list(match)
        self.assertEqual(len(r), 6)
        self.assertSetEqual(set(r), {self.r[0], self.r[1], self.r[2], self.r[3], self.r[4], self.r[5]})

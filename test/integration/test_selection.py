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

from py2neo.selection import NodeSelector
from py2neo.testing import IntegrationTestCase
from py2neo.data import Node


class NodeFinderTestCase(IntegrationTestCase):

    def setUp(self):
        self.graph.delete_all()
        with open(path_join(dirname(__file__), "..", "resources", "movies.cypher")) as f:
            cypher = f.read()
        self.graph.run(cypher)
        self.selector = NodeSelector(self.graph)

    def tearDown(self):
        self.graph.delete_all()

    def test_can_select_by_label_key_value(self):
        found = list(self.selector.select("Person", name="Keanu Reeves"))
        assert len(found) == 1
        first = found[0]
        assert isinstance(first, Node)
        assert first["name"] == "Keanu Reeves"
        assert first["born"] == 1964

    def test_can_select_by_label_only(self):
        found = list(self.selector.select("Person"))
        assert len(found) == 131

    def test_can_select_all_nodes(self):
        found = list(self.selector.select())
        assert len(found) == 169

    def test_can_count_all_nodes(self):
        count = list(self.selector.select().count())[0]
        assert count == 169

    def test_can_select_by_label_and_multiple_values(self):
        found = list(self.selector.select("Person", name="Keanu Reeves", born=1964))
        assert len(found) == 1
        first = found[0]
        assert isinstance(first, Node)
        assert first["name"] == "Keanu Reeves"
        assert first["born"] == 1964

    def test_multiple_values_must_intersect(self):
        found = list(self.selector.select("Person", name="Keanu Reeves", born=1963))
        assert len(found) == 0

    def test_custom_conditions(self):
        found = list(self.selector.select("Person").where("_.name =~ 'K.*'"))
        found_names = {actor["name"] for actor in found}
        assert found_names == {'Keanu Reeves', 'Kelly McGillis', 'Kevin Bacon',
                               'Kevin Pollak', 'Kiefer Sutherland', 'Kelly Preston'}

    def test_custom_conditions_with_parameters(self):
        found = list(self.selector.select("Person").where(("_.name = {1}", {"1": "Keanu Reeves"})))
        assert len(found) == 1
        first = found[0]
        assert isinstance(first, Node)
        assert first["name"] == "Keanu Reeves"
        assert first["born"] == 1964

    def test_order_by(self):
        found = list(self.selector.select("Person").where("_.name =~ 'K.*'").order_by("_.name"))
        found_names = [actor["name"] for actor in found]
        assert found_names == ['Keanu Reeves', 'Kelly McGillis', 'Kelly Preston',
                               'Kevin Bacon', 'Kevin Pollak', 'Kiefer Sutherland']

    def test_skip(self):
        found = list(self.selector.select("Person").where("_.name =~ 'K.*'").order_by("_.name").skip(2))
        found_names = [actor["name"] for actor in found]
        assert found_names == ['Kelly Preston', 'Kevin Bacon', 'Kevin Pollak', 'Kiefer Sutherland']

    def test_limit(self):
        found = list(self.selector.select("Person").where("_.name =~ 'K.*'").order_by("_.name").skip(2).limit(2))
        found_names = [actor["name"] for actor in found]
        assert found_names == ['Kelly Preston', 'Kevin Bacon']

    def test_multiple_custom_conditions(self):
        found = list(self.selector.select("Person").where("_.name =~ 'J.*'", "_.born >= 1960", "_.born < 1970"))
        found_names = {actor["name"] for actor in found}
        assert found_names == {'James Marshall', 'John Cusack', 'John Goodman', 'John C. Reilly', 'Julia Roberts'}

    def test_one(self):
        the_one = self.selector.select("Person").where("_.name =~ 'K.*'").order_by("_.name").first()
        assert the_one["name"] == 'Keanu Reeves'

    def test_tuple_property_value(self):
        found = list(self.selector.select("Person", name=("Kevin Bacon", "Kiefer Sutherland")))
        found_names = {actor["name"] for actor in found}
        assert found_names == {"Kevin Bacon", "Kiefer Sutherland"}

    def test_set_property_value(self):
        found = list(self.selector.select("Person", name={"Kevin Bacon", "Kiefer Sutherland"}))
        found_names = {actor["name"] for actor in found}
        assert found_names == {"Kevin Bacon", "Kiefer Sutherland"}

    def test_frozenset_property_value(self):
        found = list(self.selector.select("Person", name=frozenset(["Kevin Bacon", "Kiefer Sutherland"])))
        found_names = {actor["name"] for actor in found}
        assert found_names == {"Kevin Bacon", "Kiefer Sutherland"}

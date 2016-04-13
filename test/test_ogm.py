#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright 2011-2016, Nigel Small
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
from py2neo.ogm import GraphObject, Property, Label, Related
from test.util import Py2neoTestCase


class Person(GraphObject):
    __primary_key__ = "name"

    name = Property()
    year_of_birth = Property(key="born")

    acted_in = Related("Movie")
    directed = Related("Movie")
    produced = Related("Movie")


class Movie(GraphObject):
    __primary_key__ = "title"

    title = Property()
    tag_line = Property(key="tagline")
    year_of_release = Property(key="released")


class LoadTestCase(Py2neoTestCase):

    def setUp(self):
        Person.__graph__ = self.graph
        Movie.__graph__ = self.graph
        self.graph.delete_all()
        with open(path_join(dirname(__file__), "..", "resources", "movies.cypher")) as f:
            cypher = f.read()
        self.graph.run(cypher)

    def test_can_load_object(self):
        keanu = Person.load("Keanu Reeves")
        assert keanu.name == "Keanu Reeves"
        assert keanu.year_of_birth == 1964

    # def test_can_load_related_objects(self):
    #     keanu = Person.load("Keanu Reeves")
    #     keanu_movies = keanu.acted_in
    #     assert keanu.name == "Keanu Reeves"
    #     assert keanu.year_of_birth == 1964

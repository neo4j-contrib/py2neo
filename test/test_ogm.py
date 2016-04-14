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

""" OGM Tests

Fundamental entities are:
- GraphObject (node)
- RelationshipSet

# Load, update and push an object
keanu = Person.load("Keanu Reeves")
keanu.name = "Keanu John Reeves"
graph.push(keanu)

# Load, update and push an object and its relationships
keanu = Person.load("Keanu Reeves")
keanu.name = "Keanu John Reeves"
keanu.acted_in.add(Movie("Bill & Ted 3"), roles=['Ted "Theodore" Logan'])
keanu.acted_in.remove(Movie("The Matrix Reloaded"))
graph.push(keanu | keanu.acted_in)

nigel = Person("Nigel Small")
graph.push(nigel)

graph.delete(nigel)
"""


from os.path import join as path_join, dirname
from py2neo.ogm import GraphObject, Property, Label, Related
from test.util import Py2neoTestCase


class MovieGraphObject(GraphObject):
    pass


class Person(MovieGraphObject):
    __primary_key__ = "name"

    name = Property()
    year_of_birth = Property(key="born")

    acted_in = Related("Movie")
    directed = Related("Movie")
    produced = Related("Movie")


class Movie(MovieGraphObject):
    __primary_key__ = "title"

    title = Property()
    tag_line = Property(key="tagline")
    year_of_release = Property(key="released")


class LoadTestCase(Py2neoTestCase):

    def setUp(self):
        MovieGraphObject.__graph__ = self.graph
        self.graph.delete_all()
        with open(path_join(dirname(__file__), "..", "resources", "movies.cypher")) as f:
            cypher = f.read()
        self.graph.run(cypher)

    def test_can_load_object(self):
        keanu = Person.load("Keanu Reeves")  # load = attach + pull
        assert keanu.name == "Keanu Reeves"
        assert keanu.year_of_birth == 1964

    def test_can_load_related(self):
        keanu = Person.load("Keanu Reeves")
        matrix = Movie.load("The Matrix")
        keanu_movies = keanu.acted_in  # auto-pull if stale
        assert matrix in keanu_movies

    def test_can_pull_related(self):
        keanu = Person.load("Keanu Reeves")
        matrix = Movie.load("The Matrix")
        keanu_movies = keanu.acted_in.pull()  # auto-pull if stale
        assert matrix in keanu_movies

        self.graph.push(keanu)
        self.graph.push(keanu.acted_in)

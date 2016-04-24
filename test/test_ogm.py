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
from unittest import TestCase

from py2neo import order, size
from py2neo.ogm import GraphObject, Label, Property, Related
from test.util import GraphTestCase


class MovieGraphObject(GraphObject):
    pass


class Person(MovieGraphObject):
    __primarykey__ = "name"

    name = Property()
    year_of_birth = Property(key="born")

    acted_in = Related("Film")
    directed = Related("Film")
    produced = Related("Film")


class Film(MovieGraphObject):
    __primarylabel__ = "Movie"
    __primarykey__ = "title"

    awesome = Label()
    musical = Label()
    science_fiction = Label(name="SciFi")

    title = Property()
    tag_line = Property(key="tagline")
    year_of_release = Property(key="released")

    def __init__(self, title):
        self.title = title


class MacGuffin(MovieGraphObject):
    pass


class SubclassTestCase(TestCase):

    def test_class_primary_label_defaults_to_class_name(self):
        assert MacGuffin.__primarylabel__ == "MacGuffin"

    def test_class_primary_label_can_be_overridden(self):
        assert Film.__primarylabel__ == "Movie"

    def test_class_primary_key_defaults_to_id(self):
        assert MacGuffin.__primarykey__ == "__id__"

    def test_class_primary_key_can_be_overridden(self):
        assert Film.__primarykey__ == "title"


class InstanceTestCase(TestCase):

    def setUp(self):
        self.macguffin = MacGuffin()
        self.film = Film("Die Hard")

    def test_instance_primary_label_defaults_to_class_name(self):
        assert self.macguffin.__primarylabel__ == "MacGuffin"

    def test_instance_primary_label_can_be_overridden(self):
        assert self.film.__primarylabel__ == "Movie"

    def test_instance_primary_key_defaults_to_id(self):
        assert self.macguffin.__primarykey__ == "__id__"

    def test_instance_primary_key_can_be_overridden(self):
        assert self.film.__primarykey__ == "title"


class InstanceSubgraphTestCase(TestCase):

    def setUp(self):
        self.film = Film("Die Hard")
        self.film_node = self.film.__subgraph__

    def test_instance_subgraph_is_node_like(self):
        assert order(self.film_node) == 1
        assert size(self.film_node) == 0

    def test_instance_subgraph_inherits_primary_label(self):
        assert self.film_node.__primarylabel__ == "Movie"

    def test_instance_subgraph_inherits_primary_key(self):
        assert self.film_node.__primarykey__ == "title"


class InstanceLabelTestCase(TestCase):

    def setUp(self):
        self.film = Film("Die Hard")
        self.film.awesome = True
        self.film.science_fiction = True
        self.film_node = self.film.__subgraph__

    def test_instance_label_name_defaults_to_attribute_name_variant(self):
        assert self.film_node.has_label("Awesome")

    def test_instance_label_name_can_be_overridden(self):
        assert self.film_node.has_label("SciFi")
        assert not self.film_node.has_label("ScienceFiction")

    def test_instance_label_defaults_to_absent(self):
        assert not self.film_node.has_label("Musical")


class InstancePropertyTestCase(TestCase):

    def setUp(self):
        self.film = Film("Die Hard")
        self.film.year_of_release = 1988
        self.film_node = self.film.__subgraph__

    def test_instance_property_key_defaults_to_attribute_name(self):
        assert "title" in self.film_node

    def test_instance_property_key_can_be_overridden(self):
        assert "released" in self.film_node
        assert "year_of_release" not in self.film_node


class MovieGraphTestCase(GraphTestCase):

    def setUp(self):
        MovieGraphObject.__graph__ = self.graph
        self.graph.delete_all()
        with open(path_join(dirname(__file__), "..", "resources", "movies.cypher")) as f:
            cypher = f.read()
        self.graph.run(cypher)

    def tearDown(self):
        self.graph.delete_all()


class LoadOneTestCase(MovieGraphTestCase):

    def test_can_load_one_object(self):
        keanu = Person.load_one("Keanu Reeves")
        assert keanu.name == "Keanu Reeves"
        assert keanu.year_of_birth == 1964

#     def test_can_load_related(self):
#         keanu = Person.load("Keanu Reeves")
#         matrix = Movie.load("The Matrix")
#         keanu_movies = keanu.acted_in  # auto-pull if stale
#         assert matrix in keanu_movies
#
#     def test_can_pull_related(self):
#         keanu = Person.load("Keanu Reeves")
#         matrix = Movie.load("The Matrix")
#         keanu_movies = keanu.acted_in.pull()  # auto-pull if stale
#         assert matrix in keanu_movies
#
#         self.graph.push(keanu)
#         self.graph.push(keanu.acted_in)


class LoadTestCase(MovieGraphTestCase):

    def test_can_load_multiple_objects(self):
        keanu, hugo = list(Person.load(["Keanu Reeves", "Hugo Weaving"]))
        assert keanu.name == "Keanu Reeves"
        assert keanu.year_of_birth == 1964
        assert hugo.name == "Hugo Weaving"
        assert hugo.year_of_birth == 1960

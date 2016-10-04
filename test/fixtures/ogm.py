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

from py2neo.ogm import GraphObject, Label, Property, RelatedTo, RelatedFrom
from test.util import GraphTestCase


class MovieGraphObject(GraphObject):
    pass


class Film(MovieGraphObject):
    __primarylabel__ = "Movie"
    __primarykey__ = "title"

    awesome = Label()
    musical = Label()
    science_fiction = Label(name="SciFi")

    title = Property()
    tag_line = Property(key="tagline")
    year_of_release = Property(key="released")

    actors = RelatedFrom("Person", "ACTED_IN")

    def __init__(self, title):
        self.title = title


class Person(MovieGraphObject):
    __primarykey__ = "name"

    name = Property()
    year_of_birth = Property(key="born")

    acted_in = RelatedTo(Film)
    directed = RelatedTo("Film")
    produced = RelatedTo("test.fixtures.ogm.Film")

    def __hash__(self):
        return hash(self.name)


class MacGuffin(MovieGraphObject):
    pass


class MovieGraphTestCase(GraphTestCase):

    def setUp(self):
        self.graph.delete_all()
        with open(path_join(dirname(__file__), "..", "..", "resources", "movies.cypher")) as f:
            cypher = f.read()
        self.graph.run(cypher)

    def tearDown(self):
        self.graph.delete_all()


class BaseThing(GraphObject):
    __primarylabel__ = "MyLabel"
    __primarykey__ = "my_key"


class DerivedThing(BaseThing):
    pass


class Car(GraphObject):
    model = Property()

class House(GraphObject):
    address = Property()

class Human(GraphObject):
    name = Property()
    Owns = RelatedTo(["Car", "House"])
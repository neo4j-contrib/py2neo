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


from unittest import TestCase

from py2neo import order, size, remote, Node, Relationship
from py2neo.ogm import RelatedObjects

from test.fixtures.ogm import MovieGraphTestCase, Person, Film, MacGuffin


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
        self.film_node = self.film.__cog__.subject_node

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
        self.film_node = self.film.__cog__.subject_node

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
        self.film_node = self.film.__cog__.subject_node

    def test_instance_property_key_defaults_to_attribute_name(self):
        assert "title" in self.film_node

    def test_instance_property_key_can_be_overridden(self):
        assert "released" in self.film_node
        assert "year_of_release" not in self.film_node


class InstanceRelatedObjectTestCase(MovieGraphTestCase):

    def test_related_objects_are_automatically_loaded(self):
        keanu = Person.find_one("Keanu Reeves")
        film_titles = set(film.title for film in list(keanu.acted_in))
        assert film_titles == {"The Devil's Advocate", 'The Matrix Reloaded', "Something's Gotta Give",
                               'The Matrix', 'The Replacements', 'The Matrix Revolutions', 'Johnny Mnemonic'}

    def test_can_add_related_object_and_push(self):
        keanu = Person.find_one("Keanu Reeves")
        bill_and_ted = Film("Bill & Ted's Excellent Adventure")
        keanu.acted_in.add(bill_and_ted)
        self.graph.push(keanu)
        remote_node = remote(keanu.__cog__.subject_node)
        film_titles = set(title for title, in self.graph.run("MATCH (a:Person)-[:ACTED_IN]->(b) "
                                                             "WHERE id(a) = {x} "
                                                             "RETURN b.title", x=remote_node._id))
        assert film_titles == {"The Devil's Advocate", 'The Matrix Reloaded', "Something's Gotta Give",
                               'The Matrix', 'The Replacements', 'The Matrix Revolutions', 'Johnny Mnemonic',
                               "Bill & Ted's Excellent Adventure"}

    def test_can_add_related_object_with_properties_and_push(self):
        keanu = Person.find_one("Keanu Reeves")
        bill_and_ted = Film("Bill & Ted's Excellent Adventure")
        keanu.acted_in.add(bill_and_ted, roles=['Ted "Theodore" Logan'])
        self.graph.push(keanu)
        remote_node = remote(keanu.__cog__.subject_node)
        films = {title: roles for title, roles in self.graph.run("MATCH (a:Person)-[ab:ACTED_IN]->(b) "
                                                                 "WHERE id(a) = {x} "
                                                                 "RETURN b.title, ab.roles", x=remote_node._id)}
        bill_and_ted_roles = films["Bill & Ted's Excellent Adventure"]
        assert bill_and_ted_roles == ['Ted "Theodore" Logan']

    def test_can_remove_related_object_and_push(self):
        keanu = Person.find_one("Keanu Reeves")
        johnny_mnemonic = Film.find_one("Johnny Mnemonic")
        keanu.acted_in.remove(johnny_mnemonic)
        self.graph.push(keanu)
        remote_node = remote(keanu.__cog__.subject_node)
        film_titles = set(title for title, in self.graph.run("MATCH (a:Person)-[:ACTED_IN]->(b) "
                                                             "WHERE id(a) = {x} "
                                                             "RETURN b.title", x=remote_node._id))
        assert film_titles == {"The Devil's Advocate", 'The Matrix Reloaded', "Something's Gotta Give",
                               'The Matrix', 'The Replacements', 'The Matrix Revolutions'}

    def test_can_add_property_to_existing_relationship(self):
        keanu = Person.find_one("Keanu Reeves")
        johnny_mnemonic = Film.find_one("Johnny Mnemonic")
        keanu.acted_in.add(johnny_mnemonic, foo="bar")
        self.graph.push(keanu)
        remote_node = remote(keanu.__cog__.subject_node)
        johnny_foo = self.graph.evaluate("MATCH (a:Person)-[ab:ACTED_IN]->(b) "
                                         "WHERE id(a) = {x} AND b.title = 'Johnny Mnemonic' "
                                         "RETURN ab.foo", x=remote_node._id)
        assert johnny_foo == "bar"


class FindTestCase(MovieGraphTestCase):

    def test_can_find_one_object(self):
        keanu = Person.find_one("Keanu Reeves")
        assert keanu.name == "Keanu Reeves"
        assert keanu.year_of_birth == 1964

    def test_cannot_find_one_that_does_not_exist(self):
        keanu = Person.find_one("Keanu Jones")
        assert keanu is None

    def test_can_find_multiple_objects(self):
        keanu, hugo = list(Person.find(["Keanu Reeves", "Hugo Weaving"]))
        assert keanu.name == "Keanu Reeves"
        assert keanu.year_of_birth == 1964
        assert hugo.name == "Hugo Weaving"
        assert hugo.year_of_birth == 1960


# class CreateTestCase(MovieGraphTestCase):
#
#     def test_create_has_no_effect_on_existing(self):
#         # given
#         keanu = Person.find_one("Keanu Reeves")
#
#         # when
#         keanu.name = "Keanu Charles Reeves"
#         self.graph.create(keanu)
#
#         # then
#         remote_node = remote(keanu.__cog__.subject_node)
#         remote_name = self.graph.evaluate("MATCH (a:Person) WHERE id(a) = {x} "
#                                           "RETURN a.name", x=remote_node._id)
#         assert remote_name == "Keanu Reeves"


class PushTestCase(MovieGraphTestCase):

    def test_can_push_changes_to_existing(self):
        # given
        keanu = Person.find_one("Keanu Reeves")

        # when
        keanu.name = "Keanu Charles Reeves"
        self.graph.push(keanu)

        # then
        remote_node = remote(keanu.__cog__.subject_node)
        remote_name = self.graph.evaluate("MATCH (a:Person) WHERE id(a) = {x} "
                                          "RETURN a.name", x=remote_node._id)
        assert remote_name == "Keanu Charles Reeves"

    def test_can_push_new(self):
        # given
        alice = Person()
        alice.name = "Alice Smith"
        alice.year_of_birth = 1970

        # when
        self.graph.push(alice)

        # then
        remote_node = remote(alice.__cog__.subject_node)
        remote_name = self.graph.evaluate("MATCH (a:Person) WHERE id(a) = {x} "
                                          "RETURN a.name", x=remote_node._id)
        assert remote_name == "Alice Smith"

    def test_can_push_new_that_points_to_existing(self):
        # given
        alice = Person()
        alice.name = "Alice Smith"
        alice.year_of_birth = 1970
        alice.acted_in.add(Film.find_one("The Matrix"))

        # when
        self.graph.push(alice)

        # then
        remote_node = remote(alice.__cog__.subject_node)
        film_node = self.graph.evaluate("MATCH (a:Person)-[:ACTED_IN]->(b) WHERE id(a) = {x} RETURN b",
                                        x=remote_node._id)
        assert film_node["title"] == "The Matrix"
        assert film_node["tagline"] == "Welcome to the Real World"

    def test_can_push_new_that_points_to_new(self):
        # given
        the_dominatrix = Film("The Dominatrix")
        alice = Person()
        alice.name = "Alice Smith"
        alice.year_of_birth = 1970
        alice.acted_in.add(the_dominatrix)

        # when
        self.graph.push(alice)

        # then
        remote_node = remote(alice.__cog__.subject_node)
        film_node = self.graph.evaluate("MATCH (a:Person)-[:ACTED_IN]->(b) WHERE id(a) = {x} RETURN b",
                                        x=remote_node._id)
        assert film_node["title"] == "The Dominatrix"


class PullTestCase(MovieGraphTestCase):

    def test_can_load_and_pull(self):
        keanu = Person.find_one("Keanu Reeves")
        assert keanu.name == "Keanu Reeves"
        remote_node = remote(keanu.__cog__.subject_node)
        self.graph.run("MATCH (a:Person) WHERE id(a) = {x} SET a.name = {y}",
                       x=remote_node._id, y="Keanu Charles Reeves")
        self.graph.pull(keanu)
        assert keanu.name == "Keanu Charles Reeves"


class RelatedObjectsTestCase(MovieGraphTestCase):

    def new_keanu_acted_in(self):
        keanu_node = self.graph.find_one("Person", "name", "Keanu Reeves")
        keanu_acted_in = RelatedObjects(keanu_node, "ACTED_IN", Film)
        return keanu_acted_in

    def test_can_pull_related_objects(self):
        films_acted_in = self.new_keanu_acted_in()
        films_acted_in.__db_pull__(self.graph)
        film_titles = set(film.title for film in films_acted_in)
        assert film_titles == {"The Devil's Advocate", 'The Matrix Reloaded',
                               "Something's Gotta Give", 'The Matrix', 'The Replacements',
                               'The Matrix Revolutions', 'Johnny Mnemonic'}

    def test_can_add_object(self):
        films_acted_in = self.new_keanu_acted_in()
        films_acted_in.__db_pull__(self.graph)
        bill_and_ted = Film("Bill & Ted's Excellent Adventure")
        films_acted_in.add(bill_and_ted)
        film_titles = set(film.title for film in films_acted_in)
        assert film_titles == {"The Devil's Advocate", 'The Matrix Reloaded',
                               "Something's Gotta Give", 'The Matrix', 'The Replacements',
                               'The Matrix Revolutions', 'Johnny Mnemonic', "Bill & Ted's Excellent Adventure"}

    def test_can_add_object_when_already_present(self):
        films_acted_in = self.new_keanu_acted_in()
        films_acted_in.__db_pull__(self.graph)
        bill_and_ted = Film("Bill & Ted's Excellent Adventure")
        films_acted_in.add(bill_and_ted)
        films_acted_in.add(bill_and_ted)
        film_titles = set(film.title for film in films_acted_in)
        assert film_titles == {"The Devil's Advocate", 'The Matrix Reloaded',
                               "Something's Gotta Give", 'The Matrix', 'The Replacements',
                               'The Matrix Revolutions', 'Johnny Mnemonic', "Bill & Ted's Excellent Adventure"}

    def test_can_remove_object(self):
        # given
        films_acted_in = self.new_keanu_acted_in()
        films_acted_in.__db_pull__(self.graph)

        # when
        matrix_reloaded = Film.find_one("The Matrix Reloaded")
        films_acted_in.remove(matrix_reloaded)

        # then
        film_titles = set(film.title for film in films_acted_in)
        assert film_titles == {"The Devil's Advocate",
                               "Something's Gotta Give", 'The Matrix', 'The Replacements',
                               'The Matrix Revolutions', 'Johnny Mnemonic'}

    def test_can_remove_object_when_already_absent(self):
        # given
        films_acted_in = self.new_keanu_acted_in()
        films_acted_in.__db_pull__(self.graph)
        matrix_reloaded = Film.find_one("The Matrix Reloaded")
        films_acted_in.remove(matrix_reloaded)

        # when
        films_acted_in.remove(matrix_reloaded)

        # then
        film_titles = set(film.title for film in films_acted_in)
        assert film_titles == {"The Devil's Advocate",
                               "Something's Gotta Give", 'The Matrix', 'The Replacements',
                               'The Matrix Revolutions', 'Johnny Mnemonic'}

    def test_can_push_object_additions(self):
        # given
        films_acted_in = self.new_keanu_acted_in()
        films_acted_in.__db_pull__(self.graph)

        # when
        bill_and_ted = Film("Bill & Ted's Excellent Adventure")
        films_acted_in.add(bill_and_ted)
        films_acted_in.__db_push__(self.graph)

        # then
        del films_acted_in
        films_acted_in = self.new_keanu_acted_in()
        films_acted_in.__db_pull__(self.graph)
        film_titles = set(film.title for film in films_acted_in)
        assert film_titles == {"The Devil's Advocate", 'The Matrix Reloaded',
                               "Something's Gotta Give", 'The Matrix', 'The Replacements',
                               'The Matrix Revolutions', 'Johnny Mnemonic', "Bill & Ted's Excellent Adventure"}

    def test_can_push_object_removals(self):
        # given
        films_acted_in = self.new_keanu_acted_in()
        films_acted_in.__db_pull__(self.graph)

        # when
        matrix_reloaded = Film('The Matrix Reloaded')
        films_acted_in.remove(matrix_reloaded)
        films_acted_in.__db_push__(self.graph)

        # then
        del films_acted_in
        Node.cache.clear()
        Relationship.cache.clear()
        films_acted_in = self.new_keanu_acted_in()
        films_acted_in.__db_pull__(self.graph)
        film_titles = set(film.title for film in films_acted_in)
        assert film_titles == {"The Devil's Advocate",
                               "Something's Gotta Give", 'The Matrix', 'The Replacements',
                               'The Matrix Revolutions', 'Johnny Mnemonic'}

    def test_can_get_relationship_property(self):
        # given
        films_acted_in = self.new_keanu_acted_in()
        films_acted_in.__db_pull__(self.graph)
        matrix_reloaded = Film('The Matrix Reloaded')

        # then
        roles = films_acted_in.get(matrix_reloaded, "roles")
        assert roles == ["Neo"]

    def test_can_push_property_additions(self):
        # given
        films_acted_in = self.new_keanu_acted_in()
        films_acted_in.__db_pull__(self.graph)

        # when
        matrix = Film("The Matrix")
        films_acted_in.update(matrix, good=True)
        films_acted_in.__db_push__(self.graph)

        # then
        del films_acted_in
        Node.cache.clear()
        Relationship.cache.clear()
        films_acted_in = self.new_keanu_acted_in()
        films_acted_in.__db_pull__(self.graph)
        good = films_acted_in.get(matrix, "good")
        assert good

    def test_can_push_property_removals(self):
        # given
        films_acted_in = self.new_keanu_acted_in()
        films_acted_in.__db_pull__(self.graph)

        # when
        matrix = Film("The Matrix")
        films_acted_in.update(matrix, roles=None)
        films_acted_in.__db_push__(self.graph)

        # then
        del films_acted_in
        Node.cache.clear()
        Relationship.cache.clear()
        films_acted_in = self.new_keanu_acted_in()
        films_acted_in.__db_pull__(self.graph)
        roles = films_acted_in.get(matrix, "roles")
        assert roles is None

    def test_can_push_property_updates(self):
        # given
        films_acted_in = self.new_keanu_acted_in()
        films_acted_in.__db_pull__(self.graph)

        # when
        matrix = Film("The Matrix")
        films_acted_in.update(matrix, roles=1)
        films_acted_in.__db_push__(self.graph)

        # then
        del films_acted_in
        Node.cache.clear()
        Relationship.cache.clear()
        films_acted_in = self.new_keanu_acted_in()
        films_acted_in.__db_pull__(self.graph)
        roles = films_acted_in.get(matrix, "roles")
        assert roles == 1


class SingleCogTestCase(MovieGraphTestCase):

    @classmethod
    def new_keanu_cog(cls):
        keanu = Person.find_one("Keanu Reeves")
        return keanu.__cog__

    def test_can_pull_related_objects(self):
        # given
        keanu_cog = self.new_keanu_cog()

        # when
        keanu_cog.__db_pull__(self.graph)

        # then
        film_titles = set(film.title for film in keanu_cog.related["ACTED_IN"])
        assert film_titles == {"The Devil's Advocate", 'The Matrix Reloaded',
                               "Something's Gotta Give", 'The Matrix', 'The Replacements',
                               'The Matrix Revolutions', 'Johnny Mnemonic'}

    def test_can_add_object(self):
        # given
        keanu_cog = self.new_keanu_cog()
        keanu_cog.__db_pull__(self.graph)

        # when
        bill_and_ted = Film("Bill & Ted's Excellent Adventure")
        keanu_cog.add("ACTED_IN", bill_and_ted)

        # then
        film_titles = set(film.title for film in keanu_cog.related["ACTED_IN"])
        assert film_titles == {"The Devil's Advocate", 'The Matrix Reloaded',
                               "Something's Gotta Give", 'The Matrix', 'The Replacements',
                               'The Matrix Revolutions', 'Johnny Mnemonic', "Bill & Ted's Excellent Adventure"}

    def test_can_add_object_when_already_present(self):
        # given
        keanu_cog = self.new_keanu_cog()
        keanu_cog.__db_pull__(self.graph)

        # and
        bill_and_ted = Film("Bill & Ted's Excellent Adventure")
        keanu_cog.add("ACTED_IN", bill_and_ted)

        # when
        keanu_cog.add("ACTED_IN", bill_and_ted)

        # then
        film_titles = set(film.title for film in keanu_cog.related["ACTED_IN"])
        assert film_titles == {"The Devil's Advocate", 'The Matrix Reloaded',
                               "Something's Gotta Give", 'The Matrix', 'The Replacements',
                               'The Matrix Revolutions', 'Johnny Mnemonic', "Bill & Ted's Excellent Adventure"}

    def test_can_remove_object(self):
        # given
        keanu_cog = self.new_keanu_cog()
        keanu_cog.__db_pull__(self.graph)

        # when
        matrix_reloaded = Film.find_one("The Matrix Reloaded")
        keanu_cog.remove("ACTED_IN", matrix_reloaded)

        # then
        film_titles = set(film.title for film in keanu_cog.related["ACTED_IN"])
        assert film_titles == {"The Devil's Advocate",
                               "Something's Gotta Give", 'The Matrix', 'The Replacements',
                               'The Matrix Revolutions', 'Johnny Mnemonic'}

    def test_can_remove_object_when_already_absent(self):
        # given
        keanu_cog = self.new_keanu_cog()
        keanu_cog.__db_pull__(self.graph)

        # and
        matrix_reloaded = Film.find_one("The Matrix Reloaded")
        keanu_cog.remove("ACTED_IN", matrix_reloaded)

        # when
        keanu_cog.remove("ACTED_IN", matrix_reloaded)

        # then
        film_titles = set(film.title for film in keanu_cog.related["ACTED_IN"])
        assert film_titles == {"The Devil's Advocate",
                               "Something's Gotta Give", 'The Matrix', 'The Replacements',
                               'The Matrix Revolutions', 'Johnny Mnemonic'}

    def test_can_push_object_additions(self):
        # given
        keanu_cog = self.new_keanu_cog()
        keanu_cog.__db_pull__(self.graph)

        # when
        bill_and_ted = Film("Bill & Ted's Excellent Adventure")
        keanu_cog.add("ACTED_IN", bill_and_ted)
        keanu_cog.__db_push__(self.graph)

        # then
        del keanu_cog
        keanu_cog = self.new_keanu_cog()
        keanu_cog.__db_pull__(self.graph)
        film_titles = set(film.title for film in keanu_cog.related["ACTED_IN"])
        assert film_titles == {"The Devil's Advocate", 'The Matrix Reloaded',
                               "Something's Gotta Give", 'The Matrix', 'The Replacements',
                               'The Matrix Revolutions', 'Johnny Mnemonic', "Bill & Ted's Excellent Adventure"}

    def test_can_push_object_removals(self):
        # given
        keanu_cog = self.new_keanu_cog()
        keanu_cog.__db_pull__(self.graph)

        # when
        matrix_reloaded = Film('The Matrix Reloaded')
        keanu_cog.remove("ACTED_IN", matrix_reloaded)
        keanu_cog.__db_push__(self.graph)

        # then
        del keanu_cog
        Node.cache.clear()
        Relationship.cache.clear()
        keanu_cog = self.new_keanu_cog()
        keanu_cog.__db_pull__(self.graph)
        film_titles = set(film.title for film in keanu_cog.related["ACTED_IN"])
        assert film_titles == {"The Devil's Advocate",
                               "Something's Gotta Give", 'The Matrix', 'The Replacements',
                               'The Matrix Revolutions', 'Johnny Mnemonic'}

    def test_can_get_relationship_property(self):
        # given
        keanu_cog = self.new_keanu_cog()
        keanu_cog.__db_pull__(self.graph)
        matrix_reloaded = Film('The Matrix Reloaded')

        # then
        roles = keanu_cog.get("ACTED_IN", matrix_reloaded, "roles")
        assert roles == ["Neo"]

    def test_can_push_property_additions(self):
        # given
        keanu_cog = self.new_keanu_cog()
        keanu_cog.__db_pull__(self.graph)

        # when
        matrix = Film("The Matrix")
        keanu_cog.update("ACTED_IN", matrix, good=True)
        keanu_cog.__db_push__(self.graph)

        # then
        del keanu_cog
        Node.cache.clear()
        Relationship.cache.clear()
        keanu_cog = self.new_keanu_cog()
        keanu_cog.__db_pull__(self.graph)
        good = keanu_cog.get("ACTED_IN", matrix, "good")
        assert good

    def test_can_push_property_removals(self):
        # given
        keanu_cog = self.new_keanu_cog()
        keanu_cog.__db_pull__(self.graph)

        # when
        matrix = Film("The Matrix")
        keanu_cog.update("ACTED_IN", matrix, roles=None)
        keanu_cog.__db_push__(self.graph)

        # then
        del keanu_cog
        Node.cache.clear()
        Relationship.cache.clear()
        keanu_cog = self.new_keanu_cog()
        keanu_cog.__db_pull__(self.graph)
        roles = keanu_cog.get("ACTED_IN", matrix, "roles")
        assert roles is None

    def test_can_push_property_updates(self):
        # given
        keanu_cog = self.new_keanu_cog()
        keanu_cog.__db_pull__(self.graph)

        # when
        matrix = Film("The Matrix")
        keanu_cog.update("ACTED_IN", matrix, roles=1)
        keanu_cog.__db_push__(self.graph)

        # then
        del keanu_cog
        Node.cache.clear()
        Relationship.cache.clear()
        keanu_cog = self.new_keanu_cog()
        keanu_cog.__db_pull__(self.graph)
        roles = keanu_cog.get("ACTED_IN", matrix, "roles")
        assert roles == 1

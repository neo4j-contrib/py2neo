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


from py2neo import Node, Relationship
from py2neo.ogm import RelatedObjects

from test.fixtures.ogm import MovieGraphTestCase, Person, Film


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

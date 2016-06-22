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


from os.path import dirname, join as path_join
from unittest import TestCase

from py2neo import order, size, remote, Node, Relationship, NodeSelector
from py2neo.ogm import RelatedObjects, Property, Related, RelatedTo, RelatedFrom, OUTGOING, GraphObject, Label

from test.fixtures.ogm import MovieGraphTestCase, Person, Film, MacGuffin, MovieGraphObject
from test.util import GraphTestCase


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

    def test_instance_repr(self):
        assert repr(self.film).startswith("<Film")

    def test_instance_not_equal_to_non_graph_object(self):
        assert self.film != "this is not a graph object"


class InstanceSubgraphTestCase(TestCase):

    def setUp(self):
        self.film = Film("Die Hard")
        self.film_node = self.film.__ogm__.node

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
        self.film_node = self.film.__ogm__.node

    def test_instance_label_name_defaults_to_attribute_name_variant(self):
        assert self.film_node.has_label("Awesome")

    def test_instance_label_name_can_be_overridden(self):
        assert self.film_node.has_label("SciFi")
        assert not self.film_node.has_label("ScienceFiction")

    def test_instance_label_defaults_to_absent(self):
        assert not self.film_node.has_label("Musical")

    def test_setting_to_false_removes_label(self):
        self.film.awesome = False
        assert not self.film_node.has_label("Awesome")


class InstancePropertyTestCase(TestCase):

    def setUp(self):
        self.film = Film("Die Hard")
        self.film.year_of_release = 1988
        self.film_node = self.film.__ogm__.node

    def test_instance_property_key_defaults_to_attribute_name(self):
        assert "title" in self.film_node

    def test_instance_property_key_can_be_overridden(self):
        assert "released" in self.film_node
        assert "year_of_release" not in self.film_node


class InstanceRelatedObjectTestCase(MovieGraphTestCase):

    def test_related_objects_are_automatically_loaded(self):
        keanu = Person.select(self.graph, "Keanu Reeves").first()
        film_titles = set(film.title for film in list(keanu.acted_in))
        assert film_titles == {"The Devil's Advocate", 'The Matrix Reloaded', "Something's Gotta Give",
                               'The Matrix', 'The Replacements', 'The Matrix Revolutions', 'Johnny Mnemonic'}

    def test_graph_propagation(self):
        keanu = Person.select(self.graph, "Keanu Reeves").first()
        films = list(keanu.acted_in)
        colleagues = set()
        for film in films:
            colleagues |= set(film.actors)
        names = set(colleague.name for colleague in colleagues)
        expected_names = {'Al Pacino', 'Dina Meyer', 'Keanu Reeves', 'Brooke Langton', 'Hugo Weaving', 'Diane Keaton',
                          'Takeshi Kitano', 'Laurence Fishburne', 'Charlize Theron', 'Emil Eifrem', 'Orlando Jones',
                          'Carrie-Anne Moss', 'Ice-T', 'Gene Hackman', 'Jack Nicholson'}
        assert names == expected_names

    def test_can_add_related_object_and_push(self):
        keanu = Person.select(self.graph, "Keanu Reeves").first()
        bill_and_ted = Film("Bill & Ted's Excellent Adventure")
        keanu.acted_in.add(bill_and_ted)
        self.graph.push(keanu)
        remote_node = remote(keanu.__ogm__.node)
        film_titles = set(title for title, in self.graph.run("MATCH (a:Person)-[:ACTED_IN]->(b) "
                                                             "WHERE id(a) = {x} "
                                                             "RETURN b.title", x=remote_node._id))
        assert film_titles == {"The Devil's Advocate", 'The Matrix Reloaded', "Something's Gotta Give",
                               'The Matrix', 'The Replacements', 'The Matrix Revolutions', 'Johnny Mnemonic',
                               "Bill & Ted's Excellent Adventure"}

    def test_can_add_related_object_with_properties_and_push(self):
        keanu = Person.select(self.graph, "Keanu Reeves").first()
        bill_and_ted = Film("Bill & Ted's Excellent Adventure")
        keanu.acted_in.add(bill_and_ted, roles=['Ted "Theodore" Logan'])
        self.graph.push(keanu)
        remote_node = remote(keanu.__ogm__.node)
        films = {title: roles for title, roles in self.graph.run("MATCH (a:Person)-[ab:ACTED_IN]->(b) "
                                                                 "WHERE id(a) = {x} "
                                                                 "RETURN b.title, ab.roles", x=remote_node._id)}
        bill_and_ted_roles = films["Bill & Ted's Excellent Adventure"]
        assert bill_and_ted_roles == ['Ted "Theodore" Logan']

    def test_can_remove_related_object_and_push(self):
        keanu = Person.select(self.graph, "Keanu Reeves").first()
        johnny_mnemonic = Film.select(self.graph, "Johnny Mnemonic").first()
        keanu.acted_in.remove(johnny_mnemonic)
        self.graph.push(keanu)
        remote_node = remote(keanu.__ogm__.node)
        film_titles = set(title for title, in self.graph.run("MATCH (a:Person)-[:ACTED_IN]->(b) "
                                                             "WHERE id(a) = {x} "
                                                             "RETURN b.title", x=remote_node._id))
        assert film_titles == {"The Devil's Advocate", 'The Matrix Reloaded', "Something's Gotta Give",
                               'The Matrix', 'The Replacements', 'The Matrix Revolutions'}

    def test_can_add_property_to_existing_relationship(self):
        keanu = Person.select(self.graph, "Keanu Reeves").first()
        johnny_mnemonic = Film.select(self.graph, "Johnny Mnemonic").first()
        keanu.acted_in.add(johnny_mnemonic, foo="bar")
        self.graph.push(keanu)
        remote_node = remote(keanu.__ogm__.node)
        johnny_foo = self.graph.evaluate("MATCH (a:Person)-[ab:ACTED_IN]->(b) "
                                         "WHERE id(a) = {x} AND b.title = 'Johnny Mnemonic' "
                                         "RETURN ab.foo", x=remote_node._id)
        assert johnny_foo == "bar"


class FindTestCase(MovieGraphTestCase):

    def test_can_find_one_object(self):
        keanu = Person.select(self.graph, "Keanu Reeves").first()
        assert keanu.name == "Keanu Reeves"
        assert keanu.year_of_birth == 1964

    def test_can_find_by_id(self):
        # given
        keanu_0 = Person.select(self.graph, "Keanu Reeves").first()
        node_id = remote(keanu_0.__ogm__.node)._id

        # when

        class PersonById(MovieGraphObject):
            __primarylabel__ = "Person"

            name = Property()
            year_of_birth = Property(key="born")

            acted_in = RelatedTo(Film)
            directed = RelatedTo("Film")
            produced = RelatedTo("test.fixtures.ogm.Film")

        found = list(PersonById.select(self.graph, node_id))
        assert found
        keanu = found[0]

        # then
        assert keanu.name == "Keanu Reeves"
        assert keanu.year_of_birth == 1964

    def test_can_find_one_by_id(self):
        # given
        keanu_0 = Person.select(self.graph, "Keanu Reeves").first()
        node_id = remote(keanu_0.__ogm__.node)._id

        # when

        class PersonById(MovieGraphObject):
            __primarylabel__ = "Person"

            name = Property()
            year_of_birth = Property(key="born")

            acted_in = RelatedTo(Film)
            directed = RelatedTo("Film")
            produced = RelatedTo("test.fixtures.ogm.Film")

        keanu = PersonById.select(self.graph, node_id).first()

        # then
        assert keanu.name == "Keanu Reeves"
        assert keanu.year_of_birth == 1964

    def test_cannot_find_one_that_does_not_exist(self):
        keanu = Person.select(self.graph, "Keanu Jones").first()
        assert keanu is None

    def test_can_find_multiple_objects(self):
        keanu, hugo = list(Person.select(self.graph, ("Keanu Reeves", "Hugo Weaving")))
        assert keanu.name == "Keanu Reeves"
        assert keanu.year_of_birth == 1964
        assert hugo.name == "Hugo Weaving"
        assert hugo.year_of_birth == 1960


class CreateTestCase(MovieGraphTestCase):

    def test_create(self):
        # given
        alice = Person()
        alice.name = "Alice"
        alice.year_of_birth = 1970
        alice.acted_in.add(Film.select(self.graph, "The Matrix").first())

        # when
        self.graph.create(alice)

        # then
        node = alice.__ogm__.node
        remote_node = remote(node)
        assert remote_node

    def test_create_has_no_effect_on_existing(self):
        # given
        keanu = Person.select(self.graph, "Keanu Reeves").first()

        # when
        keanu.name = "Keanu Charles Reeves"
        self.graph.create(keanu)

        # then
        remote_node = remote(keanu.__ogm__.node)
        remote_name = self.graph.evaluate("MATCH (a:Person) WHERE id(a) = {x} "
                                          "RETURN a.name", x=remote_node._id)
        assert remote_name == "Keanu Reeves"


class DeleteTestCase(MovieGraphTestCase):

    def test_delete_on_existing(self):
        # given
        keanu = Person.select(self.graph, "Keanu Reeves").first()
        node = keanu.__ogm__.node

        # when
        self.graph.delete(keanu)

        # then
        assert not self.graph.exists(node)


class PushTestCase(MovieGraphTestCase):

    def test_can_push_changes_to_existing(self):
        # given
        keanu = Person.select(self.graph, "Keanu Reeves").first()

        # when
        keanu.name = "Keanu Charles Reeves"
        self.graph.push(keanu)

        # then
        remote_node = remote(keanu.__ogm__.node)
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
        remote_node = remote(alice.__ogm__.node)
        remote_name = self.graph.evaluate("MATCH (a:Person) WHERE id(a) = {x} "
                                          "RETURN a.name", x=remote_node._id)
        assert remote_name == "Alice Smith"

    def test_can_push_new_that_points_to_existing(self):
        # given
        alice = Person()
        alice.name = "Alice Smith"
        alice.year_of_birth = 1970
        alice.acted_in.add(Film.select(self.graph, "The Matrix").first())

        # when
        self.graph.push(alice)

        # then
        remote_node = remote(alice.__ogm__.node)
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
        remote_node = remote(alice.__ogm__.node)
        film_node = self.graph.evaluate("MATCH (a:Person)-[:ACTED_IN]->(b) WHERE id(a) = {x} RETURN b",
                                        x=remote_node._id)
        assert film_node["title"] == "The Dominatrix"

    def test_can_push_with_incoming_relationships(self):
        # given
        matrix = Film.select(self.graph, "The Matrix").first()

        # when
        matrix.actors.remove(Person.select(self.graph, "Emil Eifrem").first())
        self.graph.push(matrix)

        # then
        remote_node = remote(matrix.__ogm__.node)
        names = set()
        for name, in self.graph.run("MATCH (a:Movie)<-[:ACTED_IN]-(b) WHERE id(a) = {x} "
                                    "RETURN b.name", x=remote_node._id):
            names.add(name)
        assert names == {'Keanu Reeves', 'Carrie-Anne Moss', 'Hugo Weaving', 'Laurence Fishburne'}


class PullTestCase(MovieGraphTestCase):

    def test_can_load_and_pull(self):
        keanu = Person.select(self.graph, "Keanu Reeves").first()
        assert keanu.name == "Keanu Reeves"
        remote_node = remote(keanu.__ogm__.node)
        self.graph.run("MATCH (a:Person) WHERE id(a) = {x} SET a.name = {y}",
                       x=remote_node._id, y="Keanu Charles Reeves")
        self.graph.pull(keanu)
        assert keanu.name == "Keanu Charles Reeves"

    def test_can_pull_without_loading(self):
        keanu = Person()
        keanu.name = "Keanu Reeves"
        self.graph.pull(keanu)
        assert keanu.year_of_birth == 1964

    def test_can_pull_with_incoming_relationships(self):
        # given
        matrix = Film.select(self.graph, "The Matrix").first()
        remote_node = remote(matrix.__ogm__.node)
        self.graph.run("MATCH (a:Movie)<-[r:ACTED_IN]-(b) WHERE id(a) = {x} AND b.name = 'Emil Eifrem' DELETE r",
                       x=remote_node._id)

        # when
        self.graph.pull(matrix)

        # then
        names = set(a.name for a in matrix.actors)
        assert names == {'Keanu Reeves', 'Carrie-Anne Moss', 'Hugo Weaving', 'Laurence Fishburne'}


class RelatedObjectsTestCase(MovieGraphTestCase):

    def new_keanu_acted_in(self):
        keanu_node = NodeSelector(self.graph).select("Person", name="Keanu Reeves").first()
        keanu_acted_in = RelatedObjects(keanu_node, OUTGOING, "ACTED_IN", Film)
        return keanu_acted_in

    def test_can_pull_related_objects(self):
        films_acted_in = self.new_keanu_acted_in()
        films_acted_in.__db_pull__(self.graph)
        film_titles = set(film.title for film in films_acted_in)
        assert film_titles == {"The Devil's Advocate", 'The Matrix Reloaded',
                               "Something's Gotta Give", 'The Matrix', 'The Replacements',
                               'The Matrix Revolutions', 'Johnny Mnemonic'}

    def test_contains_object(self):
        # given
        films_acted_in = self.new_keanu_acted_in()
        films_acted_in.__db_pull__(self.graph)

        # then
        matrix_reloaded = Film.select(self.graph, "The Matrix Reloaded").first()
        assert matrix_reloaded in films_acted_in

    def test_does_not_contain_object(self):
        # given
        films_acted_in = self.new_keanu_acted_in()
        films_acted_in.__db_pull__(self.graph)

        # then
        bill_and_ted = Film("Bill & Ted's Excellent Adventure")
        assert bill_and_ted not in films_acted_in

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
        matrix_reloaded = Film.select(self.graph, "The Matrix Reloaded").first()
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
        matrix_reloaded = Film.select(self.graph, "The Matrix Reloaded").first()
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

    def test_can_get_relationship_property_from_default(self):
        # given
        films_acted_in = self.new_keanu_acted_in()
        films_acted_in.__db_pull__(self.graph)
        matrix_reloaded = Film('The Matrix Reloaded')

        # then
        foo = films_acted_in.get(matrix_reloaded, "foo", "bar")
        assert foo == "bar"

    def test_can_get_relationship_property_from_default_and_unknown_object(self):
        # given
        films_acted_in = self.new_keanu_acted_in()
        films_acted_in.__db_pull__(self.graph)
        bill_and_ted = Film("Bill & Ted's Excellent Adventure")

        # then
        foo = films_acted_in.get(bill_and_ted, "foo", "bar")
        assert foo == "bar"

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

    def test_can_push_property_updates_on_new_object(self):
        # given
        films_acted_in = self.new_keanu_acted_in()
        films_acted_in.__db_pull__(self.graph)

        # when
        bill_and_ted = Film("Bill & Ted's Excellent Adventure")
        films_acted_in.update(bill_and_ted, good=True)
        films_acted_in.__db_push__(self.graph)

        # then
        del films_acted_in
        Node.cache.clear()
        Relationship.cache.clear()
        films_acted_in = self.new_keanu_acted_in()
        films_acted_in.__db_pull__(self.graph)
        film_titles = set(film.title for film in films_acted_in)
        assert film_titles == {"The Devil's Advocate", 'The Matrix Reloaded',
                               "Something's Gotta Give", 'The Matrix', 'The Replacements',
                               'The Matrix Revolutions', 'Johnny Mnemonic', "Bill & Ted's Excellent Adventure"}

        # and
        good = films_acted_in.get(bill_and_ted, "good")
        assert good


class Thing(GraphObject):
    __primarykey__ = "name"

    p = Label()
    q = Label()

    x = Related("Thing", "X")
    y = Related("Thing", "Y")

    x_out = RelatedTo("Thing", "X")
    y_out = RelatedTo("Thing", "Y")

    x_in = RelatedFrom("Thing", "X")
    y_in = RelatedFrom("Thing", "Y")


class ComprehensiveTestCase(GraphTestCase):

    def setUp(self):
        self.graph.delete_all()
        with open(path_join(dirname(__file__), "..", "resources", "xxy.cypher")) as f:
            cypher = f.read()
        self.graph.run(cypher)

    def test_a(self):
        a = Thing.select(self.graph, "A").first()
        # A is related to B and C
        assert len(a.x) == 2
        assert len(a.x_out) == 2
        assert len(a.x_in) == 2
        assert len(a.y) == 2
        assert len(a.y_out) == 2
        assert len(a.y_in) == 2

    def test_b(self):
        b = Thing.select(self.graph, "B").first()
        # B is only related to A
        assert len(b.x) == 1
        assert len(b.x_out) == 1
        assert len(b.x_in) == 1
        assert len(b.y) == 1
        assert len(b.y_out) == 1
        assert len(b.y_in) == 1

    def test_c(self):
        c = Thing.select(self.graph, "C").first()
        # Loops are related to themselves, hence C is related to A, C and D
        assert len(c.x) == 3
        assert len(c.x_out) == 3
        assert len(c.x_in) == 3
        assert len(c.y) == 3
        assert len(c.y_out) == 3
        assert len(c.y_in) == 3

    def test_d(self):
        d = Thing.select(self.graph, "D").first()
        # D is only related to C
        assert len(d.x) == 1
        assert len(d.x_out) == 1
        assert len(d.x_in) == 1
        assert len(d.y) == 1
        assert len(d.y_out) == 1
        assert len(d.y_in) == 1


class SimpleThing(GraphObject):
    pass


class SimpleThingTestCase(GraphTestCase):

    def test_create(self):
        thing = SimpleThing()
        self.graph.create(thing)
        assert remote(thing.__ogm__.node)

    def test_merge(self):
        thing = SimpleThing()
        self.graph.merge(thing)
        assert remote(thing.__ogm__.node)

    def test_push(self):
        thing = SimpleThing()
        self.graph.push(thing)
        assert remote(thing.__ogm__.node)

#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright 2011-2020, Nigel Small
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

from pytest import fixture

from py2neo.ogm import RelatedObjects, Property, RelatedTo, OUTGOING
from py2neo.matching import NodeMatcher

from test.fixtures.ogm import MovieGraphObject, Person, Film


@fixture(scope="function")
def movie_graph(graph):
    graph.delete_all()
    with open(path_join(dirname(__file__), "..", "resources", "movies.cypher")) as f:
        cypher = f.read()
    graph.run(cypher)
    yield graph
    graph.delete_all()


def test_related_objects_are_automatically_loaded(movie_graph):
    keanu = Person.match(movie_graph, "Keanu Reeves").first()
    film_titles = set(film.title for film in list(keanu.acted_in))
    assert film_titles == {"The Devil's Advocate", 'The Matrix Reloaded', "Something's Gotta Give",
                           'The Matrix', 'The Replacements', 'The Matrix Revolutions', 'Johnny Mnemonic'}


def test_graph_propagation(movie_graph):
    keanu = Person.match(movie_graph, "Keanu Reeves").first()
    films = list(keanu.acted_in)
    colleagues = set()
    for film in films:
        colleagues |= set(film.actors)
    names = set(colleague.name for colleague in colleagues)
    expected_names = {'Al Pacino', 'Dina Meyer', 'Keanu Reeves', 'Brooke Langton', 'Hugo Weaving', 'Diane Keaton',
                      'Takeshi Kitano', 'Laurence Fishburne', 'Charlize Theron', 'Emil Eifrem', 'Orlando Jones',
                      'Carrie-Anne Moss', 'Ice-T', 'Gene Hackman', 'Jack Nicholson'}
    assert names == expected_names


def test_can_add_related_object_and_push(movie_graph):
    keanu = Person.match(movie_graph, "Keanu Reeves").first()
    bill_and_ted = Film("Bill & Ted's Excellent Adventure")
    keanu.acted_in.add(bill_and_ted)
    movie_graph.push(keanu)
    node_id = keanu.__node__.identity
    film_titles = set(title for title, in movie_graph.run("MATCH (a:Person)-[:ACTED_IN]->(b) "
                                                          "WHERE id(a) = $x "
                                                          "RETURN b.title", x=node_id))
    assert film_titles == {"The Devil's Advocate", 'The Matrix Reloaded', "Something's Gotta Give",
                           'The Matrix', 'The Replacements', 'The Matrix Revolutions', 'Johnny Mnemonic',
                           "Bill & Ted's Excellent Adventure"}


def test_can_add_related_object_with_properties_and_push(movie_graph):
    keanu = Person.match(movie_graph, "Keanu Reeves").first()
    bill_and_ted = Film("Bill & Ted's Excellent Adventure")
    keanu.acted_in.add(bill_and_ted, roles=['Ted "Theodore" Logan'])
    movie_graph.push(keanu)
    node_id = keanu.__node__.identity
    films = {title: roles for title, roles in movie_graph.run("MATCH (a:Person)-[ab:ACTED_IN]->(b) "
                                                              "WHERE id(a) = $x "
                                                              "RETURN b.title, ab.roles", x=node_id)}
    bill_and_ted_roles = films["Bill & Ted's Excellent Adventure"]
    assert bill_and_ted_roles == ['Ted "Theodore" Logan']


def test_can_remove_related_object_and_push(movie_graph):
    keanu = Person.match(movie_graph, "Keanu Reeves").first()
    johnny_mnemonic = Film.match(movie_graph, "Johnny Mnemonic").first()
    keanu.acted_in.remove(johnny_mnemonic)
    movie_graph.push(keanu)
    node_id = keanu.__node__.identity
    film_titles = set(title for title, in movie_graph.run("MATCH (a:Person)-[:ACTED_IN]->(b) "
                                                          "WHERE id(a) = $x "
                                                          "RETURN b.title", x=node_id))
    assert film_titles == {"The Devil's Advocate", 'The Matrix Reloaded', "Something's Gotta Give",
                           'The Matrix', 'The Replacements', 'The Matrix Revolutions'}


def test_can_add_property_to_existing_relationship(movie_graph):
    keanu = Person.match(movie_graph, "Keanu Reeves").first()
    johnny_mnemonic = Film.match(movie_graph, "Johnny Mnemonic").first()
    keanu.acted_in.add(johnny_mnemonic, foo="bar")
    movie_graph.push(keanu)
    node_id = keanu.__node__.identity
    johnny_foo = movie_graph.evaluate("MATCH (a:Person)-[ab:ACTED_IN]->(b) "
                                      "WHERE id(a) = $x AND b.title = 'Johnny Mnemonic' "
                                      "RETURN ab.foo", x=node_id)
    assert johnny_foo == "bar"


def test_can_match_one_object(movie_graph):
    keanu = Person.match(movie_graph, "Keanu Reeves").first()
    assert keanu.name == "Keanu Reeves"
    assert keanu.year_of_birth == 1964


def test_can_match_by_id(movie_graph):
    # given
    keanu_0 = Person.match(movie_graph, "Keanu Reeves").first()
    node_id = keanu_0.__node__.identity

    # when

    class PersonById(MovieGraphObject):
        __primarylabel__ = "Person"

        name = Property()
        year_of_birth = Property(key="born")

        acted_in = RelatedTo(Film)
        directed = RelatedTo("Film")
        produced = RelatedTo("test.fixtures.ogm.Film")

    found = list(PersonById.match(movie_graph, node_id))
    assert found
    keanu = found[0]

    # then
    assert keanu.name == "Keanu Reeves"
    assert keanu.year_of_birth == 1964


def test_can_match_one_by_id(movie_graph):
    # given
    keanu_0 = Person.match(movie_graph, "Keanu Reeves").first()
    node_id = keanu_0.__node__.identity

    # when

    class PersonById(MovieGraphObject):
        __primarylabel__ = "Person"

        name = Property()
        year_of_birth = Property(key="born")

        acted_in = RelatedTo(Film)
        directed = RelatedTo("Film")
        produced = RelatedTo("test.fixtures.ogm.Film")

    keanu = PersonById.match(movie_graph, node_id).first()

    # then
    assert keanu.name == "Keanu Reeves"
    assert keanu.year_of_birth == 1964


def test_cannot_match_one_that_does_not_exist(movie_graph):
    keanu = Person.match(movie_graph, "Keanu Jones").first()
    assert keanu is None


def test_can_match_multiple_objects(movie_graph):
    people = list(Person.match(movie_graph, ("Keanu Reeves", "Hugo Weaving")))
    if people[0].name == "Keanu Reeves":
        keanu, hugo = people
    else:
        hugo, keanu = people
    assert keanu.name == "Keanu Reeves"
    assert keanu.year_of_birth == 1964
    assert hugo.name == "Hugo Weaving"
    assert hugo.year_of_birth == 1960


def test_create(movie_graph):
    # given
    alice = Person()
    alice.name = "Alice"
    alice.year_of_birth = 1970
    alice.acted_in.add(Film.match(movie_graph, "The Matrix").first())

    # when
    movie_graph.create(alice)

    # then
    node = alice.__node__
    assert node.graph == movie_graph
    assert node.identity is not None


def test_create_has_no_effect_on_existing(movie_graph):
    # given
    keanu = Person.match(movie_graph, "Keanu Reeves").first()

    # when
    keanu.name = "Keanu Charles Reeves"
    movie_graph.create(keanu)

    # then
    node_id = keanu.__node__.identity
    remote_name = movie_graph.evaluate("MATCH (a:Person) WHERE id(a) = $x "
                                       "RETURN a.name", x=node_id)
    assert remote_name == "Keanu Reeves"


def test_delete_on_existing(movie_graph):
    # given
    keanu = Person.match(movie_graph, "Keanu Reeves").first()

    # when
    movie_graph.delete(keanu)

    # then
    assert not movie_graph.exists(keanu)


def test_exists_on_existing(movie_graph):
    # given
    keanu = Person.match(movie_graph, "Keanu Reeves").first()

    # when
    exists = movie_graph.exists(keanu)

    # then
    assert exists


def test_not_exists_on_non_existing(movie_graph):
    # given
    alice = Person()
    alice.name = "Alice Smith"
    alice.year_of_birth = 1970

    # when
    exists = movie_graph.exists(alice)

    # then
    assert not exists


def test_can_push_changes_to_existing(movie_graph):
    # given
    keanu = Person.match(movie_graph, "Keanu Reeves").first()

    # when
    keanu.name = "Keanu Charles Reeves"
    movie_graph.push(keanu)

    # then
    node_id = keanu.__node__.identity
    remote_name = movie_graph.evaluate("MATCH (a:Person) WHERE id(a) = $x "
                                       "RETURN a.name", x=node_id)
    assert remote_name == "Keanu Charles Reeves"


def test_can_push_new(movie_graph):
    # given
    alice = Person()
    alice.name = "Alice Smith"
    alice.year_of_birth = 1970

    # when
    movie_graph.push(alice)

    # then
    node_id = alice.__node__.identity
    remote_name = movie_graph.evaluate("MATCH (a:Person) WHERE id(a) = $x "
                                       "RETURN a.name", x=node_id)
    assert remote_name == "Alice Smith"


def test_can_push_new_that_points_to_existing(movie_graph):
    # given
    alice = Person()
    alice.name = "Alice Smith"
    alice.year_of_birth = 1970
    alice.acted_in.add(Film.match(movie_graph, "The Matrix").first())

    # when
    movie_graph.push(alice)

    # then
    node_id = alice.__node__.identity
    film_node = movie_graph.evaluate("MATCH (a:Person)-[:ACTED_IN]->(b) WHERE id(a) = $x RETURN b",
                                     x=node_id)
    assert film_node["title"] == "The Matrix"
    assert film_node["tagline"] == "Welcome to the Real World"


def test_can_push_new_that_points_to_new(movie_graph):
    # given
    the_dominatrix = Film("The Dominatrix")
    alice = Person()
    alice.name = "Alice Smith"
    alice.year_of_birth = 1970
    alice.acted_in.add(the_dominatrix)

    # when
    movie_graph.push(alice)

    # then
    node_id = alice.__node__.identity
    film_node = movie_graph.evaluate("MATCH (a:Person)-[:ACTED_IN]->(b) WHERE id(a) = $x RETURN b",
                                     x=node_id)
    assert film_node["title"] == "The Dominatrix"


def test_can_push_with_incoming_relationships(movie_graph):
    # given
    matrix = Film.match(movie_graph, "The Matrix").first()

    # when
    matrix.actors.remove(Person.match(movie_graph, "Emil Eifrem").first())
    movie_graph.push(matrix)

    # then
    node_id = matrix.__node__.identity
    names = set()
    for name, in movie_graph.run("MATCH (a:Movie)<-[:ACTED_IN]-(b) WHERE id(a) = $x "
                                 "RETURN b.name", x=node_id):
        names.add(name)
    assert names, {'Keanu Reeves', 'Carrie-Anne Moss', 'Hugo Weaving' == 'Laurence Fishburne'}


def test_can_load_and_pull(movie_graph):
    keanu = Person.match(movie_graph, "Keanu Reeves").first()
    assert keanu.name == "Keanu Reeves"
    node_id = keanu.__node__.identity
    movie_graph.run("MATCH (a:Person) WHERE id(a) = $x SET a.name = $y",
                    x=node_id, y="Keanu Charles Reeves")
    movie_graph.pull(keanu)
    assert keanu.name == "Keanu Charles Reeves"


def test_can_pull_without_loading(movie_graph):
    keanu = Person()
    keanu.name = "Keanu Reeves"
    movie_graph.pull(keanu)
    assert keanu.year_of_birth == 1964


def test_can_pull_with_incoming_relationships(movie_graph):
    # given
    matrix = Film.match(movie_graph, "The Matrix").first()
    node_id = matrix.__node__.identity
    movie_graph.run("MATCH (a:Movie)<-[r:ACTED_IN]-(b) WHERE id(a) = $x AND b.name = 'Emil Eifrem' DELETE r",
                    x=node_id)

    # when
    movie_graph.pull(matrix)

    # then
    names = set(a.name for a in matrix.actors)
    assert names, {'Keanu Reeves', 'Carrie-Anne Moss', 'Hugo Weaving' == 'Laurence Fishburne'}


@fixture()
def new_keanu_acted_in(movie_graph):

    def f():
        keanu_node = NodeMatcher(movie_graph).match("Person", name="Keanu Reeves").first()
        keanu_acted_in = RelatedObjects(keanu_node, OUTGOING, "ACTED_IN", Film)
        return keanu_acted_in

    return f


def test_can_pull_related_objects(movie_graph, new_keanu_acted_in):
    filmography = new_keanu_acted_in()
    movie_graph.pull(filmography)
    film_titles = set(film.title for film in filmography)
    assert film_titles == {"The Devil's Advocate", 'The Matrix Reloaded',
                           "Something's Gotta Give", 'The Matrix', 'The Replacements',
                           'The Matrix Revolutions', 'Johnny Mnemonic'}


def test_contains_object(movie_graph, new_keanu_acted_in):
    # given
    filmography = new_keanu_acted_in()
    movie_graph.pull(filmography)

    # then
    matrix_reloaded = Film.match(movie_graph, "The Matrix Reloaded").first()
    assert matrix_reloaded in filmography


def test_does_not_contain_object(movie_graph, new_keanu_acted_in):
    # given
    filmography = new_keanu_acted_in()
    movie_graph.pull(filmography)

    # then
    bill_and_ted = Film("Bill & Ted's Excellent Adventure")
    assert bill_and_ted not in filmography


def test_can_add_object(movie_graph, new_keanu_acted_in):
    filmography = new_keanu_acted_in()
    movie_graph.pull(filmography)
    bill_and_ted = Film("Bill & Ted's Excellent Adventure")
    filmography.add(bill_and_ted)
    film_titles = set(film.title for film in filmography)
    assert film_titles == {"The Devil's Advocate", 'The Matrix Reloaded',
                           "Something's Gotta Give", 'The Matrix', 'The Replacements',
                           'The Matrix Revolutions', 'Johnny Mnemonic', "Bill & Ted's Excellent Adventure"}


def test_can_add_object_when_already_present(movie_graph, new_keanu_acted_in):
    filmography = new_keanu_acted_in()
    movie_graph.pull(filmography)
    bill_and_ted = Film("Bill & Ted's Excellent Adventure")
    filmography.add(bill_and_ted)
    filmography.add(bill_and_ted)
    film_titles = set(film.title for film in filmography)
    assert film_titles == {"The Devil's Advocate", 'The Matrix Reloaded',
                           "Something's Gotta Give", 'The Matrix', 'The Replacements',
                           'The Matrix Revolutions', 'Johnny Mnemonic', "Bill & Ted's Excellent Adventure"}


def test_can_remove_object(movie_graph, new_keanu_acted_in):
    # given
    filmography = new_keanu_acted_in()
    movie_graph.pull(filmography)

    # when
    matrix_reloaded = Film.match(movie_graph, "The Matrix Reloaded").first()
    filmography.remove(matrix_reloaded)

    # then
    film_titles = set(film.title for film in filmography)
    assert film_titles == {"The Devil's Advocate",
                           "Something's Gotta Give", 'The Matrix', 'The Replacements',
                           'The Matrix Revolutions', 'Johnny Mnemonic'}


def test_can_remove_object_when_already_absent(movie_graph, new_keanu_acted_in):
    # given
    filmography = new_keanu_acted_in()
    movie_graph.pull(filmography)
    matrix_reloaded = Film.match(movie_graph, "The Matrix Reloaded").first()
    filmography.remove(matrix_reloaded)

    # when
    filmography.remove(matrix_reloaded)

    # then
    film_titles = set(film.title for film in filmography)
    assert film_titles == {"The Devil's Advocate",
                           "Something's Gotta Give", 'The Matrix', 'The Replacements',
                           'The Matrix Revolutions', 'Johnny Mnemonic'}


def test_can_push_object_additions(movie_graph, new_keanu_acted_in):
    # given
    filmography = new_keanu_acted_in()
    movie_graph.pull(filmography)

    # when
    bill_and_ted = Film("Bill & Ted's Excellent Adventure")
    filmography.add(bill_and_ted)
    movie_graph.push(filmography)

    # then
    del filmography
    filmography = new_keanu_acted_in()
    movie_graph.pull(filmography)
    film_titles = set(film.title for film in filmography)
    assert film_titles == {"The Devil's Advocate", 'The Matrix Reloaded',
                           "Something's Gotta Give", 'The Matrix', 'The Replacements',
                           'The Matrix Revolutions', 'Johnny Mnemonic', "Bill & Ted's Excellent Adventure"}


def test_can_push_object_removals(movie_graph, new_keanu_acted_in):
    # given
    filmography = new_keanu_acted_in()
    movie_graph.pull(filmography)

    # when
    matrix_reloaded = Film('The Matrix Reloaded')
    filmography.remove(matrix_reloaded)
    movie_graph.push(filmography)

    # then
    del filmography
    movie_graph.node_cache.clear()
    movie_graph.relationship_cache.clear()
    filmography = new_keanu_acted_in()
    movie_graph.pull(filmography)
    film_titles = set(film.title for film in filmography)
    assert film_titles == {"The Devil's Advocate",
                           "Something's Gotta Give", 'The Matrix', 'The Replacements',
                           'The Matrix Revolutions', 'Johnny Mnemonic'}


def test_can_get_relationship_property(movie_graph, new_keanu_acted_in):
    # given
    filmography = new_keanu_acted_in()
    movie_graph.pull(filmography)
    matrix_reloaded = Film('The Matrix Reloaded')

    # then
    roles = filmography.get(matrix_reloaded, "roles")
    assert roles == ["Neo"]


def test_can_get_relationship_property_from_default(movie_graph, new_keanu_acted_in):
    # given
    filmography = new_keanu_acted_in()
    movie_graph.pull(filmography)
    matrix_reloaded = Film('The Matrix Reloaded')

    # then
    foo = filmography.get(matrix_reloaded, "foo", "bar")
    assert foo == "bar"


def test_can_get_relationship_property_from_default_and_unknown_object(movie_graph, new_keanu_acted_in):
    # given
    filmography = new_keanu_acted_in()
    movie_graph.pull(filmography)
    bill_and_ted = Film("Bill & Ted's Excellent Adventure")

    # then
    foo = filmography.get(bill_and_ted, "foo", "bar")
    assert foo == "bar"


def test_can_push_property_additions(movie_graph, new_keanu_acted_in):
    # given
    filmography = new_keanu_acted_in()
    movie_graph.pull(filmography)

    # when
    matrix = Film("The Matrix")
    filmography.update(matrix, good=True)
    movie_graph.push(filmography)

    # then
    del filmography
    movie_graph.node_cache.clear()
    movie_graph.relationship_cache.clear()
    filmography = new_keanu_acted_in()
    movie_graph.pull(filmography)
    good = filmography.get(matrix, "good")
    assert good


def test_can_push_property_removals(movie_graph, new_keanu_acted_in):
    # given
    filmography = new_keanu_acted_in()
    movie_graph.pull(filmography)

    # when
    matrix = Film("The Matrix")
    filmography.update(matrix, roles=None)
    movie_graph.push(filmography)

    # then
    del filmography
    movie_graph.node_cache.clear()
    movie_graph.relationship_cache.clear()
    filmography = new_keanu_acted_in()
    movie_graph.pull(filmography)
    roles = filmography.get(matrix, "roles")
    assert roles is None


def test_can_push_property_updates(movie_graph, new_keanu_acted_in):
    # given
    filmography = new_keanu_acted_in()
    movie_graph.pull(filmography)

    # when
    matrix = Film("The Matrix")
    filmography.update(matrix, roles=1)
    movie_graph.push(filmography)

    # then
    del filmography
    movie_graph.node_cache.clear()
    movie_graph.relationship_cache.clear()
    filmography = new_keanu_acted_in()
    movie_graph.pull(filmography)
    roles = filmography.get(matrix, "roles")
    assert roles == 1


def test_can_push_property_updates_on_new_object(movie_graph, new_keanu_acted_in):
    # given
    filmography = new_keanu_acted_in()
    movie_graph.pull(filmography)

    # when
    bill_and_ted = Film("Bill & Ted's Excellent Adventure")
    filmography.update(bill_and_ted, good=True)
    movie_graph.push(filmography)

    # then
    del filmography
    movie_graph.node_cache.clear()
    movie_graph.relationship_cache.clear()
    filmography = new_keanu_acted_in()
    movie_graph.pull(filmography)
    film_titles = set(film.title for film in filmography)
    assert film_titles == {"The Devil's Advocate", 'The Matrix Reloaded',
                           "Something's Gotta Give", 'The Matrix', 'The Replacements',
                           'The Matrix Revolutions', 'Johnny Mnemonic', "Bill & Ted's Excellent Adventure"}

    # and
    good = filmography.get(bill_and_ted, "good")
    assert good


def test_property_default(movie_graph):
    # given
    film = Film.match(movie_graph, "Something's Gotta Give").first()

    # then
    assert film.tag_line == "Bit boring"

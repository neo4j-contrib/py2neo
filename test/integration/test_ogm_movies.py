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


from pytest import warns

from py2neo.ogm import Property, RelatedTo

from test.fixtures.ogm import MovieModel, Person, Film


def test_related_objects_are_automatically_loaded(movie_repo):
    keanu = movie_repo.get(Person, "Keanu Reeves")
    film_titles = set(film.title for film in list(keanu.acted_in))
    assert film_titles == {"The Devil's Advocate", 'The Matrix Reloaded', "Something's Gotta Give",
                           'The Matrix', 'The Replacements', 'The Matrix Revolutions',
                           'Johnny Mnemonic'}


def test_graph_propagation(movie_repo):
    keanu = movie_repo.get(Person, "Keanu Reeves")
    films = list(keanu.acted_in)
    colleagues = set()
    for film in films:
        colleagues |= set(film.actors)
    names = set(colleague.name for colleague in colleagues)
    expected_names = {'Al Pacino', 'Dina Meyer', 'Keanu Reeves', 'Brooke Langton', 'Hugo Weaving',
                      'Diane Keaton', 'Takeshi Kitano', 'Laurence Fishburne', 'Charlize Theron',
                      'Emil Eifrem', 'Orlando Jones', 'Carrie-Anne Moss', 'Ice-T', 'Gene Hackman',
                      'Jack Nicholson'}
    assert names == expected_names


def test_can_add_related_object_and_push(movie_repo):
    keanu = movie_repo.get(Person, "Keanu Reeves")
    bill_and_ted = Film("Bill & Ted's Excellent Adventure")
    keanu.acted_in.add(bill_and_ted)
    movie_repo.save(keanu)
    node_id = keanu.__node__.identity
    film_titles = set(title
                      for title, in movie_repo.graph.run("MATCH (a:Person)-[:ACTED_IN]->(b) "
                                                         "WHERE id(a) = $x "
                                                         "RETURN b.title", x=node_id))
    assert film_titles == {"The Devil's Advocate", 'The Matrix Reloaded', "Something's Gotta Give",
                           'The Matrix', 'The Replacements', 'The Matrix Revolutions',
                           'Johnny Mnemonic', "Bill & Ted's Excellent Adventure"}


def test_can_add_related_object_with_properties_and_push(movie_repo):
    keanu = movie_repo.get(Person, "Keanu Reeves")
    bill_and_ted = Film("Bill & Ted's Excellent Adventure")
    keanu.acted_in.add(bill_and_ted, roles=['Ted "Theodore" Logan'])
    movie_repo.save(keanu)
    node_id = keanu.__node__.identity
    films = {title: roles
             for title, roles in movie_repo.graph.run("MATCH (a:Person)-[ab:ACTED_IN]->(b) "
                                                      "WHERE id(a) = $x "
                                                      "RETURN b.title, ab.roles", x=node_id)}
    bill_and_ted_roles = films["Bill & Ted's Excellent Adventure"]
    assert bill_and_ted_roles == ['Ted "Theodore" Logan']


def test_can_remove_related_object_and_push(movie_repo):
    keanu = movie_repo.get(Person, "Keanu Reeves")
    johnny_mnemonic = Film.match(movie_repo, "Johnny Mnemonic").first()
    keanu.acted_in.remove(johnny_mnemonic)
    movie_repo.save(keanu)
    node_id = keanu.__node__.identity
    film_titles = set(title
                      for title, in movie_repo.graph.run("MATCH (a:Person)-[:ACTED_IN]->(b) "
                                                         "WHERE id(a) = $x "
                                                         "RETURN b.title", x=node_id))
    assert film_titles == {"The Devil's Advocate", 'The Matrix Reloaded', "Something's Gotta Give",
                           'The Matrix', 'The Replacements', 'The Matrix Revolutions'}


def test_can_add_property_to_existing_relationship(movie_repo):
    keanu = movie_repo.get(Person, "Keanu Reeves")
    johnny_mnemonic = Film.match(movie_repo, "Johnny Mnemonic").first()
    keanu.acted_in.add(johnny_mnemonic, foo="bar")
    movie_repo.save(keanu)
    node_id = keanu.__node__.identity
    johnny_foo = movie_repo.graph.evaluate("MATCH (a:Person)-[ab:ACTED_IN]->(b) "
                                           "WHERE id(a) = $x AND b.title = 'Johnny Mnemonic' "
                                           "RETURN ab.foo", x=node_id)
    assert johnny_foo == "bar"


def test_can_match_one_object(movie_repo):
    keanu = movie_repo.get(Person, "Keanu Reeves")
    assert keanu.name == "Keanu Reeves"
    assert keanu.year_of_birth == 1964


def test_can_match_by_id(movie_repo):
    # given
    keanu_0 = movie_repo.get(Person, "Keanu Reeves")
    node_id = keanu_0.__node__.identity

    # when

    class PersonById(MovieModel):
        __primarylabel__ = "Person"

        name = Property()
        year_of_birth = Property(key="born")

        acted_in = RelatedTo(Film)
        directed = RelatedTo("Film")
        produced = RelatedTo("test.fixtures.ogm.Film")

    found = list(PersonById.match(movie_repo, node_id))
    assert found
    keanu = found[0]

    # then
    assert keanu.name == "Keanu Reeves"
    assert keanu.year_of_birth == 1964


def test_can_match_one_by_id(movie_repo):
    # given
    keanu_0 = movie_repo.get(Person, "Keanu Reeves")
    node_id = keanu_0.__node__.identity

    # when

    class PersonById(MovieModel):
        __primarylabel__ = "Person"

        name = Property()
        year_of_birth = Property(key="born")

        acted_in = RelatedTo(Film)
        directed = RelatedTo("Film")
        produced = RelatedTo("test.fixtures.ogm.Film")

    keanu = PersonById.match(movie_repo, node_id).first()

    # then
    assert keanu.name == "Keanu Reeves"
    assert keanu.year_of_birth == 1964


def test_cannot_match_one_that_does_not_exist(movie_repo):
    keanu = Person.match(movie_repo, "Keanu Jones").first()
    assert keanu is None


def test_can_match_multiple_objects(movie_repo):
    people = list(Person.match(movie_repo, ("Keanu Reeves", "Hugo Weaving")))
    if people[0].name == "Keanu Reeves":
        keanu, hugo = people
    else:
        hugo, keanu = people
    assert keanu.name == "Keanu Reeves"
    assert keanu.year_of_birth == 1964
    assert hugo.name == "Hugo Weaving"
    assert hugo.year_of_birth == 1960


def test_create(movie_repo):
    # given
    alice = Person()
    alice.name = "Alice"
    alice.year_of_birth = 1970
    alice.acted_in.add(Film.match(movie_repo, "The Matrix").first())

    # when
    movie_repo.save(alice)

    # then
    node = alice.__node__
    assert node.graph == movie_repo.graph
    assert node.identity is not None


def test_create_has_no_effect_on_existing(movie_repo):
    # given
    keanu = movie_repo.get(Person, "Keanu Reeves")

    # when
    keanu.name = "Keanu Charles Reeves"
    with warns(DeprecationWarning):
        movie_repo.create(keanu)

    # then
    node_id = keanu.__node__.identity
    remote_name = movie_repo.graph.evaluate("MATCH (a:Person) WHERE id(a) = $x "
                                            "RETURN a.name", x=node_id)
    assert remote_name == "Keanu Reeves"


def test_delete_on_existing(movie_repo):
    # given
    keanu = movie_repo.get(Person, "Keanu Reeves")

    # when
    movie_repo.delete(keanu)

    # then
    assert not movie_repo.exists(keanu)


def test_exists_on_existing(movie_repo):
    # given
    keanu = movie_repo.get(Person, "Keanu Reeves")

    # then
    assert movie_repo.exists(keanu)


def test_not_exists_on_non_existing(movie_repo):
    # given
    alice = Person()
    alice.name = "Alice Smith"
    alice.year_of_birth = 1970

    # when
    exists = movie_repo.exists(alice)

    # then
    assert not exists


def test_can_push_changes_to_existing(movie_repo):
    # given
    keanu = movie_repo.get(Person, "Keanu Reeves")

    # when
    keanu.name = "Keanu Charles Reeves"
    movie_repo.save(keanu)

    # then
    node_id = keanu.__node__.identity
    remote_name = movie_repo.graph.evaluate("MATCH (a:Person) WHERE id(a) = $x "
                                            "RETURN a.name", x=node_id)
    assert remote_name == "Keanu Charles Reeves"


def test_can_push_new(movie_repo):
    # given
    alice = Person()
    alice.name = "Alice Smith"
    alice.year_of_birth = 1970

    # when
    movie_repo.save(alice)

    # then
    node_id = alice.__node__.identity
    remote_name = movie_repo.graph.evaluate("MATCH (a:Person) WHERE id(a) = $x "
                                            "RETURN a.name", x=node_id)
    assert remote_name == "Alice Smith"


def test_can_push_new_that_points_to_existing(movie_repo):
    # given
    alice = Person()
    alice.name = "Alice Smith"
    alice.year_of_birth = 1970
    alice.acted_in.add(Film.match(movie_repo, "The Matrix").first())

    # when
    movie_repo.save(alice)

    # then
    node_id = alice.__node__.identity
    film_node = movie_repo.graph.evaluate("MATCH (a:Person)-[:ACTED_IN]->(b) "
                                          "WHERE id(a) = $x RETURN b", x=node_id)
    assert film_node["title"] == "The Matrix"
    assert film_node["tagline"] == "Welcome to the Real World"


def test_can_push_new_that_points_to_new(movie_repo):
    # given
    the_dominatrix = Film("The Dominatrix")
    alice = Person()
    alice.name = "Alice Smith"
    alice.year_of_birth = 1970
    alice.acted_in.add(the_dominatrix)

    # when
    movie_repo.save(alice)

    # then
    node_id = alice.__node__.identity
    film_node = movie_repo.graph.evaluate("MATCH (a:Person)-[:ACTED_IN]->(b) "
                                          "WHERE id(a) = $x RETURN b", x=node_id)
    assert film_node["title"] == "The Dominatrix"


def test_can_push_with_incoming_relationships(movie_repo):
    # given
    matrix = Film.match(movie_repo, "The Matrix").first()

    # when
    matrix.actors.remove(Person.match(movie_repo, "Emil Eifrem").first())
    movie_repo.save(matrix)

    # then
    node_id = matrix.__node__.identity
    names = set()
    for name, in movie_repo.graph.run("MATCH (a:Movie)<-[:ACTED_IN]-(b) WHERE id(a) = $x "
                                      "RETURN b.name", x=node_id):
        names.add(name)
    assert names, {'Keanu Reeves', 'Carrie-Anne Moss',
                   'Hugo Weaving' == 'Laurence Fishburne'}


def test_can_load_and_pull(movie_repo):
    keanu = movie_repo.get(Person, "Keanu Reeves")
    assert keanu.name == "Keanu Reeves"
    node_id = keanu.__node__.identity
    movie_repo.graph.run("MATCH (a:Person) WHERE id(a) = $x SET a.name = $y",
                         x=node_id, y="Keanu Charles Reeves")
    movie_repo.reload(keanu)
    assert keanu.name == "Keanu Charles Reeves"


def test_can_pull_without_loading(movie_repo):
    keanu = Person()
    keanu.name = "Keanu Reeves"
    movie_repo.reload(keanu)
    assert keanu.year_of_birth == 1964


def test_can_pull_with_incoming_relationships(movie_repo):
    # given
    matrix = Film.match(movie_repo, "The Matrix").first()
    node_id = matrix.__node__.identity
    movie_repo.graph.run("MATCH (a:Movie)<-[r:ACTED_IN]-(b) "
                         "WHERE id(a) = $x AND b.name = 'Emil Eifrem' DELETE r", x=node_id)

    # when
    movie_repo.reload(matrix)

    # then
    names = set(a.name for a in matrix.actors)
    assert names, {'Keanu Reeves', 'Carrie-Anne Moss', 'Hugo Weaving' == 'Laurence Fishburne'}


def test_can_pull_related_objects(movie_repo):
    filmography = movie_repo.get(Person, "Keanu Reeves").acted_in
    film_titles = set(film.title for film in filmography)
    assert film_titles == {"The Devil's Advocate", 'The Matrix Reloaded',
                           "Something's Gotta Give", 'The Matrix', 'The Replacements',
                           'The Matrix Revolutions', 'Johnny Mnemonic'}


def test_contains_object(movie_repo):
    # given
    filmography = movie_repo.get(Person, "Keanu Reeves").acted_in

    # then
    matrix_reloaded = Film.match(movie_repo, "The Matrix Reloaded").first()
    assert matrix_reloaded in filmography


def test_does_not_contain_object(movie_repo):
    # given
    filmography = movie_repo.get(Person, "Keanu Reeves").acted_in

    # then
    bill_and_ted = Film("Bill & Ted's Excellent Adventure")
    assert bill_and_ted not in filmography


def test_can_add_object(movie_repo):
    filmography = movie_repo.get(Person, "Keanu Reeves").acted_in
    bill_and_ted = Film("Bill & Ted's Excellent Adventure")
    filmography.add(bill_and_ted)
    film_titles = set(film.title for film in filmography)
    assert film_titles == {"The Devil's Advocate", 'The Matrix Reloaded',
                           "Something's Gotta Give", 'The Matrix', 'The Replacements',
                           'The Matrix Revolutions', 'Johnny Mnemonic',
                           "Bill & Ted's Excellent Adventure"}


def test_can_add_object_when_already_present(movie_repo):
    filmography = movie_repo.get(Person, "Keanu Reeves").acted_in
    bill_and_ted = Film("Bill & Ted's Excellent Adventure")
    filmography.add(bill_and_ted)
    filmography.add(bill_and_ted)
    film_titles = set(film.title for film in filmography)
    assert film_titles == {"The Devil's Advocate", 'The Matrix Reloaded',
                           "Something's Gotta Give", 'The Matrix', 'The Replacements',
                           'The Matrix Revolutions', 'Johnny Mnemonic',
                           "Bill & Ted's Excellent Adventure"}


def test_can_remove_object(movie_repo):
    # given
    filmography = movie_repo.get(Person, "Keanu Reeves").acted_in

    # when
    matrix_reloaded = Film.match(movie_repo, "The Matrix Reloaded").first()
    filmography.remove(matrix_reloaded)

    # then
    film_titles = set(film.title for film in filmography)
    assert film_titles == {"The Devil's Advocate",
                           "Something's Gotta Give", 'The Matrix', 'The Replacements',
                           'The Matrix Revolutions', 'Johnny Mnemonic'}


def test_can_remove_object_when_already_absent(movie_repo):
    # given
    filmography = movie_repo.get(Person, "Keanu Reeves").acted_in
    matrix_reloaded = Film.match(movie_repo, "The Matrix Reloaded").first()
    filmography.remove(matrix_reloaded)

    # when
    filmography.remove(matrix_reloaded)

    # then
    film_titles = set(film.title for film in filmography)
    assert film_titles == {"The Devil's Advocate",
                           "Something's Gotta Give", 'The Matrix', 'The Replacements',
                           'The Matrix Revolutions', 'Johnny Mnemonic'}


def test_can_push_object_additions(movie_repo):
    # given
    filmography = movie_repo.get(Person, "Keanu Reeves").acted_in

    # when
    bill_and_ted = Film("Bill & Ted's Excellent Adventure")
    filmography.add(bill_and_ted)
    movie_repo.save(filmography)

    # then
    del filmography
    filmography = movie_repo.get(Person, "Keanu Reeves").acted_in
    movie_repo.reload(filmography)
    film_titles = set(film.title for film in filmography)
    assert film_titles == {"The Devil's Advocate", 'The Matrix Reloaded',
                           "Something's Gotta Give", 'The Matrix', 'The Replacements',
                           'The Matrix Revolutions', 'Johnny Mnemonic',
                           "Bill & Ted's Excellent Adventure"}


def test_can_push_object_removals(movie_repo):
    # given
    filmography = movie_repo.get(Person, "Keanu Reeves").acted_in

    # when
    matrix_reloaded = Film('The Matrix Reloaded')
    filmography.remove(matrix_reloaded)
    movie_repo.save(filmography)

    # then
    del filmography
    filmography = movie_repo.get(Person, "Keanu Reeves").acted_in
    movie_repo.reload(filmography)
    film_titles = set(film.title for film in filmography)
    assert film_titles == {"The Devil's Advocate",
                           "Something's Gotta Give", 'The Matrix', 'The Replacements',
                           'The Matrix Revolutions', 'Johnny Mnemonic'}


def test_can_get_relationship_property(movie_repo):
    # given
    filmography = movie_repo.get(Person, "Keanu Reeves").acted_in
    matrix_reloaded = Film('The Matrix Reloaded')

    # then
    roles = filmography.get(matrix_reloaded, "roles")
    assert roles == ["Neo"]


def test_can_get_relationship_property_from_default(movie_repo):
    # given
    filmography = movie_repo.get(Person, "Keanu Reeves").acted_in
    matrix_reloaded = Film('The Matrix Reloaded')

    # then
    foo = filmography.get(matrix_reloaded, "foo", "bar")
    assert foo == "bar"


def test_can_get_relationship_property_from_default_and_unknown_object(movie_repo):
    # given
    filmography = movie_repo.get(Person, "Keanu Reeves").acted_in
    bill_and_ted = Film("Bill & Ted's Excellent Adventure")

    # then
    foo = filmography.get(bill_and_ted, "foo", "bar")
    assert foo == "bar"


def test_can_push_property_additions(movie_repo):
    # given
    filmography = movie_repo.get(Person, "Keanu Reeves").acted_in

    # when
    matrix = Film("The Matrix")
    filmography.add(matrix, good=True)
    movie_repo.save(filmography)

    # then
    del filmography
    filmography = movie_repo.get(Person, "Keanu Reeves").acted_in
    movie_repo.reload(filmography)
    good = filmography.get(matrix, "good")
    assert good


def test_can_push_property_removals(movie_repo):
    # given
    filmography = movie_repo.get(Person, "Keanu Reeves").acted_in

    # when
    matrix = Film("The Matrix")
    filmography.add(matrix, roles=None)
    movie_repo.save(filmography)

    # then
    del filmography
    filmography = movie_repo.get(Person, "Keanu Reeves").acted_in
    movie_repo.reload(filmography)
    roles = filmography.get(matrix, "roles")
    assert roles is None


def test_can_push_property_updates(movie_repo):
    # given
    filmography = movie_repo.get(Person, "Keanu Reeves").acted_in

    # when
    matrix = Film("The Matrix")
    filmography.add(matrix, roles=1)
    movie_repo.save(filmography)

    # then
    del filmography
    filmography = movie_repo.get(Person, "Keanu Reeves").acted_in
    movie_repo.reload(filmography)
    roles = filmography.get(matrix, "roles")
    assert roles == 1


def test_can_push_property_updates_on_new_object(movie_repo):
    # given
    filmography = movie_repo.get(Person, "Keanu Reeves").acted_in

    # when
    bill_and_ted = Film("Bill & Ted's Excellent Adventure")
    filmography.add(bill_and_ted, good=True)
    movie_repo.save(filmography)

    # then
    del filmography
    filmography = movie_repo.get(Person, "Keanu Reeves").acted_in
    movie_repo.reload(filmography)
    film_titles = set(film.title for film in filmography)
    assert film_titles == {"The Devil's Advocate", 'The Matrix Reloaded',
                           "Something's Gotta Give", 'The Matrix', 'The Replacements',
                           'The Matrix Revolutions', 'Johnny Mnemonic',
                           "Bill & Ted's Excellent Adventure"}

    # and
    good = filmography.get(bill_and_ted, "good")
    assert good


def test_property_default(movie_repo):
    # given
    film = Film.match(movie_repo, "Something's Gotta Give").first()

    # then
    assert film.tag_line == "Bit boring"

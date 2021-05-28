#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright 2011-2021, Nigel Small
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


from pytest import fixture

from py2neo.ogm import Repository, RelatedTo, RelatedFrom, Model, Property


class SimplePerson(Model):
    name = Property()


class Wife(Model):
    name = Property()
    husband = RelatedTo("Husband")

    def __init__(self):
        husband = Husband()
        self.husband.add(husband)
        husband.wife.add(self)


class Husband(Model):
    wife = RelatedFrom(Wife)


class UniquePerson(Model):
    __primarykey__ = "name"
    name = Property()

    def __init__(self):
        self.name = "Alice"


class UniqueFullyNamedPerson(Model):
    __primarykey__ = ("name", "family_name")
    name = Property()
    family_name = Property()
    age = Property()

    def __init__(self):
        self.name = "Alice"
        self.family_name = "Smith"


class PersonWithExplicitLabel(Model):
    __primarylabel__ = "Person"
    name = Property()

    def __init__(self):
        self.name = "Alice"


class PersonWithCompositeLabels(Model):
    __primarylabel__ = ("Person", "Human")
    name = Property()

    def __init__(self):
        self.name = "Alice"


@fixture(params=[SimplePerson,
                 Wife,
                 UniquePerson,
                 UniqueFullyNamedPerson,
                 PersonWithExplicitLabel,
                 PersonWithCompositeLabels])
def thing(request):
    return request.param()


def test_repo(uri, graph):
    repo = Repository(uri)
    assert repo.graph == graph


def test_reload(repo, thing):
    repo.save(thing)
    repo.graph.update("MATCH (a) WHERE id(a) = $x SET a.name = $name",
                      {"x": thing.__node__.identity, "name": "Bob"})
    repo.reload(thing)
    assert thing.name == "Bob"


def test_save(repo, thing):
    repo.save(thing)
    assert repo.exists(thing)


def test_delete(repo, thing):
    repo.save(thing)
    repo.delete(thing)
    assert not repo.exists(thing)


def test_exists(repo, thing):
    repo.save(thing)
    assert repo.exists(thing)


def test_not_exists(repo, thing):
    assert not repo.exists(thing)


def test_match(repo, thing):
    repo.save(thing)
    assert repo.match(thing.__class__).count() == 1


def test_no_match(repo, thing):
    assert repo.match(thing.__class__).count() == 0


def test_get(repo, thing):
    repo.save(thing)
    assert repo.get(thing.__class__) == thing


def test_get_none(repo, thing):
    assert repo.get(thing.__class__) is None


def test_create(repo, thing):
    repo.create(thing)
    assert thing.__node__.graph is repo.graph
    assert thing.__node__.identity is not None


def test_merge(repo, thing):
    repo.merge(thing)
    assert thing.__node__.graph is repo.graph
    assert thing.__node__.identity is not None


def test_merge_with_composite_key(repo):
    thing1 = UniqueFullyNamedPerson()
    thing1.age = 33
    thing2 = UniqueFullyNamedPerson()
    thing2.age = 99
    repo.save(thing1, thing2)
    assert repo.match(UniqueFullyNamedPerson).count() == 1


def test_pull(repo, thing):
    repo.save(thing)
    repo.graph.update("MATCH (a) WHERE id(a) = $x SET a.name = $name",
                      {"x": thing.__node__.identity, "name": "Alice"})
    repo.pull(thing)
    assert thing.name == "Alice"


def test_push(repo, thing):
    repo.push(thing)
    assert thing.__node__.graph is repo.graph
    assert thing.__node__.identity is not None


def test_primary_value_with_single_key(repo):
    person = UniquePerson()
    assert person.__primaryvalue__ == "Alice"


def test_primary_value_with_composite_key(repo):
    person = UniqueFullyNamedPerson()
    assert person.__primaryvalue__ == ("Alice", "Smith")

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


from pytest import raises

from py2neo.ogm import Model, RelatedTo


class Person(Model):
    friends = RelatedTo("Person", "FRIEND")


def test_add_new_and_existing():
    alice = Person()
    bob = Person()
    assert len(alice.friends) == 0
    assert alice.friends.add(bob) == 1
    assert len(alice.friends) == 1
    assert alice.friends.add(bob) == 0
    assert len(alice.friends) == 1


def test_add_non_model():
    alice = Person()
    with raises(TypeError):
        alice.friends.add(object())

#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright 2011-2014, Nigel Small
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

from py2neo.http import Node, Relationship, cast_to_relationship


def test_can_cast_rel(graph):
    a, b, ab = graph.create({}, {}, (0, "KNOWS", 1))
    casted = cast_to_relationship(ab)
    assert isinstance(casted, Relationship)
    assert casted.bound
    assert casted.start_node == a
    assert casted.type == "KNOWS"
    assert casted.end_node == b


def test_cannot_cast_0_tuple():
    try:
        cast_to_relationship(())
        assert False
    except TypeError:
        assert True


def test_cannot_cast_1_tuple():
    try:
        cast_to_relationship(("Alice",))
        assert False
    except TypeError:
        assert True


def test_cannot_cast_2_tuple():
    try:
        cast_to_relationship(("Alice", "KNOWS"))
        assert False
    except TypeError:
        assert True


def test_can_cast_3_tuple():
    casted = cast_to_relationship(("Alice", "KNOWS", "Bob"))
    assert isinstance(casted, Relationship)
    assert not casted.bound
    assert casted.start_node == Node("Alice")
    assert casted.type == "KNOWS"
    assert casted.end_node == Node("Bob")


def test_can_cast_3_tuple_with_unbound_rel():
    casted = cast_to_relationship(("Alice", ("KNOWS", {"since": 1999}), "Bob"))
    assert isinstance(casted, Relationship)
    assert not casted.bound
    assert casted.start_node == Node("Alice")
    assert casted.type == "KNOWS"
    assert casted.end_node == Node("Bob")
    assert casted["since"] == 1999


def test_can_cast_4_tuple():
    casted = cast_to_relationship(("Alice", "KNOWS", "Bob", {"since": 1999}))
    assert isinstance(casted, Relationship)
    assert not casted.bound
    assert casted.start_node == Node("Alice")
    assert casted.type == "KNOWS"
    assert casted.end_node == Node("Bob")
    assert casted["since"] == 1999


def test_cannot_cast_6_tuple():
    try:
        cast_to_relationship(("Alice", "KNOWS", "Bob", "foo", "bar", "baz"))
        assert False
    except TypeError:
        assert True


def test_cannot_cast_0_args():
    try:
        cast_to_relationship()
        assert False
    except TypeError:
        assert True


def test_cannot_cast_1_arg():
    try:
        cast_to_relationship("Alice")
        assert False
    except TypeError:
        assert True


def test_cannot_cast_2_args():
    try:
        cast_to_relationship("Alice", "KNOWS")
        assert False
    except TypeError:
        assert True


def test_can_cast_3_args():
    casted = cast_to_relationship("Alice", "KNOWS", "Bob")
    assert isinstance(casted, Relationship)
    assert not casted.bound
    assert casted.start_node == Node("Alice")
    assert casted.type == "KNOWS"
    assert casted.end_node == Node("Bob")


def test_can_cast_3_args_with_mid_tuple():
    casted = cast_to_relationship("Alice", ("KNOWS", {"since": 1999}), "Bob")
    assert isinstance(casted, Relationship)
    assert not casted.bound
    assert casted.start_node == Node("Alice")
    assert casted.type == "KNOWS"
    assert casted.end_node == Node("Bob")
    assert casted["since"] == 1999


def test_can_cast_3_args_with_mid_tuple_and_props():
    casted = cast_to_relationship("Alice", ("KNOWS", {"since": 1999}), "Bob", foo="bar")
    assert isinstance(casted, Relationship)
    assert not casted.bound
    assert casted.start_node == Node("Alice")
    assert casted.type == "KNOWS"
    assert casted.end_node == Node("Bob")
    assert casted["since"] == 1999
    assert casted["foo"] == "bar"


def test_can_cast_kwargs():
    casted = cast_to_relationship("Alice", "KNOWS", "Bob", since=1999)
    assert isinstance(casted, Relationship)
    assert not casted.bound
    assert casted.start_node == Node("Alice")
    assert casted.type == "KNOWS"
    assert casted.end_node == Node("Bob")
    assert casted["since"] == 1999


def test_can_cast_4_args():
    casted = cast_to_relationship("Alice", "KNOWS", "Bob", {"since": 1999})
    assert isinstance(casted, Relationship)
    assert not casted.bound
    assert casted.start_node == Node("Alice")
    assert casted.type == "KNOWS"
    assert casted.end_node == Node("Bob")
    assert casted["since"] == 1999


def test_can_cast_4_args_and_props():
    casted = cast_to_relationship("Alice", "KNOWS", "Bob", {"since": 1999}, foo="bar")
    assert isinstance(casted, Relationship)
    assert not casted.bound
    assert casted.start_node == Node("Alice")
    assert casted.type == "KNOWS"
    assert casted.end_node == Node("Bob")
    assert casted["since"] == 1999
    assert casted["foo"] == "bar"

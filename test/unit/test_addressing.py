#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright (c) "Neo4j"
# Neo4j Sweden AB [https://neo4j.com]
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

from py2neo.addressing import Address, IPv4Address, IPv6Address


def test_parsing_simple_address():
    a = Address.parse("localhost:80")
    assert isinstance(a, IPv4Address)
    assert a.host == "localhost"
    assert a.port == 80
    assert a.port_number == 80
    assert repr(a)
    assert str(a) == "localhost:80"


def test_parsing_address_with_named_port():
    a = Address.parse("localhost:http")
    assert isinstance(a, IPv4Address)
    assert a.host == "localhost"
    assert a.port == "http"
    assert a.port_number == 80
    assert repr(a)
    assert str(a) == "localhost:http"


def test_parsing_address_with_named_bolt_port():
    a = Address.parse("localhost:bolt")
    assert isinstance(a, IPv4Address)
    assert a.host == "localhost"
    assert a.port == "bolt"
    assert a.port_number == 7687
    assert repr(a)
    assert str(a) == "localhost:bolt"


def test_parsing_simple_ipv6_address():
    a = Address.parse("[::1]:80")
    assert isinstance(a, IPv6Address)
    assert a.host == "::1"
    assert a.port == 80
    assert a.port_number == 80
    assert repr(a)
    assert str(a) == "[::1]:80"


def test_parsing_ipv6_address_with_named_port():
    a = Address.parse("[::1]:http")
    assert isinstance(a, IPv6Address)
    assert a.host == "::1"
    assert a.port == "http"
    assert a.port_number == 80
    assert repr(a)
    assert str(a) == "[::1]:http"


def test_address_with_wrong_number_of_parts():
    with raises(ValueError):
        _ = Address((1, 2, 3, 4, 5))


def test_address_with_unknown_port_name():
    a = Address(("localhost", "x"))
    with raises(ValueError):
        _ = a.port_number


def test_address_from_address():
    a1 = Address.parse("[::1]:http")
    a2 = Address(a1)
    assert a2 is a1

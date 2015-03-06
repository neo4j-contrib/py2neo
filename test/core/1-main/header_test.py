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


from py2neo.core import _add_header, _get_headers


def test_can_add_and_retrieve_global_header():
    _add_header("Key1", "Value1")
    headers = _get_headers("localhost:7474")
    assert headers["Key1"] == "Value1"


def test_can_add_and_retrieve_header_for_specific_host_port():
    _add_header("Key1", "Value1", "example.com:7474")
    _add_header("Key1", "Value2", "example.net:7474")
    headers = _get_headers("example.com:7474")
    assert headers["Key1"] == "Value1"


def test_can_add_and_retrieve_multiple_headers_for_specific_host_port():
    _add_header("Key1", "Value1", "example.com:7474")
    _add_header("Key2", "Value2", "example.com:7474")
    headers = _get_headers("example.com:7474")
    assert headers["Key1"] == "Value1"
    assert headers["Key2"] == "Value2"

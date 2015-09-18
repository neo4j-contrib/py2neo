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


from py2neo.http import RootView


def test_can_create_root_with_trailing_slash():
    uri = "http://localhost:7474/"
    root = RootView(uri)
    assert root.uri == uri
    index = root.resource.get().content
    assert "data" in index


def test_can_create_root_without_trailing_slash():
    uri = "http://localhost:7474/"
    root = RootView(uri[:-1])
    assert root.uri == uri
    index = root.resource.get().content
    assert "data" in index


def test_same_uri_gives_same_instance():
    uri = "http://localhost:7474/"
    root_1 = RootView(uri)
    root_2 = RootView(uri)
    assert root_1 is root_2


def test_root_equality():
    uri = "http://localhost:7474/"
    root_1 = RootView(uri)
    root_2 = RootView(uri)
    assert root_1 == root_2


def test_root_inequality():
    uri = "http://localhost:7474/"
    root_1 = RootView(uri)
    root_2 = RootView("http://remotehost:7474/")
    assert root_1 != root_2


def test_root_is_not_equal_to_non_root():
    uri = "http://localhost:7474/"
    root = RootView(uri)
    assert root != object()

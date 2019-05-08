#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright 2011-2019, Nigel Small
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


from py2neo import Database, Graph


def test_can_generate_graph(database):
    graph = database["data"]
    assert isinstance(graph, Graph)


def test_repr(database):
    assert repr(database).startswith("<Database uri=")


def test_same_uri_gives_same_instance(database):
    uri = database.uri
    dbms_1 = Database(uri)
    dbms_2 = Database(uri)
    assert dbms_1 is dbms_2


def test_dbms_equality(database):
    uri = database.uri
    dbms_1 = Database(uri)
    dbms_2 = Database(uri)
    assert dbms_1 == dbms_2
    assert hash(dbms_1) == hash(dbms_2)


def test_dbms_is_not_equal_to_non_dbms(database):
    assert database != object()


def test_dbms_metadata(database):
    assert database.kernel_start_time
    assert database.kernel_version
    assert database.store_creation_time
    assert database.store_id
    assert database.primitive_counts
    assert database.store_file_sizes
    assert database.config


def test_database_name(database):
    assert database.name == "graph.db"


def test_kernel_version(database):
    version = database.kernel_version
    assert isinstance(version, tuple)
    assert 3 <= len(version) <= 4
    assert isinstance(version[0], int)
    assert isinstance(version[1], int)
    assert isinstance(version[2], int)


def test_can_get_set_of_graphs_in_database(database):
    assert set(database) == {"data"}


def test_can_get_dictionary_of_databases(database):
    databases = dict(database)
    assert len(databases) == 1
    assert databases["data"] is database.default_graph

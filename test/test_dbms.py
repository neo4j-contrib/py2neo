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


from py2neo import DBMS, remote
from test.util import GraphTestCase


class DBMSTestCase(GraphTestCase):

    def test_can_create_dbms_with_bolt_uri(self):
        uri = "bolt://localhost/"
        dbms = DBMS(uri)
        assert repr(dbms).startswith("<DBMS")
        assert remote(dbms).uri == "http://localhost:7474/"
        index = remote(dbms).get().content
        assert "data" in index

    def test_can_create_dbms_with_settings(self):
        uri = "http://127.0.0.1:7474/"
        dbms = DBMS(host="127.0.0.1")
        assert repr(dbms).startswith("<DBMS")
        assert remote(dbms).uri == uri
        index = remote(dbms).get().content
        assert "data" in index

    def test_can_create_dbms_with_trailing_slash(self):
        uri = "http://localhost:7474/"
        dbms = DBMS(uri)
        assert repr(dbms).startswith("<DBMS")
        assert remote(dbms).uri == uri
        index = remote(dbms).get().content
        assert "data" in index

    def test_can_create_dbms_without_trailing_slash(self):
        uri = "http://localhost:7474/"
        dbms = DBMS(uri[:-1])
        assert remote(dbms).uri == uri
        index = remote(dbms).get().content
        assert "data" in index

    def test_same_uri_gives_same_instance(self):
        uri = "http://localhost:7474/"
        dbms_1 = DBMS(uri)
        dbms_2 = DBMS(uri)
        assert dbms_1 is dbms_2

    def test_dbms_equality(self):
        uri = "http://localhost:7474/"
        dbms_1 = DBMS(uri)
        dbms_2 = DBMS(uri)
        assert dbms_1 == dbms_2
        assert hash(dbms_1) == hash(dbms_2)

    def test_dbms_inequality(self):
        uri = "http://localhost:7474/"
        dbms_1 = DBMS(uri)
        dbms_2 = DBMS("http://remotehost:7474/")
        assert dbms_1 != dbms_2
        assert hash(dbms_1) != hash(dbms_2)

    def test_dbms_is_not_equal_to_non_dbms(self):
        uri = "http://localhost:7474/"
        dbms = DBMS(uri)
        assert dbms != object()

    def test_dbms_metadata(self):
        assert self.dbms.kernel_start_time
        assert self.dbms.kernel_version
        assert self.dbms.store_creation_time
        assert self.dbms.store_id
        assert self.dbms.primitive_counts
        assert self.dbms.store_file_sizes
        assert self.dbms.config

    def test_database_name(self):
        _ = self.dbms.database_name

    def test_store_directory(self):
        _ = self.dbms.store_directory

    def test_kernel_version(self):
        version = self.dbms.kernel_version
        assert isinstance(version, tuple)
        assert 3 <= len(version) <= 4
        assert isinstance(version[0], int)
        assert isinstance(version[1], int)
        assert isinstance(version[2], int)

    def test_can_get_list_of_databases(self):
        databases = list(self.dbms)
        assert databases == ["data"]

    def test_can_get_dictionary_of_databases(self):
        databases = dict(self.dbms)
        assert len(databases) == 1
        assert databases["data"] is self.graph

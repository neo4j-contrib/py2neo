#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright 2011-2015, Nigel Small
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


from py2neo import DBMS
from test.util import Py2neoTestCase


class DBMSTestCase(Py2neoTestCase):

    def test_can_create_dbms_with_trailing_slash(self):
        uri = "http://localhost:7474/"
        dbms = DBMS(uri)
        assert dbms.uri == uri
        index = dbms.resource.get().content
        assert "data" in index

    def test_can_create_dbms_without_trailing_slash(self):
        uri = "http://localhost:7474/"
        dbms = DBMS(uri[:-1])
        assert dbms.uri == uri
        index = dbms.resource.get().content
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

    def test_dbms_inequality(self):
        uri = "http://localhost:7474/"
        dbms_1 = DBMS(uri)
        dbms_2 = DBMS("http://remotehost:7474/")
        assert dbms_1 != dbms_2

    def test_dbms_is_not_equal_to_non_dbms(v):
        uri = "http://localhost:7474/"
        dbms = DBMS(uri)
        assert dbms != object()

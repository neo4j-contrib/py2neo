#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright 2011-2017, Nigel Small
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


from unittest import skipUnless

from neo4j.exceptions import AuthError

from py2neo.addressing import GraphServiceAddress, register_graph_service, keyring, authenticate
from py2neo.http import HTTP

from test.util import GraphTestCase, HTTPGraphTestCase


class AuthorizationFailedTestCase(HTTPGraphTestCase):

    def test_can_raise_unauthorized_on_get(self):
        with self.assertRaises(AuthError):
            _ = HTTP("http://foo:bar@127.0.0.1:7474/db/data/").get_json("")

    def test_can_raise_unauthorized_on_post(self):
        with self.assertRaises(AuthError):
            _ = HTTP("http://foo:bar@127.0.0.1:7474/db/data/").post("", {}, expected=(201,)).close()

    def test_can_raise_unauthorized_on_delete(self):
        with self.assertRaises(AuthError):
            _ = HTTP("http://foo:bar@127.0.0.1:7474/db/data/").delete("", expected=(204,)).close()


class ServiceAddressTestCase(GraphTestCase):

    def test_service_address_repr(self):
        address = GraphServiceAddress()
        assert repr(address).startswith("<GraphServiceAddress")


class ServiceRegistrationTestCase(GraphTestCase):

    def setUp(self):
        self.keyring = {}
        self.keyring.update(keyring)
        keyring.clear()

    def tearDown(self):
        keyring.update(self.keyring)

    def test_can_register_service_via_uri(self):
        register_graph_service("http://camelot:1234/")
        address = GraphServiceAddress("http://camelot:1234/")
        assert address.http_uri in keyring
        assert keyring[address.http_uri] is None

    def test_can_register_service_and_password_via_uri(self):
        register_graph_service("http://camelot:1234/", user="arthur", password="excalibur")
        address = GraphServiceAddress("http://camelot:1234/")
        assert address.http_uri in keyring
        assert keyring[address.http_uri].user == "arthur"
        assert keyring[address.http_uri].password == "excalibur"

    def test_can_register_service_and_password_through_authenticate_function(self):
        authenticate("camelot:1234", "arthur", "excalibur")
        address = GraphServiceAddress("http://camelot:1234/")
        assert address.http_uri in keyring
        assert keyring[address.http_uri].user == "arthur"
        assert keyring[address.http_uri].password == "excalibur"

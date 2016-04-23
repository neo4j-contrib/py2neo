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


from unittest import skipUnless

from py2neo.database import DBMS, Resource, ServerAddress, register_server, keyring, authenticate, Unauthorized
from test.util import DatabaseTestCase


dbms = DBMS()
supports_auth = dbms.supports_auth


class AuthorizationFailedTestCase(DatabaseTestCase):

    @skipUnless(supports_auth, "Auth not supported")
    def test_can_raise_unauthorized_on_get(self):
        with self.assertRaises(Unauthorized):
            _ = Resource("http://foo:bar@127.0.0.1:7474/db/data/").get().content

    @skipUnless(supports_auth, "Auth not supported")
    def test_can_raise_unauthorized_on_put(self):
        with self.assertRaises(Unauthorized):
            _ = Resource("http://foo:bar@127.0.0.1:7474/db/data/").put({}).content

    @skipUnless(supports_auth, "Auth not supported")
    def test_can_raise_unauthorized_on_post(self):
        with self.assertRaises(Unauthorized):
            _ = Resource("http://foo:bar@127.0.0.1:7474/db/data/").post({}).content

    @skipUnless(supports_auth, "Auth not supported")
    def test_can_raise_unauthorized_on_delete(self):
        with self.assertRaises(Unauthorized):
            _ = Resource("http://foo:bar@127.0.0.1:7474/db/data/").delete().content


class ServerAddressTestCase(DatabaseTestCase):

    def test_server_address_repr(self):
        address = ServerAddress()
        assert repr(address).startswith("<ServerAddress")


class ServerRegistrationTestCase(DatabaseTestCase):

    def setUp(self):
        self.keyring = {}
        self.keyring.update(keyring)
        keyring.clear()

    def tearDown(self):
        keyring.update(self.keyring)

    def test_can_register_server_via_uri(self):
        register_server("http://camelot:1234/")
        expected_address = ServerAddress("http://camelot:1234/")
        assert expected_address in keyring
        assert keyring[expected_address] is None

    def test_can_register_server_and_password_via_uri(self):
        register_server("http://camelot:1234/", user="arthur", password="excalibur")
        expected_address = ServerAddress("http://camelot:1234/")
        assert expected_address in keyring
        assert keyring[expected_address].user == "arthur"
        assert keyring[expected_address].password == "excalibur"

    def test_can_register_server_and_password_through_authenticate_function(self):
        authenticate("camelot:1234", "arthur", "excalibur")
        expected_address = ServerAddress("http://camelot:1234/")
        assert expected_address in keyring
        assert keyring[expected_address].user == "arthur"
        assert keyring[expected_address].password == "excalibur"

#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright 2011-2020, Nigel Small
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


from unittest import TestCase

from py2neo.internal.connectors import get_connection_data
from py2neo.meta import bolt_user_agent, http_user_agent


class AddressingTestCase(TestCase):

    def test_bolt_uri_only(self):
        data = get_connection_data("bolt://host:9999")
        del data["hash"]
        self.assertEqual(data, {
            'auth': ('neo4j', 'password'),
            'host': 'host',
            'password': 'password',
            'port': 9999,
            'scheme': 'bolt',
            'secure': False,
            'verified': False,
            'uri': 'bolt://host:9999',
            'user': 'neo4j',
            'user_agent': bolt_user_agent(),
        })

    def test_http_uri_only(self):
        data = get_connection_data("http://host:9999")
        del data["hash"]
        self.assertEqual(data, {
            'auth': ('neo4j', 'password'),
            'host': 'host',
            'password': 'password',
            'port': 9999,
            'scheme': 'http',
            'secure': False,
            'verified': False,
            'uri': 'http://host:9999',
            'user': 'neo4j',
            'user_agent': http_user_agent(),
        })

    def test_http_uri_wth_secure(self):
        data = get_connection_data("http://host:9999", secure=True)
        del data["hash"]
        self.assertEqual(data, {
            'auth': ('neo4j', 'password'),
            'host': 'host',
            'password': 'password',
            'port': 9999,
            'scheme': 'https',
            'secure': True,
            'verified': False,
            'uri': 'https://host:9999',
            'user': 'neo4j',
            'user_agent': http_user_agent(),
        })

    def test_http_uri_wth_secure_and_verified(self):
        data = get_connection_data("http://host:9999", secure=True, verified=True)
        del data["hash"]
        self.assertEqual(data, {
            'auth': ('neo4j', 'password'),
            'host': 'host',
            'password': 'password',
            'port': 9999,
            'scheme': 'https',
            'secure': True,
            'verified': True,
            'uri': 'https://host:9999',
            'user': 'neo4j',
            'user_agent': http_user_agent(),
        })

    def test_https_uri_only(self):
        data = get_connection_data("https://host:9999")
        del data["hash"]
        self.assertEqual(data, {
            'auth': ('neo4j', 'password'),
            'host': 'host',
            'password': 'password',
            'port': 9999,
            'scheme': 'https',
            'secure': True,
            'verified': False,
            'uri': 'https://host:9999',
            'user': 'neo4j',
            'user_agent': http_user_agent(),
        })

    def test_https_uri_without_secure(self):
        data = get_connection_data("https://host:9999", secure=False)
        del data["hash"]
        self.assertEqual(data, {
            'auth': ('neo4j', 'password'),
            'host': 'host',
            'password': 'password',
            'port': 9999,
            'scheme': 'http',
            'secure': False,
            'verified': False,
            'uri': 'http://host:9999',
            'user': 'neo4j',
            'user_agent': http_user_agent(),
        })

    def test_uri_and_scheme(self):
        data = get_connection_data("bolt://host:9999", scheme="http")
        del data["hash"]
        self.assertEqual(data, {
            'auth': ('neo4j', 'password'),
            'host': 'host',
            'password': 'password',
            'port': 9999,
            'scheme': 'http',
            'secure': False,
            'verified': False,
            'uri': 'http://host:9999',
            'user': 'neo4j',
            'user_agent': http_user_agent(),
        })

    def test_uri_and_host(self):
        data = get_connection_data("bolt://host:9999", host="other")
        del data["hash"]
        self.assertEqual(data, {
            'auth': ('neo4j', 'password'),
            'host': 'other',
            'password': 'password',
            'port': 9999,
            'scheme': 'bolt',
            'secure': False,
            'verified': False,
            'uri': 'bolt://other:9999',
            'user': 'neo4j',
            'user_agent': bolt_user_agent(),
        })

    def test_uri_and_port(self):
        data = get_connection_data("bolt://host:8888", port=8888)
        del data["hash"]
        self.assertEqual(data, {
            'auth': ('neo4j', 'password'),
            'host': 'host',
            'password': 'password',
            'port': 8888,
            'scheme': 'bolt',
            'secure': False,
            'verified': False,
            'uri': 'bolt://host:8888',
            'user': 'neo4j',
            'user_agent': bolt_user_agent(),
        })

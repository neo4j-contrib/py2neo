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

from py2neo.connect import ConnectionProfile
from py2neo.connect.addressing import IPv4Address


class ConnectionProfileTestCase(TestCase):

    def test_default_profile(self):
        data = ConnectionProfile().to_dict()
        self.assertEqual(data, {
            'address': IPv4Address(('localhost', 7687)),
            'auth': ('neo4j', 'password'),
            'host': 'localhost',
            'password': 'password',
            'port': 7687,
            'port_number': 7687,
            'protocol': 'bolt',
            'scheme': 'bolt',
            'secure': False,
            'verify': True,
            'uri': 'bolt://neo4j@localhost:7687',
            'user': 'neo4j',
        })

    def test_bolt_uri_only(self):
        data = ConnectionProfile("bolt://host:9999").to_dict()
        self.assertEqual(data, {
            'address': IPv4Address(('host', 9999)),
            'auth': ('neo4j', 'password'),
            'host': 'host',
            'password': 'password',
            'port': 9999,
            'port_number': 9999,
            'protocol': 'bolt',
            'scheme': 'bolt',
            'secure': False,
            'verify': True,
            'uri': 'bolt://neo4j@host:9999',
            'user': 'neo4j',
        })

    def test_http_uri_only(self):
        data = ConnectionProfile("http://host:9999").to_dict()
        self.assertEqual(data, {
            'address': IPv4Address(('host', 9999)),
            'auth': ('neo4j', 'password'),
            'host': 'host',
            'password': 'password',
            'port': 9999,
            'port_number': 9999,
            'protocol': 'http',
            'scheme': 'http',
            'secure': False,
            'verify': True,
            'uri': 'http://neo4j@host:9999',
            'user': 'neo4j',
        })

    def test_http_uri_wth_secure(self):
        data = ConnectionProfile("http://host:9999", secure=True).to_dict()
        self.assertEqual(data, {
            'address': IPv4Address(('host', 9999)),
            'auth': ('neo4j', 'password'),
            'host': 'host',
            'password': 'password',
            'port': 9999,
            'port_number': 9999,
            'protocol': 'http',
            'scheme': 'https',
            'secure': True,
            'verify': True,
            'uri': 'https://neo4j@host:9999',
            'user': 'neo4j',
        })

    def test_http_uri_wth_secure_and_no_verify(self):
        data = ConnectionProfile("http://host:9999", secure=True, verify=False).to_dict()
        self.assertEqual(data, {
            'address': IPv4Address(('host', 9999)),
            'auth': ('neo4j', 'password'),
            'host': 'host',
            'password': 'password',
            'port': 9999,
            'port_number': 9999,
            'protocol': 'http',
            'scheme': 'http+ssc',
            'secure': True,
            'verify': False,
            'uri': 'http+ssc://neo4j@host:9999',
            'user': 'neo4j',
        })

    def test_https_uri_only(self):
        data = ConnectionProfile("https://host:9999").to_dict()
        self.assertEqual(data, {
            'address': IPv4Address(('host', 9999)),
            'auth': ('neo4j', 'password'),
            'host': 'host',
            'password': 'password',
            'port': 9999,
            'port_number': 9999,
            'protocol': 'http',
            'scheme': 'https',
            'secure': True,
            'verify': True,
            'uri': 'https://neo4j@host:9999',
            'user': 'neo4j',
        })

    def test_https_uri_without_secure(self):
        data = ConnectionProfile("https://host:9999", secure=False).to_dict()
        self.assertEqual(data, {
            'address': IPv4Address(('host', 9999)),
            'auth': ('neo4j', 'password'),
            'host': 'host',
            'password': 'password',
            'port': 9999,
            'port_number': 9999,
            'protocol': 'http',
            'scheme': 'http',
            'secure': False,
            'verify': True,
            'uri': 'http://neo4j@host:9999',
            'user': 'neo4j',
        })

    def test_uri_and_scheme(self):
        data = ConnectionProfile("bolt://host:9999", scheme="http").to_dict()
        self.assertEqual(data, {
            'address': IPv4Address(('host', 9999)),
            'auth': ('neo4j', 'password'),
            'host': 'host',
            'password': 'password',
            'port': 9999,
            'port_number': 9999,
            'protocol': 'http',
            'scheme': 'http',
            'secure': False,
            'verify': True,
            'uri': 'http://neo4j@host:9999',
            'user': 'neo4j',
        })

    def test_uri_and_host(self):
        data = ConnectionProfile("bolt://host:9999", host="other").to_dict()
        self.assertEqual(data, {
            'address': IPv4Address(('other', 9999)),
            'auth': ('neo4j', 'password'),
            'host': 'other',
            'password': 'password',
            'port': 9999,
            'port_number': 9999,
            'protocol': 'bolt',
            'scheme': 'bolt',
            'secure': False,
            'verify': True,
            'uri': 'bolt://neo4j@other:9999',
            'user': 'neo4j',
        })

    def test_uri_and_port(self):
        data = ConnectionProfile("bolt://host:8888", port=8888).to_dict()
        self.assertEqual(data, {
            'address': IPv4Address(('host', 8888)),
            'auth': ('neo4j', 'password'),
            'host': 'host',
            'password': 'password',
            'port': 8888,
            'port_number': 8888,
            'protocol': 'bolt',
            'scheme': 'bolt',
            'secure': False,
            'verify': True,
            'uri': 'bolt://neo4j@host:8888',
            'user': 'neo4j',
        })

    def test_bolt_uri_with_ssc(self):
        data = ConnectionProfile("bolt+ssc://host:9999").to_dict()
        self.assertEqual(data, {
            'address': IPv4Address(('host', 9999)),
            'auth': ('neo4j', 'password'),
            'host': 'host',
            'password': 'password',
            'port': 9999,
            'port_number': 9999,
            'protocol': 'bolt',
            'scheme': 'bolt+ssc',
            'secure': True,
            'verify': False,
            'uri': 'bolt+ssc://neo4j@host:9999',
            'user': 'neo4j',
        })

    def test_bolt_uri_with_user(self):
        data = ConnectionProfile("bolt://bob@host:9999").to_dict()
        self.assertEqual(data, {
            'address': IPv4Address(('host', 9999)),
            'auth': ('bob', 'password'),
            'host': 'host',
            'password': 'password',
            'port': 9999,
            'port_number': 9999,
            'protocol': 'bolt',
            'scheme': 'bolt',
            'secure': False,
            'verify': True,
            'uri': 'bolt://bob@host:9999',
            'user': 'bob',
        })

    def test_secure_and_verify(self):
        data = ConnectionProfile(secure=True, verify=True).to_dict()
        self.assertEqual(data, {
            'address': IPv4Address(('localhost', 7687)),
            'auth': ('neo4j', 'password'),
            'host': 'localhost',
            'password': 'password',
            'port': 7687,
            'port_number': 7687,
            'protocol': 'bolt',
            'scheme': 'bolt+s',
            'secure': True,
            'verify': True,
            'uri': 'bolt+s://neo4j@localhost:7687',
            'user': 'neo4j',
        })

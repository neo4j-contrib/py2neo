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


from pytest import raises

from py2neo.client.config import bolt_user_agent, http_user_agent, ConnectionProfile
from py2neo.wiring import IPv4Address


def test_bolt_user_agent():
    agent = bolt_user_agent()
    assert agent.startswith("py2neo")


def test_http_user_agent():
    agent = http_user_agent()
    assert agent.startswith("py2neo")


def test_default_profile():
    data = dict(ConnectionProfile())
    assert data == {
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
    }


def test_profile_from_profile():
    prof1 = ConnectionProfile(password="secret")
    prof2 = ConnectionProfile(prof1)
    data = dict(prof2)
    assert data == {
        'address': IPv4Address(('localhost', 7687)),
        'auth': ('neo4j', 'secret'),
        'host': 'localhost',
        'password': 'secret',
        'port': 7687,
        'port_number': 7687,
        'protocol': 'bolt',
        'scheme': 'bolt',
        'secure': False,
        'verify': True,
        'uri': 'bolt://neo4j@localhost:7687',
        'user': 'neo4j',
    }


def test_profile_from_dict():
    dict1 = {
        'address': IPv4Address(('localhost', 7687)),
        'auth': ('neo4j', 'dictionary'),
        'host': 'localhost',
        'password': 'dictionary',
        'port': 7687,
        'port_number': 7687,
        'protocol': 'bolt',
        'scheme': 'bolt',
        'secure': False,
        'verify': True,
        'uri': 'bolt://neo4j@localhost:7687',
        'user': 'neo4j',
    }
    prof1 = ConnectionProfile(dict1)
    data = dict(prof1)
    assert data == dict1


def test_profile_from_invalid_argument():
    with raises(TypeError):
        _ = ConnectionProfile(object())


def test_bolt_uri_only():
    data = dict(ConnectionProfile("bolt://host:9999"))
    assert data == {
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
    }


def test_http_uri_only():
    data = dict(ConnectionProfile("http://host:9999"))
    assert data == {
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
    }


def test_http_uri_wth_secure():
    data = dict(ConnectionProfile("http://host:9999", secure=True))
    assert data == {
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
    }


def test_http_uri_wth_secure_and_no_verify():
    data = dict(ConnectionProfile("http://host:9999", secure=True, verify=False))
    assert data == {
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
    }


def test_https_uri_only():
    data = dict(ConnectionProfile("https://host:9999"))
    assert data == {
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
    }


def test_https_uri_without_secure():
    data = dict(ConnectionProfile("https://host:9999", secure=False))
    assert data == {
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
    }


def test_uri_and_scheme():
    data = dict(ConnectionProfile("bolt://host:9999", scheme="http"))
    assert data == {
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
    }


def test_uri_and_host():
    data = dict(ConnectionProfile("bolt://host:9999", host="other"))
    assert data == {
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
    }


def test_uri_and_port():
    data = dict(ConnectionProfile("bolt://host:8888", port=8888))
    assert data == {
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
    }


def test_bolt_uri_with_ssc():
    data = dict(ConnectionProfile("bolt+ssc://host:9999"))
    assert data == {
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
    }


def test_bolt_uri_with_user():
    data = dict(ConnectionProfile("bolt://bob@host:9999"))
    assert data == {
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
    }


def test_secure_and_verify():
    data = dict(ConnectionProfile(secure=True, verify=True))
    assert data == {
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
    }


def test_equality():
    prof1 = ConnectionProfile()
    prof2 = ConnectionProfile()
    assert prof1 == prof2


def test_hash_equality():
    prof1 = ConnectionProfile()
    prof2 = ConnectionProfile()
    assert hash(prof1) == hash(prof2)


def test_length():
    prof1 = ConnectionProfile()
    assert len(prof1) == 12


def test_bolt_default_port():
    data = dict(ConnectionProfile("bolt://host"))
    assert data == {
        'address': IPv4Address(('host', 7687)),
        'auth': ('neo4j', 'password'),
        'host': 'host',
        'password': 'password',
        'port': 7687,
        'port_number': 7687,
        'protocol': 'bolt',
        'scheme': 'bolt',
        'secure': False,
        'verify': True,
        'uri': 'bolt://neo4j@host:7687',
        'user': 'neo4j',
    }


def test_http_default_port():
    data = dict(ConnectionProfile("http://host"))
    assert data == {
        'address': IPv4Address(('host', 7474)),
        'auth': ('neo4j', 'password'),
        'host': 'host',
        'password': 'password',
        'port': 7474,
        'port_number': 7474,
        'protocol': 'http',
        'scheme': 'http',
        'secure': False,
        'verify': True,
        'uri': 'http://neo4j@host:7474',
        'user': 'neo4j',
    }


def test_https_default_port():
    data = dict(ConnectionProfile("https://host"))
    assert data == {
        'address': IPv4Address(('host', 7473)),
        'auth': ('neo4j', 'password'),
        'host': 'host',
        'password': 'password',
        'port': 7473,
        'port_number': 7473,
        'protocol': 'http',
        'scheme': 'https',
        'secure': True,
        'verify': True,
        'uri': 'https://neo4j@host:7473',
        'user': 'neo4j',
    }


def test_explicit_auth_tuple():
    data = dict(ConnectionProfile(auth=("bob", "secret")))
    assert data == {
        'address': IPv4Address(('localhost', 7687)),
        'auth': ('bob', 'secret'),
        'host': 'localhost',
        'password': 'secret',
        'port': 7687,
        'port_number': 7687,
        'protocol': 'bolt',
        'scheme': 'bolt',
        'secure': False,
        'verify': True,
        'uri': 'bolt://bob@localhost:7687',
        'user': 'bob',
    }


def test_explicit_auth_string():
    profile = ConnectionProfile(auth="bob:secret")
    data = dict(profile)
    assert data == {
        'address': IPv4Address(('localhost', 7687)),
        'auth': ('bob', 'secret'),
        'host': 'localhost',
        'password': 'secret',
        'port': 7687,
        'port_number': 7687,
        'protocol': 'bolt',
        'scheme': 'bolt',
        'secure': False,
        'verify': True,
        'uri': 'bolt://bob@localhost:7687',
        'user': 'bob',
    }


def test_explicit_address_string():
    profile = ConnectionProfile(address="victor:4291")
    assert profile.host == "victor"
    assert profile.port == 4291


def test_get_non_existent_key():
    prof = ConnectionProfile()
    with raises(KeyError):
        _ = prof["routing"]


def test_profile_repr():
    prof = ConnectionProfile()
    assert repr(prof).startswith("ConnectionProfile")


def test_uri_env_var():
    from py2neo.client import config
    config.NEO4J_URI = "http://alice@somewhere:8899"
    prof = ConnectionProfile()
    assert prof.uri == "http://alice@somewhere:8899"


def test_auth_env_var():
    from py2neo.client import config
    config.NEO4J_AUTH = "alice:python"
    prof = ConnectionProfile()
    assert prof.auth == ("alice", "python")

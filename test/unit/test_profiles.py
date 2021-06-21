#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright 2011-2021, Nigel Small
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


from os import path

from pytest import raises

from py2neo import ConnectionProfile, ServiceProfile
from py2neo.addressing import IPv4Address


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
        'uri': 'bolt://localhost:7687',
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
        'uri': 'bolt://localhost:7687',
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
        'uri': 'bolt://localhost:7687',
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
        'uri': 'bolt://host:9999',
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
        'uri': 'http://host:9999',
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
        'uri': 'https://host:9999',
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
        'uri': 'http+ssc://host:9999',
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
        'uri': 'https://host:9999',
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
        'uri': 'http://host:9999',
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
        'uri': 'http://host:9999',
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
        'uri': 'bolt://other:9999',
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
        'uri': 'bolt://host:8888',
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
        'uri': 'bolt+ssc://host:9999',
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
        'uri': 'bolt://host:9999',
        'user': 'bob',
    }


def test_bolt_uri_with_user_and_password():
    data = dict(ConnectionProfile("bolt://bob:secret@host:9999"))
    assert data == {
        'address': IPv4Address(('host', 9999)),
        'auth': ('bob', 'secret'),
        'host': 'host',
        'password': 'secret',
        'port': 9999,
        'port_number': 9999,
        'protocol': 'bolt',
        'scheme': 'bolt',
        'secure': False,
        'verify': True,
        'uri': 'bolt://host:9999',
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
        'uri': 'bolt+s://localhost:7687',
        'user': 'neo4j',
    }


def test_bad_scheme_extension():
    with raises(ValueError):
        _ = ConnectionProfile("bolt+x://localhost:7687")


def test_bad_protocol():
    p = ConnectionProfile()
    with raises(ValueError):
        p._apply_protocol("x")


def test_bad_settings():
    p = ConnectionProfile()
    with raises(ValueError):
        p._apply_settings(foo="bar")


def test_equality():
    prof1 = ConnectionProfile()
    prof2 = ConnectionProfile()
    assert prof1 == prof2


def test_equality_of_subtypes():
    prof1 = ConnectionProfile()
    prof2 = ServiceProfile()
    assert prof1 != prof2


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
        'uri': 'bolt://host:7687',
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
        'uri': 'http://host:7474',
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
        'uri': 'https://host:7473',
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
        'uri': 'bolt://localhost:7687',
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
        'uri': 'bolt://localhost:7687',
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


def test_profile_str():
    prof = ConnectionProfile()
    s = str(prof)
    assert s.startswith("«")
    assert s.endswith("»")


def test_profile_repr():
    prof = ConnectionProfile()
    assert repr(prof).startswith("ConnectionProfile")


def test_to_dict():
    p = ConnectionProfile()
    d = p.to_dict()
    assert d == {
        'address': IPv4Address(('localhost', 7687)),
        'host': 'localhost',
        'port': 7687,
        'port_number': 7687,
        'protocol': 'bolt',
        'scheme': 'bolt',
        'secure': False,
        'verify': True,
        'uri': 'bolt://localhost:7687',
        'user': 'neo4j',
    }


def test_to_dict_with_password():
    p = ConnectionProfile()
    d = p.to_dict(include_password=True)
    assert d == {
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
        'uri': 'bolt://localhost:7687',
        'user': 'neo4j',
    }


def test_loading_profile_from_file():
    filename = path.join(path.dirname(__file__),
                         "..", "resources", "example.ini")
    prof = ConnectionProfile.from_file(filename, "Neo4j")
    data = dict(prof)
    assert data == {
        'secure': False,
        'verify': True,
        'scheme': 'bolt',
        'user': 'shaggy',
        'password': 'velma',
        'address': IPv4Address(('graph.mystery.inc', 7777)),
        'auth': ('shaggy', 'velma'),
        'host': 'graph.mystery.inc',
        'port': 7777,
        'port_number': 7777,
        'protocol': 'bolt',
        'uri': 'bolt://graph.mystery.inc:7777',
    }


def test_uri_env_var(monkeypatch):
    import py2neo
    monkeypatch.setattr(py2neo, "NEO4J_URI", "http://alice@somewhere:8899")
    prof = ConnectionProfile()
    assert prof.uri == "http://somewhere:8899"


def test_auth_env_var(monkeypatch):
    import py2neo
    monkeypatch.setattr(py2neo, "NEO4J_AUTH", "alice:python")
    prof = ConnectionProfile()
    assert prof.auth == ("alice", "python")


def test_secure_env_var(monkeypatch):
    import py2neo
    monkeypatch.setattr(py2neo, "NEO4J_SECURE", "1")
    prof = ConnectionProfile()
    assert prof.secure


def test_verify_env_var(monkeypatch):
    import py2neo
    monkeypatch.setattr(py2neo, "NEO4J_VERIFY", "1")
    prof = ConnectionProfile()
    assert prof.verify


def test_default_service_profile():
    data = dict(ServiceProfile())
    assert data == {
        'address': IPv4Address(('localhost', 7687)),
        'auth': ('neo4j', 'password'),
        'host': 'localhost',
        'password': 'password',
        'port': 7687,
        'port_number': 7687,
        'protocol': 'bolt',
        'routing': False,
        'scheme': 'bolt',
        'secure': False,
        'verify': True,
        'uri': 'bolt://localhost:7687',
        'user': 'neo4j',
    }


def test_default_service_profile_with_neo4j_uri():
    data = dict(ServiceProfile("neo4j://localhost:7687"))
    assert data == {
        'address': IPv4Address(('localhost', 7687)),
        'auth': ('neo4j', 'password'),
        'host': 'localhost',
        'password': 'password',
        'port': 7687,
        'port_number': 7687,
        'protocol': 'bolt',
        'routing': True,
        'scheme': 'neo4j',
        'secure': False,
        'verify': True,
        'uri': 'neo4j://localhost:7687',
        'user': 'neo4j',
    }


def test_default_service_profile_with_neo4j_plus_s_uri():
    data = dict(ServiceProfile("neo4j+s://localhost:7687"))
    assert data == {
        'address': IPv4Address(('localhost', 7687)),
        'auth': ('neo4j', 'password'),
        'host': 'localhost',
        'password': 'password',
        'port': 7687,
        'port_number': 7687,
        'protocol': 'bolt',
        'routing': True,
        'scheme': 'neo4j+s',
        'secure': True,
        'verify': True,
        'uri': 'neo4j+s://localhost:7687',
        'user': 'neo4j',
    }


def test_default_service_profile_with_neo4j_plus_ssc_uri():
    data = dict(ServiceProfile("neo4j+ssc://localhost:7687"))
    assert data == {
        'address': IPv4Address(('localhost', 7687)),
        'auth': ('neo4j', 'password'),
        'host': 'localhost',
        'password': 'password',
        'port': 7687,
        'port_number': 7687,
        'protocol': 'bolt',
        'routing': True,
        'scheme': 'neo4j+ssc',
        'secure': True,
        'verify': False,
        'uri': 'neo4j+ssc://localhost:7687',
        'user': 'neo4j',
    }


def test_default_service_profile_with_routing_keyword():
    data = dict(ServiceProfile("bolt://localhost:7687", routing=True))
    assert data == {
        'address': IPv4Address(('localhost', 7687)),
        'auth': ('neo4j', 'password'),
        'host': 'localhost',
        'password': 'password',
        'port': 7687,
        'port_number': 7687,
        'protocol': 'bolt',
        'routing': True,
        'scheme': 'neo4j',
        'secure': False,
        'verify': True,
        'uri': 'neo4j://localhost:7687',
        'user': 'neo4j',
    }

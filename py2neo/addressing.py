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


from base64 import b64encode
from os import getenv

from neo4j.v1 import basic_auth

from py2neo.compat import urlsplit


#: Authentication dictionary mapping server addresses to auth details
keyring = {}


class GraphServiceURI(object):

    @classmethod
    def default_scheme(cls, secure=False):
        return "https" if secure else "http"

    @classmethod
    def default_host(cls):
        return "localhost"

    @classmethod
    def default_port(cls, scheme):
        return {
            "http": 7474,
            "https": 7473,
            "bolt": 7687,
            "bolt+routing": 7687,
        }.get(scheme, 7474)

    def __init__(self, uri=None, **parts):
        parsed = urlsplit(uri or "")
        self.secure = parts.get("secure", False)
        self.scheme = parts.get("scheme") or parsed.scheme or self.default_scheme(self.secure)
        if self.scheme == "https":
            self.secure = True
        self.host = (parts.get("%s_host" % self.scheme) or parts.get("host") or
                     parsed.hostname or self.default_host())
        self.port = (parts.get("%s_port" % self.scheme) or parts.get("port") or
                     parsed.port or self.default_port(self.scheme))

    def __repr__(self):
        return repr(self["/"])

    def __hash__(self):
        return hash((self.secure, self.scheme, self.host, self.port))

    def __eq__(self, other):
        return hash(self) == hash(other)

    def __ne__(self, other):
        return not self.__eq__(other)

    def __getitem__(self, path):
        return "%s://%s:%d%s" % (self.scheme, self.host, self.port, path)


class GraphServiceAddress(object):

    def __init__(self, *uris, **parts):
        self.uris = []
        self.http_uri = None
        self.bolt_uri = None
        for raw_uri in uris:
            uri = GraphServiceURI(raw_uri, **parts)
            self.uris.append(uri)
            if self.http_uri is None and uri.scheme in ("http", "https"):
                self.http_uri = uri
            if self.bolt_uri is None and uri.scheme in ("bolt", "bolt+routing"):
                self.bolt_uri = uri

        if self.http_uri is None:
            self.http_uri = GraphServiceURI(**parts)
        http_uri = self.http_uri
        http_scheme = http_uri.scheme
        http_host = http_uri.host
        http_port = http_uri.port

        bolt = parts.get("bolt")
        if bolt is None:
            from socket import create_connection
            try:
                s = create_connection((http_host, parts.get("bolt_port", 7687)))
            except IOError:
                bolt = False
            else:
                s.close()
                bolt = True
        if bolt:
            if self.bolt_uri is None:
                self.bolt_uri = GraphServiceURI(scheme="bolt", **parts)
        else:
            self.bolt_uri = None

        self.data = {
            "secure": self.secure,
            "http_host": http_host,
            "http_port": http_port if http_scheme == "http" else None,
            "https_port": http_port if http_scheme == "https" else None,
            "bolt": False,
            "bolt_host": None,
            "bolt_port": None,
        }
        if self.bolt_uri:
            self.data.update({
                "bolt": True,
                "bolt_host": self.bolt_uri.host,
                "bolt_port": self.bolt_uri.port,
            })

    def __repr__(self):
        return "<%s http_uri=%r bolt_uri=%r>" % (self.__class__.__name__, self.http_uri, self.bolt_uri)

    def __getitem__(self, item):
        return self.data[item]

    def __hash__(self):
        return hash((self.http_uri, self.bolt_uri))

    def __eq__(self, other):
        return hash(self) == hash(other)

    def __ne__(self, other):
        return not self.__eq__(other)

    @property
    def secure(self):
        return self.http_uri.secure

    @property
    def uri(self):
        return self.bolt_uri or self.http_uri

    def keys(self):
        return self.data.keys()


class GraphServiceAuth(object):

    def __init__(self, *uris, **settings):
        self.__settings = {
            "user": "neo4j",
        }

        def apply_uri(u):
            parsed = urlsplit(u)
            if parsed.username:
                apply_auth(parsed.username + ":" + parsed.password)

        def apply_auth(a):
            user, _, password = a.partition(":")
            if user:
                self.__settings["user"] = user
            if password:
                self.__settings["password"] = password

        # Apply URIs
        for uri in uris:
            apply_uri(uri)

        # Apply individual settings
        self.__settings.update({k: v for k, v in settings.items()
                                if k in ["auth", "user", "password"]})

        if self.password is None:
            raise TypeError("No auth details available")

    def __repr__(self):
        return "<ServerAuth settings=%r>" % self.__settings

    @property
    def user(self):
        settings = self.__settings
        if "user" in settings:
            return settings["user"]
        elif "auth" in settings:
            return settings["auth"][0]
        else:
            return None

    @property
    def password(self):
        settings = self.__settings
        if "password" in settings:
            return settings["password"]
        elif "auth" in settings:
            return settings["auth"][1]
        else:
            return None

    @property
    def token(self):
        return self.user, self.password

    @property
    def bolt_auth_token(self):
        return basic_auth(self.user, self.password)

    @property
    def http_authorization(self):
        return 'Basic ' + b64encode((self.user + ":" +
                                     self.password).encode("UTF-8")).decode("ASCII")


def register_graph_service(*uris, **settings):
    """ Register service address details and return a
    :class:`.ServiceAddress` instance.

    :param uris:
    :param settings:
    :return:
    """
    new_address = GraphServiceAddress(*uris, **settings)
    try:
        new_auth = GraphServiceAuth(*uris, **settings)
    except TypeError:
        new_auth = None
    if new_auth is None:
        keyring.setdefault(new_address.http_uri)
        if new_address.bolt_uri:
            keyring.setdefault(new_address.bolt_uri)
    else:
        keyring[new_address.http_uri] = new_auth
        if new_address.bolt_uri:
            keyring[new_address.bolt_uri] = new_auth
    return new_address


def _register_graph_service_from_environment():
    neo4j_uri = getenv("NEO4J_URI")
    if neo4j_uri:
        settings = {}
        neo4j_auth = getenv("NEO4J_AUTH")
        if neo4j_auth:
            user, _, password = neo4j_auth.partition(":")
            settings["user"] = user
            settings["password"] = password
        register_graph_service(neo4j_uri, **settings)


def get_graph_service_auth(address):
    for addr, auth in keyring.items():
        if addr in (address.http_uri, address.bolt_uri):
            return auth
    raise KeyError(address)


def authenticate(host_port, user, password):
    """ Set HTTP basic authentication values for specified `host_port` for use
    with both Neo4j 2.2 built-in authentication as well as if a database server
    is behind (for example) an Apache proxy. The code below shows a simple example::

        from py2neo import authenticate, Graph

        # set up authentication parameters
        authenticate("camelot:7474", "arthur", "excalibur")

        # connect to authenticated graph database
        graph = Graph("http://camelot:7474/db/data/")

    Note: a `host_port` can be either a server name or a server name and port
    number but must match exactly that used within the Graph
    URI.

    :arg host_port: the host and optional port requiring authentication
        (e.g. "bigserver", "camelot:7474")
    :arg user: the user name to authenticate as
    :arg password: the password
    """
    register_graph_service("http://%s/" % host_port, user=user, password=password)


_register_graph_service_from_environment()

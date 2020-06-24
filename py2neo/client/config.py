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


from os import getenv
from sys import platform, version_info

from py2neo.compat import urlsplit, string_types
from py2neo.wiring import Address


NEO4J_URI = getenv("NEO4J_URI")
NEO4J_AUTH = getenv("NEO4J_AUTH")
NEO4J_USER_AGENT = getenv("NEO4J_USER_AGENT")
NEO4J_SECURE = True if getenv("NEO4J_SECURE") == "1" else False if getenv("NEO4J_SECURE") == "0" else None
NEO4J_VERIFY = True if getenv("NEO4J_VERIFY") == "1" else False if getenv("NEO4J_VERIFY") == "0" else None


DEFAULT_PROTOCOL = "bolt"
DEFAULT_SECURE = False
DEFAULT_VERIFY = True
DEFAULT_USER = "neo4j"
DEFAULT_PASSWORD = "password"
DEFAULT_HOST = "localhost"
DEFAULT_BOLT_PORT = 7687
DEFAULT_HTTP_PORT = 7474
DEFAULT_HTTPS_PORT = 7473


class ConnectionProfile(object):
    """ Connection details for a Neo4j service.

    Configuration keys:
    - uri
    - auth
    - secure
    - verify
    - scheme
    - user
    - password
    - address
    - host
    - port

    """

    __secure = None
    __verify = None
    __scheme = None
    __user = None
    __password = None
    __address = None
    __hash_keys = ("secure", "verify", "scheme", "user", "password", "address")

    def __init__(self, uri=None, **settings):
        # TODO: recognise IPv6 addresses explicitly
        uri = self._coalesce(uri, NEO4J_URI)
        self._apply_uri(uri)
        self._apply_auth(**settings)
        self._apply_components(**settings)
        self._apply_correct_scheme_for_security()
        self._apply_other_defaults()

    def __repr__(self):
        return "%s(%r)" % (self.__class__.__name__, self.uri)

    def _apply_uri(self, uri):
        if uri is not None:
            parsed = urlsplit(uri)
            if parsed.scheme is not None:
                self.__scheme = parsed.scheme
                if self.scheme in ["bolt+s", "bolt+ssc",
                                   "https", "http+s", "http+ssc"]:
                    self.__secure = True
                elif self.scheme in ["bolt", "http"]:
                    self.__secure = False
                if self.scheme in ["bolt+ssc", "http+ssc"]:
                    self.__verify = False
                else:
                    self.__verify = True
            self.__user = self._coalesce(parsed.username, self.user)
            self.__password = self._coalesce(parsed.password, self.password)
            netloc = parsed.netloc
            if "@" in netloc:
                self.__address = Address.parse(netloc.partition("@")[-1])
            else:
                self.__address = Address.parse(netloc)
        else:
            self.__address = Address.parse("")

    def _apply_auth(self, **settings):
        if "auth" in settings and settings["auth"] is not None:
            if isinstance(settings["auth"], string_types):
                self.__user, _, self.__password = settings["auth"].partition(":")
            else:
                self.__user, self.__password = settings["auth"]
        elif NEO4J_AUTH is not None:
            self.__user, _, self.__password = NEO4J_AUTH.partition(":")

    def _apply_components(self, **settings):
        self.__secure = self._coalesce(settings.get("secure"), self.secure, NEO4J_SECURE)
        self.__verify = self._coalesce(settings.get("verify"), self.verify, NEO4J_VERIFY)
        self.__scheme = self._coalesce(settings.get("scheme"), self.scheme)
        self.__user = self._coalesce(settings.get("user"), self.user)
        self.__password = self._coalesce(settings.get("password"), self.password)
        if "address" in settings:
            address = settings.get("address")
            if isinstance(address, tuple):
                self.__address = Address(address)
            else:
                self.__address = Address.parse(settings.get("address"))
        if "host" in settings and "port" in settings:
            self.__address = Address.parse("%s:%s" % (settings.get("host"), settings.get("port")))
        elif "host" in settings:
            self.__address = Address.parse("%s:%s" % (settings.get("host"), self.port))
        elif "port" in settings:
            self.__address = Address.parse("%s:%s" % (self.host, settings.get("port")))

    def _apply_correct_scheme_for_security(self):
        if self.secure is None:
            self.__secure = DEFAULT_SECURE
        if self.verify is None:
            self.__verify = DEFAULT_VERIFY
        if self.protocol == "bolt":
            if self.secure:
                self.__scheme = "bolt+s" if self.verify else "bolt+ssc"
            else:
                self.__scheme = "bolt"
        elif self.protocol == "http":
            if self.secure:
                self.__scheme = "https" if self.verify else "http+ssc"
            else:
                self.__scheme = "http"

    def _apply_other_defaults(self):
        if not self.user:
            self.__user = DEFAULT_USER
        if not self.password:
            self.__password = DEFAULT_PASSWORD
        if not self.address.port:
            bits = list(self.address)
            if self.scheme == "http":
                bits[1] = DEFAULT_HTTP_PORT
            elif self.scheme in ("https", "http+s", "http+ssc"):
                bits[1] = DEFAULT_HTTPS_PORT
            else:
                bits[1] = DEFAULT_BOLT_PORT
            self.__address = Address(bits)

    def __hash__(self):
        values = tuple(getattr(self, key) for key in self.__hash_keys)
        return hash(values)

    def __eq__(self, other):
        self_values = tuple(getattr(self, key) for key in self.__hash_keys)
        other_values = tuple(getattr(other, key) for key in self.__hash_keys)
        return self_values == other_values

    @staticmethod
    def _coalesce(*values):
        """ Utility function to return the first non-null value from a
        sequence of values.
        """
        for value in values:
            if value is not None:
                return value
        return None

    def to_dict(self):
        keys = ["secure", "verify", "scheme", "user", "password", "address",
                "auth", "host", "port", "port_number", "protocol", "uri"]
        d = {}
        for key in keys:
            d[key] = getattr(self, key)
        return d

    @property
    def secure(self):
        return self.__secure

    @property
    def verify(self):
        return self.__verify

    @property
    def scheme(self):
        return self.__scheme

    @property
    def user(self):
        return self.__user

    @property
    def password(self):
        return self.__password

    @property
    def address(self):
        return self.__address

    @property
    def auth(self):
        return self.user, self.password

    @property
    def host(self):
        return self.address.host

    @property
    def port(self):
        return self.address.port

    @property
    def port_number(self):
        return self.address.port_number

    @property
    def protocol(self):
        if self.scheme in ("bolt", "bolt+s", "bolt+ssc"):
            return "bolt"
        elif self.scheme in ("http", "https", "http+s", "http+ssc"):
            return "http"
        else:
            return DEFAULT_PROTOCOL

    @property
    def uri(self):
        return "%s://%s@%s:%s" % (self.scheme, self.user, self.host, self.port)


def bolt_user_agent():
    import py2neo
    fields = ((py2neo.__package__, py2neo.__version__) +
              tuple(version_info) + (platform,))
    return "{}/{} Python/{}.{}.{}-{}-{} ({})".format(*fields)


def http_user_agent():
    import py2neo
    import urllib3
    fields = ((py2neo.__package__, py2neo.__version__, urllib3.__version__) +
              tuple(version_info) + (platform,))
    return "{}/{} urllib3/{} Python/{}.{}.{}-{}-{} ({})".format(*fields)

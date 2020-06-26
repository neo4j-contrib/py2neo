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


from collections.abc import Mapping
from os import getenv
from sys import platform, version_info

from py2neo.compat import urlsplit, string_types
from py2neo.wiring import Address


NEO4J_URI = getenv("NEO4J_URI")
NEO4J_AUTH = getenv("NEO4J_AUTH")
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


def bolt_user_agent():
    """ Returns the default user agent sent over Bolt connections.
    """
    import py2neo
    fields = ((py2neo.__package__, py2neo.__version__) +
              tuple(version_info) + (platform,))
    return "{}/{} Python/{}.{}.{}-{}-{} ({})".format(*fields)


def http_user_agent():
    """ Returns the default user agent sent over HTTP connections.
    """
    import py2neo
    import urllib3
    fields = ((py2neo.__package__, py2neo.__version__, urllib3.__version__) +
              tuple(version_info) + (platform,))
    return "{}/{} urllib3/{} Python/{}.{}.{}-{}-{} ({})".format(*fields)


class ConnectionProfile(Mapping):
    """ Connection details for a Neo4j service.

    A connection profile holds a set of values that describe how to
    connect to, and authorise against, a particular Neo4j service.
    The set of values held within a profile are available as either
    object attributes (e.g. ``profile.uri``) or sub-items (e.g.
    ``profile["uri"]``.

    Profile instances are immutable, so can safely be hashed for
    inclusion within a set or as dictionary keys.

    :param profile:
        The base connection information, provided as a dictionary, an
        existing :class:`.ConnectionProfile` object or a string URI.
        This value can also be :const:`None`, in which case default
        base settings are used.

        * (None) -- no base profile; use default settings
        * (str) -- URI profile, e.g. `bolt://bob@graph.example.com:7687`
        * (dict) -- dictionary of individual settings
        * (:class:`.ConnectionProfile`) -- existing profile object

    :param settings:
        An optional set of individual overrides for each value.
        Valid options are:

        * :attr:`.address` -- either a tuple (e.g. ``('localhost', 7687)``), an :class:`.Address` object, or a string (e.g. ``'localhost:7687'``)
        * :attr:`.auth` -- either a 2-tuple (e.g. ``('user', 'password')``), or a string (e.g. ``'user:password'``)
        * :attr:`.host`
        * :attr:`.password`
        * :attr:`.port`
        * :attr:`.scheme`
        * :attr:`.secure`
        * :attr:`.user`
        * :attr:`.verify`

    The values used for a default profile will be based on a local
    server listening on the default Bolt port, with the password
    ``password``. These defaults can be altered via environment
    variables, if required:

    .. envvar :: NEO4J_URI

    .. envvar :: NEO4J_AUTH

    .. envvar :: NEO4J_SECURE

    .. envvar :: NEO4J_VERIFY

    The full set of attributes and operations are described below.

    .. describe:: profile == other

        Return :const:`True` if `profile` and `other` are equal.

    .. describe:: profile != other

        Return :const:`True` if `profile` and `other` are unequal.

    .. describe:: hash(profile)

        Return a hash of `profile` based on its contained values.

    .. describe:: profile[key]

        Return a profile value using a string key.
        Key names are identical to the corresponding attribute names.

    .. describe:: len(profile)

        Return the number of values encoded within this profile.

    .. describe:: dict(profile)

        Coerce the profile into a dictionary of key-value pairs.

    """

    __keys = ("secure", "verify", "scheme", "user", "password", "address",
              "auth", "host", "port", "port_number", "protocol", "uri")

    __hash_keys = ("secure", "verify", "scheme", "user", "password", "address")

    def __init__(self, profile=None, **settings):
        # TODO: recognise IPv6 addresses explicitly

        # Apply base defaults, URI, or profile
        if profile is None:
            if NEO4J_URI:
                self._apply_base_uri(NEO4J_URI)
            else:
                self._apply_base_defaults()
        elif isinstance(profile, string_types):
            self._apply_base_uri(profile)
        elif isinstance(profile, Mapping):
            self._apply_base_defaults()
            settings = dict(profile, **settings)
        else:
            raise TypeError("Profile %r is neither a ConnectionProfile "
                            "nor a string URI" % profile)

        # Apply extra settings as overrides
        self._apply_auth(**settings)
        self._apply_components(**settings)

        # Clean up and derive secondary attributes
        self._apply_correct_scheme_for_security()
        self._apply_fallback_defaults()

    def __repr__(self):
        return "%s(%r)" % (self.__class__.__name__, self.uri)

    def __getitem__(self, key):
        if key in self.__keys:
            return getattr(self, key)
        else:
            raise KeyError(key)

    def __len__(self):
        return len(self.__keys)

    def __iter__(self):
        return iter(self.__keys)

    def _apply_base_defaults(self):
        self.__secure = None
        self.__verify = None
        self.__scheme = None
        self.__user = None
        self.__password = None
        self.__address = Address.parse("")

    def _apply_base_uri(self, uri):
        assert uri
        parsed = urlsplit(uri)
        if parsed.scheme is not None:
            self.__scheme = parsed.scheme
            if self.__scheme in ["bolt+s", "bolt+ssc",
                                 "https", "http+s", "http+ssc"]:
                self.__secure = True
            elif self.__scheme in ["bolt", "http"]:
                self.__secure = False
            if self.__scheme in ["bolt+ssc", "http+ssc"]:
                self.__verify = False
            else:
                self.__verify = True
        self.__user = parsed.username or None
        self.__password = parsed.password or None
        netloc = parsed.netloc
        if "@" in netloc:
            self.__address = Address.parse(netloc.partition("@")[-1])
        else:
            self.__address = Address.parse(netloc)

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

    def _apply_fallback_defaults(self):
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

    @property
    def secure(self):
        """ A flag for whether or not to apply security to the
        connection. If unspecified, and uninfluenced by environment
        variables, this will default to :const:`True`.
        """
        return self.__secure

    @property
    def verify(self):
        """ A flag for verification of remote server certificates.
        If unspecified, and uninfluenced by environment variables, this
        will default to :const:`True`.
        """
        return self.__verify

    @property
    def scheme(self):
        """ The URI scheme for contacting the remote server.
        If unspecified, and uninfluenced by environment variables, this
        will default to ``'bolt'``.
        """
        return self.__scheme

    @property
    def user(self):
        """ The user as whom to authorise.
        If unspecified, and uninfluenced by environment variables, this
        will default to ``'neo4j'``.
        """
        return self.__user

    @property
    def password(self):
        """ The password which with to authorise.
        If unspecified, and uninfluenced by environment variables, this
        will default to ``'password'``.
        """
        return self.__password

    @property
    def address(self):
        """ The full socket :class:`.Address` of the remote server.
        If unspecified, and uninfluenced by environment variables, this
        will default to ``IPv4Address(('localhost', 7687))``.
        """
        return self.__address

    @property
    def auth(self):
        """ A 2-tuple of `(user, password)` representing the combined
        auth details. If unspecified, and uninfluenced by environment
        variables, this will default to ``('neo4j', 'password')``.
        """
        return self.user, self.password

    @property
    def host(self):
        """ The host name or IP address of the remote server.
        If unspecified, and uninfluenced by environment variables, this
        will default to ``'localhost'``.
        """
        return self.address.host

    @property
    def port(self):
        """ The port to which to connect on the remote server. This
        will be the correct port for the given :attr:`.protocol`.
        If unspecified, and uninfluenced by environment variables, this
        will default to ``7687`` (for Bolt traffic).
        """
        return self.address.port

    @property
    def port_number(self):
        """ A variant of :attr:`.port` guaranteed to be returned as a
        number. In some cases, the regular port value can be a string,
        this attempts to resolve or convert that value into a number.
        If unspecified, and uninfluenced by environment variables, this
        will default to ``7687`` (for Bolt traffic).
        """
        return self.address.port_number

    @property
    def protocol(self):
        """ The name of the underlying point-to-point protocol, derived
        from the URI scheme. This will either be ``'bolt'`` or
        ``'http'``, regardless of security and verification settings.
        If unspecified, and uninfluenced by environment variables, this
        will default to ``'bolt'``.
        """
        if self.scheme in ("bolt", "bolt+s", "bolt+ssc"):
            return "bolt"
        elif self.scheme in ("http", "https", "http+s", "http+ssc"):
            return "http"
        else:
            return DEFAULT_PROTOCOL

    @property
    def uri(self):
        """ A full URI for the profile. This generally includes all
        other information, excluding the password (for security
        reasons). If unspecified, and uninfluenced by environment
        variables, this will default to ``'bolt://neo4j@localhost:7687'``.
        """
        return "%s://%s@%s:%s" % (self.scheme, self.user, self.host, self.port)

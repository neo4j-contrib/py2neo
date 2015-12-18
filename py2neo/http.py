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


import base64

from py2neo import PRODUCT
from py2neo.packages.httpstream import http, ClientError, ServerError, \
    Resource as _Resource, ResourceTemplate as _ResourceTemplate
from py2neo.packages.httpstream.http import JSONResponse, user_agent
from py2neo.packages.httpstream.numbers import UNAUTHORIZED
from py2neo.packages.httpstream.packages.urimagic import URI
from py2neo.status import GraphError
from py2neo.status.security import AuthorizationFailed
from py2neo.util import raise_from


http.default_encoding = "UTF-8"

_headers = {
    None: [
        ("User-Agent", user_agent(PRODUCT)),
        ("X-Stream", "true"),
    ],
}

_http_rewrites = {}


def _add_header(key, value, host_port=None):
    """ Add an HTTP header to be sent with all requests if no `host_port`
    is provided or only to those matching the value supplied otherwise.
    """
    if host_port in _headers:
        _headers[host_port].append((key, value))
    else:
        _headers[host_port] = [(key, value)]


def _get_headers(host_port):
    """Fetch all HTTP headers relevant to the `host_port` provided.
    """
    uri_headers = {}
    for n, headers in _headers.items():
        if n is None or n == host_port:
            uri_headers.update(headers)
    return uri_headers


def authenticate(host_port, user_name, password):
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
    :arg user_name: the user name to authenticate as
    :arg password: the password
    """
    credentials = (user_name + ":" + password).encode("UTF-8")
    value = 'Basic ' + base64.b64encode(credentials).decode("ASCII")
    _add_header("Authorization", value, host_port=host_port)


def rewrite(from_scheme_host_port, to_scheme_host_port):
    """ Automatically rewrite all URIs directed to the scheme, host and port
    specified in `from_scheme_host_port` to that specified in
    `to_scheme_host_port`.

    As an example::

        from py2neo import rewrite
        # implicitly convert all URIs beginning with <http://localhost:7474>
        # to instead use <https://dbserver:9999>
        rewrite(("http", "localhost", 7474), ("https", "dbserver", 9999))

    If `to_scheme_host_port` is :py:const:`None` then any rewrite rule for
    `from_scheme_host_port` is removed.

    This facility is primarily intended for use by database servers behind
    proxies which are unaware of their externally visible network address.
    """
    global _http_rewrites
    if to_scheme_host_port is None:
        try:
            del _http_rewrites[from_scheme_host_port]
        except KeyError:
            pass
    else:
        _http_rewrites[from_scheme_host_port] = to_scheme_host_port


class Resource(_Resource):
    """ Base class for all local resources mapped to remote counterparts.
    """

    #: The class of error raised by failure responses from this resource.
    error_class = GraphError

    def __init__(self, uri, metadata=None, headers=None):
        uri = URI(uri)
        scheme_host_port = (uri.scheme, uri.host, uri.port)
        if scheme_host_port in _http_rewrites:
            scheme_host_port = _http_rewrites[scheme_host_port]
            # This is fine - it's all my code anyway...
            uri._URI__set_scheme(scheme_host_port[0])
            uri._URI__set_authority("{0}:{1}".format(scheme_host_port[1],
                                                     scheme_host_port[2]))
        if uri.user_info:
            authenticate(uri.host_port, *uri.user_info.partition(":")[0::2])
        self._resource = _Resource.__init__(self, uri)
        self._headers = dict(headers or {})
        self.__base = super(Resource, self)
        if metadata is None:
            self.__initial_metadata = None
        else:
            self.__initial_metadata = dict(metadata)
        self.__last_get_response = None

        uri = uri.string
        dbms_uri = uri[:uri.find("/", uri.find("//") + 2)] + "/"
        if dbms_uri == uri:
            self.__dbms = self
        else:
            from py2neo.core import DBMS
            self.__dbms = DBMS(dbms_uri)
        self.__ref = NotImplemented

    @property
    def graph(self):
        """ The parent graph of this resource.

        :rtype: :class:`.Graph`
        """
        return self.__dbms.graph

    @property
    def headers(self):
        """ The HTTP headers sent with this resource.
        """
        headers = _get_headers(self.__uri__.host_port)
        headers.update(self._headers)
        return headers

    @property
    def metadata(self):
        """ Metadata received in the last HTTP response.
        """
        if self.__last_get_response is None:
            if self.__initial_metadata is not None:
                return self.__initial_metadata
            self.get()
        return self.__last_get_response.content

    def resolve(self, reference, strict=True):
        """ Resolve a URI reference against the URI for this resource,
        returning a new resource represented by the new target URI.

        :arg reference: Relative URI to resolve.
        :arg strict: Strict mode flag.
        :rtype: :class:`.Resource`
        """
        return Resource(_Resource.resolve(self, reference, strict).uri)

    @property
    def dbms(self):
        """ The root service associated with this resource.

        :return: :class:`.DBMS`
        """
        return self.__dbms

    def get(self, headers=None, redirect_limit=5, **kwargs):
        """ Perform an HTTP GET to this resource.

        :arg headers: Extra headers to pass in the request.
        :arg redirect_limit: Maximum number of times to follow redirects.
        :arg kwargs: Other arguments to pass to the underlying `httpstream` method.
        :rtype: :class:`httpstream.Response`
        :raises: :class:`py2neo.GraphError`
        """
        headers = dict(self.headers, **(headers or {}))
        kwargs.update(cache=True)
        try:
            response = self.__base.get(headers=headers, redirect_limit=redirect_limit, **kwargs)
        except (ClientError, ServerError) as error:
            if error.status_code == UNAUTHORIZED:
                raise AuthorizationFailed(self.uri.string)
            if isinstance(error, JSONResponse):
                content = dict(error.content, request=error.request, response=error)
            else:
                content = {}
            message = content.pop("message", "HTTP GET returned response %s" % error.status_code)
            raise_from(self.error_class(message, **content), error)
        else:
            self.__last_get_response = response
            return response

    def put(self, body=None, headers=None, **kwargs):
        """ Perform an HTTP PUT to this resource.

        :arg body: The payload of this request.
        :arg headers: Extra headers to pass in the request.
        :arg kwargs: Other arguments to pass to the underlying `httpstream` method.
        :rtype: :class:`httpstream.Response`
        :raises: :class:`py2neo.GraphError`
        """
        headers = dict(self.headers, **(headers or {}))
        try:
            response = self.__base.put(body, headers, **kwargs)
        except (ClientError, ServerError) as error:
            if error.status_code == UNAUTHORIZED:
                raise AuthorizationFailed(self.uri.string)
            if isinstance(error, JSONResponse):
                content = dict(error.content, request=error.request, response=error)
            else:
                content = {}
            message = content.pop("message", "HTTP PUT returned response %s" % error.status_code)
            raise_from(self.error_class(message, **content), error)
        else:
            return response

    def post(self, body=None, headers=None, **kwargs):
        """ Perform an HTTP POST to this resource.

        :arg body: The payload of this request.
        :arg headers: Extra headers to pass in the request.
        :arg kwargs: Other arguments to pass to the underlying `httpstream` method.
        :rtype: :class:`httpstream.Response`
        :raises: :class:`py2neo.GraphError`
        """
        headers = dict(self.headers, **(headers or {}))
        try:
            response = self.__base.post(body, headers, **kwargs)
        except (ClientError, ServerError) as error:
            if error.status_code == UNAUTHORIZED:
                raise AuthorizationFailed(self.uri.string)
            if isinstance(error, JSONResponse):
                content = dict(error.content, request=error.request, response=error)
            else:
                content = {}
            message = content.pop("message", "HTTP POST returned response %s" % error.status_code)
            raise_from(self.error_class(message, **content), error)
        else:
            return response

    def delete(self, headers=None, **kwargs):
        """ Perform an HTTP DELETE to this resource.

        :arg headers: Extra headers to pass in the request.
        :arg kwargs: Other arguments to pass to the underlying `httpstream` method.
        :rtype: :class:`httpstream.Response`
        :raises: :class:`py2neo.GraphError`
        """
        headers = dict(self.headers, **(headers or {}))
        try:
            response = self.__base.delete(headers, **kwargs)
        except (ClientError, ServerError) as error:
            if error.status_code == UNAUTHORIZED:
                raise AuthorizationFailed(self.uri.string)
            if isinstance(error, JSONResponse):
                content = dict(error.content, request=error.request, response=error)
            else:
                content = {}
            message = content.pop("message", "HTTP DELETE returned response %s" % error.status_code)
            raise_from(self.error_class(message, **content), error)
        else:
            return response


class ResourceTemplate(_ResourceTemplate):
    """ A factory class for producing :class:`.Resource` objects dynamically
    based on a template URI.
    """

    #: The class of error raised by failure responses from resources produced by this template.
    error_class = GraphError

    def expand(self, **values):
        """ Produce a resource instance by substituting values into the
        stored template URI.

        :arg values: A set of named values to plug into the template URI.
        :rtype: :class:`.Resource`
        """
        resource = Resource(self.uri_template.expand(**values))
        resource.error_class = self.error_class
        return resource

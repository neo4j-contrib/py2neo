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

from __future__ import absolute_import

from json import dumps as json_dumps, loads as json_loads

from urllib3 import HTTPConnectionPool, HTTPSConnectionPool

from py2neo.meta import HTTP_USER_AGENT
from py2neo.compat import urlsplit
from py2neo.addressing import keyring
from py2neo.status import GraphError, Unauthorized


OK = 200
CREATED = 201
NO_CONTENT = 204
UNAUTHORIZED = 401
NOT_FOUND = 404


ConnectionPool = {
    "http": HTTPConnectionPool,
    "https": HTTPSConnectionPool,
}

_http_headers = {
    (None, None, None): [
        ("User-Agent", HTTP_USER_AGENT),
        ("X-Stream", "true"),
    ],
}


def set_http_header(key, value, scheme=None, host=None, port=None):
    """ Add an HTTP header for all future requests. If a `host_port` is
    specified, this header will only be included in requests to that
    destination.

    :arg key: name of the HTTP header
    :arg value: value of the HTTP header
    :arg scheme:
    :arg host:
    :arg port:
    """
    address_key = (scheme, host, port)
    if address_key in _http_headers:
        _http_headers[address_key].append((key, value))
    else:
        _http_headers[address_key] = [(key, value)]


def get_http_headers(scheme, host, port):
    """Fetch all HTTP headers relevant to the `host_port` provided.

    :arg scheme:
    :arg host:
    :arg port:
    """
    uri_headers = {}
    for (s, h, p), headers in _http_headers.items():
        if (s is None or s == scheme) and (h is None or h == host) and (p is None or p == port):
            uri_headers.update(headers)
    for uri, auth in keyring.items():
        if auth and uri.scheme in ("http", "https") and uri.host == host and uri.port == port:
            uri_headers["Authorization"] = auth.http_authorization
    return uri_headers


class WebResource(object):
    """ Base class for all local resources mapped to remote counterparts.
    """

    def __init__(self, uri, cached_json=None):
        self.uri = uri
        self.cached_json = cached_json
        parts = urlsplit(uri)
        scheme = parts.scheme
        host = parts.hostname
        port = parts.port
        self.path = parts.path
        self.http = http = ConnectionPool[scheme]("%s:%d" % (host, port))
        self.request = http.request
        self.headers = get_http_headers(scheme, host, port)

    def __del__(self):
        self.http.close()

    def __eq__(self, other):
        try:
            return self.uri == other.uri
        except AttributeError:
            return False

    def __ne__(self, other):
        return not self.__eq__(other)

    def get_json(self, force=True):
        """ Perform an HTTP GET to this resource and return JSON.
        """
        if not force and self.cached_json is not None:
            return self.cached_json
        rs = self.request("GET", self.path, headers=self.headers)
        try:
            if rs.status == 200:
                self.cached_json = json_loads(rs.data.decode('utf-8'))
                return self.cached_json
            else:
                raise_error(self.uri, rs.status, rs.data)
        finally:
            rs.close()

    def post(self, body, expected):
        """ Perform an HTTP POST to this resource.
        """
        headers = dict(self.headers)
        if body is not None:
            headers["Content-Type"] = "application/json"
            body = json_dumps(body).encode('utf-8')
        rs = self.request("POST", self.path, headers=self.headers, body=body)
        if rs.status not in expected:
            raise_error(self.uri, rs.status, rs.data)
        return rs

    def delete(self, expected):
        """ Perform an HTTP DELETE to this resource.
        """
        rs = self.request("DELETE", self.path, headers=self.headers)
        if rs.status not in expected:
            raise_error(self.uri, rs.status, rs.data)
        return rs


def raise_error(uri, status_code, data):
    if status_code == UNAUTHORIZED:
        raise Unauthorized(uri)
    if data:
        content = json_loads(data.decode('utf-8'))
    else:
        content = {}
    message = content.pop("message", "HTTP request returned unexpected status code %s" % status_code)
    error = GraphError(message, **content)
    error.http_status_code = status_code
    raise error

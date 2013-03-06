#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright 2011-2013, Nigel Small
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

"""REST client based on httplib for use with Neo4j REST interface.
"""

try:
    import http.client as httplib
except ImportError:
    import httplib
import json
import logging
import socket
import sys
import threading

try:
    from urllib.parse import urlsplit, urlparse
except ImportError:
    from urlparse import urlsplit, urlparse

from . import __package__ as py2neo_package
from . import __version__ as py2neo_version

from .util import PropertyCache

logger = logging.getLogger(__name__)

# HTTP status codes to count as automatic redirects
_auto_redirects = [301, 302, 303, 307, 308]


class HTTPHeaders(object):

    def __init__(self):
        self._headers = {}

    def add(self, key, value, netloc=None):
        """Add an HTTP header to be sent with all requests if no `netloc`
        is provided or only to those matching the value supplied otherwise.
        """
        if netloc in self._headers:
            self._headers[netloc].append((key, value))
        else:
            self._headers[netloc] = [(key, value)]

    def get(self, netloc):
        """Fetch all HTTP headers relevant to the `netloc` provided.
        """
        uri_headers = {}
        for n, headers in self._headers.items():
            if n is None or n == netloc:
                uri_headers.update(headers)
        return uri_headers

http_headers = HTTPHeaders()
http_headers.add("Accept", "application/json")
http_headers.add("Content-Type", "application/json")
http_headers.add("User-Agent", "{0}/{1} ({2}; python/{3})".format(
    py2neo_package, py2neo_version,
    sys.platform, sys.version.partition(" ")[0],
))
http_headers.add("X-Stream", "true;format=pretty")

http_rewrites = {}

http_timeouts = {}


_thread_local = threading.local()


class BadRequest(ValueError):
    """ Exception triggered by a 400 HTTP response status.
    """

    def __init__(self, data, id_=None):
        """
        :param data: information describing the fault identified
        :param id_:  unique request ID
        """
        if id_:
            logger.debug("Bad request for id {0}:\n{1}".format(id_, data))
        else:
            logger.debug("Bad request:\n{0}".format(data))
        ValueError.__init__(self)
        self.id = id_
        try:
            self.exception = data["exception"]
        except KeyError:
            self.exception = None
        try:
            self.message = data["message"]
        except KeyError:
            self.message = None
        try:
            self.stacktrace = data["stacktrace"]
        except KeyError:
            self.stacktrace = None
        self._data = data

    def __str__(self):
        if self.exception and self.message:
            return "{0}: {1}".format(self.exception, self.message)
        elif self.exception:
            return repr(self.exception)
        else:
            return repr(self._data)


class Unauthorized(Exception):
    """ Exception triggered by a 401 HTTP response status.
    """

    def __init__(self, uri):
        """
        :param uri:  URI of the resource
        """
        logger.debug("Resource <{0}> requires user authentication.".format(uri))
        Exception.__init__(self)
        self.uri = uri

    def __str__(self):
        return repr(self.uri)


class ResourceNotFound(LookupError):
    """ Exception triggered by a 404 HTTP response status.
    """

    def __init__(self, uri, id_=None):
        """
        :param uri:  URI of the resource
        :param id_:  unique request ID
        """
        if id_:
            logger.debug("Resource not found for id {0} <{1}>".format(id_, uri))
        else:
            logger.debug("Resource not found <{0}>".format(uri))
        LookupError.__init__(self)
        self.id = id_
        self.uri = uri

    def __str__(self):
        return repr(self.uri)


class ResourceConflict(EnvironmentError):
    """ Exception triggered by a 409 HTTP response status.
    """

    def __init__(self, uri, id_=None):
        """
        :param uri:  URI of the resource
        :param id_:  unique request ID
        """
        if id_:
            logger.debug("Resource conflict for id {0} <{1}>".format(id_, uri))
        else:
            logger.debug("Resource conflict <{0}>".format(uri))
        EnvironmentError.__init__(self)
        self.id = id_
        self.uri = uri

    def __str__(self):
        return repr(self.uri)


class SocketError(IOError):
    """ Exception triggered by low-level socket error.
    """

    def __init__(self, uri):
        logger.debug("Socket error <{0}>".format(uri))
        IOError.__init__(self)
        self.uri = uri

    def __str__(self):
        return repr(self.uri)


class URI(object):

    def __init__(self, uri):
        try:
            self.__uri__ = str(uri.__uri__)
        except AttributeError:
            self.__uri__ = str(uri)
        parsed = urlparse(self.__uri__)
        self.scheme = parsed.scheme
        self.netloc = parsed.netloc
        self.path = parsed.path
        self.params = parsed.params
        self.query = parsed.query
        self.fragment = parsed.fragment
        self.username = parsed.username
        self.password = parsed.password
        self.hostname = parsed.hostname
        self.port = parsed.port
        metadata = ServiceRoot.get(self.scheme, self.hostname, self.port)
        for key, value in metadata.items():
            if self.__uri__.startswith(value):
                self.base, self.reference = self.__uri__.partition(value)[1:3]
                break
        else:
            self.base, self.reference = self.__uri__, ""

    def __repr__(self):
        return self.__uri__

    def __eq__(self, other):
        return URI(self).__uri__ == URI(other).__uri__

    def __ne__(self, other):
        return URI(self).__uri__ != URI(other).__uri__


class Request(object):

    def __init__(self, graph_db, method, uri, body=None, headers=None):
        self.graph_db = graph_db
        self.method = method
        self.uri = uri
        self.body = body
        self.headers = headers

    def __repr__(self):
        return repr({
            "method": self.method,
            "to": self.uri,
            "body": self.body,
        })

    def description(self, id):
        return {
            "id": id,
            "method": self.method,
            "to": self.uri,
            "body": self.body,
        }


class Response(object):

    def __init__(self, graph_db, status, uri, location=None, body=None, id=None):
        self.graph_db = graph_db
        self.status = int(status)
        if self.status // 100 == 2:
            self.uri = str(uri)
            self.location = location
            self.body = body
            self.id = id
        elif self.status == 400:
            raise BadRequest(body, id_=id)
        elif self.status == 401:
            raise Unauthorized(uri)
        elif self.status == 404:
            raise ResourceNotFound(uri, id_=id)
        elif self.status == 409:
            raise ResourceConflict(uri, id_=id)
        elif self.status // 100 == 5:
            raise SystemError(body)


class Client(object):
    """ HTTP/HTTPS connection manager intended to be instantiated once per
        thread. Maintains a collection of HTTP and HTTPS connection objects,
        each for use with a unique combination of host and port (e.g.
        "localhost:7474").
    """

    def __init__(self):
        self.http = {}
        self.https = {}

    def _connection(self, scheme, netloc, reconnect=False):
        if scheme == "http":
            return self._http_connection(netloc, reconnect)
        elif scheme == "https":
            return self._https_connection(netloc, reconnect)
        else:
            raise ValueError("Unsupported URI scheme: " + scheme)

    def _http_connection(self, netloc, reconnect=False):
        if netloc not in self.http or reconnect:
            self.http[netloc] = httplib.HTTPConnection(netloc, timeout=http_timeouts.get(netloc))
        return self.http[netloc]

    def _https_connection(self, netloc, reconnect=False):
        if netloc not in self.https or reconnect:
            self.https[netloc] = httplib.HTTPSConnection(netloc, timeout=http_timeouts.get(netloc))
        return self.https[netloc]

    def _send_request(self, method, uri, data=None, headers=None):
        uri_values = urlsplit(str(uri))
        if uri_values[3]:
            path = uri_values[2] + "?" + uri_values[3]
        else:
            path = uri_values[2]
        scheme, netloc = uri_values[0:2]
        if (scheme, netloc) in http_rewrites:
            alt_scheme, alt_netloc = http_rewrites[(scheme, netloc)]
            logger.debug("Rewriting <{0}://{1}> to <{2}://{3}>".format(scheme, netloc, alt_scheme, alt_netloc))
            scheme, netloc = alt_scheme, alt_netloc
        _headers = http_headers.get(netloc)
        _headers.update(headers or {})
        _headers["Host"] = netloc
        if data is not None:
            logger.debug("Encoding request body as JSON")
            data = json.dumps(data, separators=(",", ":"))
        reconnect = False
        for tries in range(1, 4):
            logger.debug("Establishing " + scheme + " connection to " + netloc)
            http = self._connection(scheme, netloc, reconnect)
            logger.debug("Sending request")
            if data:
                logger.info("{0} {1} {2} ({3} bytes)".format(method, path, _headers, len(data)))
                logger.debug("Request body: " + data)
            else:
                logger.info("{0} {1} {2} (no data)".format(method, path, _headers))
            try:
                http.request(method, path, data, _headers)
                logger.debug("Awaiting response")
                rs = http.getresponse()
                logger.info("{0} {1} {2}".format(rs.status, rs.reason, dict(rs.getheaders())))
                return rs
            except httplib.HTTPException as err:
                if tries < 3:
                    logger.warn("Request failed ({0}), retrying".format(err.__class__.__name__))
                    reconnect = True
                else:
                    raise err

    def send(self, request, *args, **kwargs):
        rs = self._send_request(request.method, request.uri, request.body, request.headers)
        if rs.status in _auto_redirects:
            # automatic redirection - discard data and call recursively
            rs.read()
            request.uri = rs.getheader("Location")
            return self.send(request, *args, **kwargs)
        else:
            # direct response
            rs_body = rs.read().decode("utf-8")
            try:
                rs_body = json.loads(rs_body)
            except ValueError:
                rs_body = None
            return Response(request.graph_db, rs.status, request.uri, rs.getheader("Location", None), rs_body)


class Resource(object):
    """ Web service resource class, designed to work with a well-behaved REST
    web service.

    :param uri: the URI identifying this resource
    """

    def __init__(self, uri):
        self.__uri__ = uri
        self.__metadata = PropertyCache()

    def __repr__(self):
        """ Return a valid Python representation of this object.
        """
        return "{0}('{1}')".format(self.__class__.__name__, repr(self.__uri__))

    def __eq__(self, other):
        """ Determine equality of two objects based on URI.
        """
        return self.__uri__ == other.__uri__

    def __ne__(self, other):
        """ Determine inequality of two objects based on URI.
        """
        return self.__uri__ != other.__uri__

    @property
    def __uri__(self):
        """ The URI for this resource.
        """
        return self.__uri

    @__uri__.setter
    def __uri__(self, uri):
        if uri is None:
            self.__uri = None
        else:
            self.__uri = URI(uri)

    def _client(self):
        """ Fetch the HTTP client for use by this resource. Uses the client
        belonging to the local thread.
        """
        global _thread_local
        if not hasattr(_thread_local, "client"):
            _thread_local.client = Client()
        return _thread_local.client

    def _send(self, request):
        """ Issue an HTTP request.

        :param request: a rest.Request object
        :return: object created from returned content (200), C{Location} header value (201) or C{None} (204)
        :raise BadRequest: when supplied data is not appropriate (400)
        :raise ResourceNotFound: when URI is not found (404)
        :raise ResourceConflict: when a conflict occurs (409)
        :raise SystemError: when a server error occurs (500)
        :raise SocketError: when a connection fails or cannot be established
        """
        try:
            return self._client().send(request)
        except socket.error as err:
            raise SocketError(err)

    def _metadata(self, key, default=None):
        """ Look up a value in the resource metadata by key; will lazily load
            metadata if required.
        
        :param key: the key to look up
        """
        if self.__metadata.needs_update:
            self.refresh()
        if key in self.__metadata:
            return self.__metadata[key]
        else:
            return default

    def _update_metadata(self, metadata):
        self.__metadata = PropertyCache(metadata)

    @property
    def __metadata__(self):
        """ Dictionary of resource metadata, cached from the last request made
        to the remote server. To force an update of this metadata, use the
        :py:func:`refresh` method.
        """
        if self.__metadata.needs_update:
            self.refresh()
        return self.__metadata._properties

    def refresh(self):
        """ Refresh resource metadata by submitting a GET request to the main
        resource URI.
        """
        rs = self._send(Request(None, "GET", self.__uri__))
        self.__metadata.update(rs.body)


class ServiceRoot(object):

    _cache = {}

    @classmethod
    def _client(cls):
        global _thread_local
        if not hasattr(_thread_local, "client"):
            _thread_local.client = Client()
        return _thread_local.client

    @classmethod
    def get(cls, scheme, hostname, port):
        uri = "{0}://{1}:{2}/".format(scheme, hostname, port)
        if uri not in cls._cache:
            try:
                response = cls._client().send(Request(None, "GET", uri))
                cls._cache[uri] = response.body
            except socket.error as err:
                raise SocketError(err)
        return cls._cache[uri]

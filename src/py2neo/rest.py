#!/usr/bin/env python

# Copyright 2011 Nigel Small
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
    import simplejson as json
except ImportError:
    import json
import httplib
import logging
import socket
import threading
import time
from urlparse import urlsplit


__author__    = "Nigel Small <py2neo@nigelsmall.org>"
__copyright__ = "Copyright 2011 Nigel Small"
__license__   = "Apache License, Version 2.0"


AUTO_REDIRECTS = [301, 302, 303, 307, 308]

logger = logging.getLogger(__name__)

_thread_local = threading.local()


def local_client():
    if not hasattr(_thread_local, "client"):
        _thread_local.client = Client()
    return _thread_local.client


class BadRequest(ValueError):

    def __init__(self, data):
        ValueError.__init__(self)
        self.data = data

    def __str__(self):
        return repr(self.data)


class ResourceNotFound(LookupError):

    def __init__(self, uri):
        LookupError.__init__(self)
        self.uri = uri

    def __str__(self):
        return repr(self.uri)


class ResourceConflict(EnvironmentError):

    def __init__(self, uri):
        EnvironmentError.__init__(self)
        self.uri = uri

    def __str__(self):
        return repr(self.uri)


class SocketError(IOError):

    def __init__(self, uri):
        IOError.__init__(self)
        self.uri = uri

    def __str__(self):
        return repr(self.uri)


class PropertyCache(object):

    def __init__(self, properties=None, max_age=None):
        self._properties = {}
        self.max_age = max_age
        self._last_updated_time = None
        if properties:
            self.update(properties)

    def __nonzero__(self):
        return bool(self._properties)

    def __len__(self):
        return len(self._properties)

    def __getitem__(self, item):
        return self._properties[item]

    def __setitem__(self, item, value):
        self._properties[item] = value

    def __delitem__(self, item):
        del self._properties[item]

    def __iter__(self):
        return self._properties.__iter__()

    def __contains__(self, item):
        return item in self._properties

    @property
    def expired(self):
        if self._last_updated_time and self.max_age:
            return time.time() - self._last_updated_time > self.max_age
        else:
            return None

    @property
    def needs_update(self):
        return not self._properties or self.expired

    def clear(self):
        self.update(None)

    def update(self, properties):
        self._properties.clear()
        if properties:
            self._properties.update(properties)
        self._last_updated_time = time.time()

    def get(self, key, default=None):
        return self._properties.get(key, default)

    def get_all(self):
        return self._properties


class URI(object):

    def __init__(self, uri, marker):
        bits = str(uri).rpartition(marker)
        self.base = bits[0]
        self.reference = "".join(bits[1:])

    def __repr__(self):
        return self.base + self.reference

    def __eq__(self, other):
        return str(self) == str(other)

    def __ne__(self, other):
        return str(self) != str(other)


class Client(object):

    def __init__(self):
        self.http = {}
        self.https = {}
        self.headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "X-Stream": "true",
        }

    def _connection(self, scheme, netloc, reconnect=False):
        if scheme == "http":
            return self._http_connection(netloc, reconnect)
        elif scheme == "https":
            return self._https_connection(netloc, reconnect)
        else:
            raise ValueError("Unsupported URI scheme: " + scheme)

    def _http_connection(self, netloc, reconnect=False):
        if netloc not in self.http or reconnect:
            self.http[netloc] = httplib.HTTPConnection(netloc)
        return self.http[netloc]

    def _https_connection(self, netloc, reconnect=False):
        if netloc not in self.https or reconnect:
            self.https[netloc] = httplib.HTTPSConnection(netloc)
        return self.https[netloc]

    def _send_request(self, method, uri, data=None):
        reconnect = False
        uri_values = urlsplit(uri)
        scheme, netloc = uri_values[0:2]
        for tries in range(1, 4):
            http = self._connection(scheme, netloc, reconnect)
            if uri_values[3]:
                path = uri_values[2] + "?" + uri_values[3]
            else:
                path = uri_values[2]
            if data is not None:
                data = json.dumps(data)
            logger.info("{0} {1}".format(method, path))
            try:
                http.request(method, path, data, self.headers)
                return http.getresponse()
            except httplib.HTTPException as err:
                if tries < 3:
                    reconnect = True
                else:
                    raise err

    def request(self, method, uri, data=None, **kwargs):
        rs = self._send_request(method, uri, data)
        if rs.status in AUTO_REDIRECTS:
            # automatic redirection - discard data and call recursively
            rs.read()
            return self.request(
                method, rs.getheader("Location"), data=data, **kwargs
            )
        else:
            # direct response
            rs_data = rs.read()
            try:
                rs_data = json.loads(rs_data)
            except ValueError:
                rs_data = None
            return rs.status, uri, rs.getheaders(), rs_data

    def get(self, uri, **kwargs):
        return self.request("GET", uri, data=None, **kwargs)

    def put(self, uri, data, **kwargs):
        return self.request("PUT", uri, data=data, **kwargs)

    def post(self, uri, data, **kwargs):
        return self.request("POST", uri, data=data, **kwargs)

    def delete(self, uri, **kwargs):
        return self.request("DELETE", uri, data=None, **kwargs)


class Resource(object):
    """Web service resource class, designed to work with a well-behaved REST
    web service.

    :param uri:              the URI identifying this resource
    :param reference_marker:
    :param metadata:         previously obtained resource metadata
    """

    def __init__(self, uri, reference_marker, metadata=None):
        self._uri = URI(uri, reference_marker)
        self._last_location = None
        self._last_headers = None
        self._metadata = PropertyCache(metadata)

    def __repr__(self):
        """Return a valid Python representation of this object.
        """
        return "{0}('{1}')".format(self.__class__.__name__, repr(self._uri))

    def __eq__(self, other):
        """Determine equality of two objects based on URI.
        """
        return self._uri == other._uri

    def __ne__(self, other):
        """Determine inequality of two objects based on URI.
        """
        return self._uri != other._uri

    def _request(self, method, uri, data=None):
        """Issue an HTTP request.

        :param method: the HTTP method to use for this call
        :param uri: the URI of the resource to access
        :param data: optional data to be passed as request payload
        :return: object created from returned content (200), C{Location} header value (201) or C{None} (204)
        :raise BadRequest: when supplied data is not appropriate (400)
        :raise ResourceNotFound: when URI is not found (404)
        :raise ResourceConflict: when a conflict occurs (409)
        :raise SystemError: when a server error occurs (500)
        :raise SocketError: when a connection fails or cannot be established
        """
        try:
            status, self._last_location, self._last_headers, data = \
            local_client().request(method, str(uri), data)
            if status == 200:
                return data
            elif status == 201:
                return data
            elif status == 204:
                return None
            elif status == 400:
                raise BadRequest(data)
            elif status == 404:
                raise ResourceNotFound(uri)
            elif status == 409:
                raise ResourceConflict(uri)
            elif status // 100 == 5:
                raise SystemError(data)
        except socket.error as err:
            raise SocketError(err)

    def _get(self, uri, **kwargs):
        """Issue HTTP GET request.
        """
        return self._request('GET', uri, **kwargs)

    def _post(self, uri, data, **kwargs):
        """Issue HTTP POST request.
        """
        return self._request('POST', uri, data, **kwargs)

    def _put(self, uri, data, **kwargs):
        """Issue HTTP PUT request.
        """
        return self._request('PUT', uri, data, **kwargs)

    def _delete(self, uri, **kwargs):
        """Issue HTTP DELETE request.
        """
        return self._request('DELETE', uri, **kwargs)

    def _lookup(self, key):
        """Look up a value in the resource metadata by key; will lazily load
        metadata if required and auto-correct URI from Content-Location header.
        
        :param key: the key to look up
        """
        if self._metadata.needs_update:
            self._metadata.update(self._get(self._uri))
        if key in self._metadata:
            return self._metadata[key]
        else:
            raise KeyError(key)


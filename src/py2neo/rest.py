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

"""Tornado-based REST client for use with Neo4j REST interface.
"""


__author__    = "Nigel Small <py2neo@nigelsmall.org>"
__copyright__ = "Copyright 2011 Nigel Small"
__license__   = "Apache License, Version 2.0"


from tornado import httpclient

try:
    import json
except ImportError:
    import simplejson as json

import logging
logger = logging.getLogger(__name__)

import threading
import time


_thread_local = threading.local()

def local_http_client():
    if not hasattr(_thread_local, "http_client"):
        _thread_local.http_client = httpclient.HTTPClient()
    return _thread_local.http_client


_REQUEST_PARAMS = {
    "request_timeout": 300,    #: default 5 minutes timeout
    "user_agent": "py2neo"
}
_REQUEST_HEADERS = {
    "Accept": "application/json",
    "Content-Type": "application/json"
}


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


class NoResponse(IOError):

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


class Resource(object):
    """Web service resource class, designed to work with a well-behaved REST
    web service.

    :param uri:           the URI identifying this resource
    :param metadata:      previously obtained resource metadata
    """

    def __init__(self, uri, reference_marker, metadata=None, **request_params):
        self._uri = URI(uri, reference_marker)
        self._request_params = _REQUEST_PARAMS.copy()
        self._request_params.update(request_params)
        self._last_response = None
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

    def _request(self, method, uri, data=None, **request_params):
        """Issue an HTTP request.
        
        :param method: the HTTP method to use for this call
        :param uri: the URI of the resource to access
        :param data: optional data to be passed as request payload
        :param request_params: extra parameters to be passed to HTTP engine
        :return: object created from returned content (200), C{Location} header value (201) or C{None} (204)
        :raise ValueError: when supplied data is not appropriate (400)
        :raise ResourceNotFound: when URI is not found (404)
        :raise ResourceConflict: when a conflict occurs (409)
        :raise SystemError: when a server error occurs (500)
        :raise NoResponse: when a connection fails or cannot be established
        """
        params = self._request_params.copy()
        params.update(request_params)
        params.update({
            "method": method,
            "headers": _REQUEST_HEADERS,
            "body": data
        })
        try:
            logger.info("{0} {1}".format(method, uri))
            response = local_http_client().fetch(str(uri), **params)
            self._last_response = response
            if response.code == 200:
                if response.body:
                    return json.loads(response.body)
                else:
                    return None
            elif response.code == 201:
                #return response.headers['location']
                if response.body:
                    return json.loads(response.body)
                else:
                    return None
            elif response.code == 204:
                return None
        except httpclient.HTTPError as err:
            self._last_response = err.response
            if err.code == 400:
                try:
                    args = json.loads(err.response.body)
                except ValueError:
                    args = err.response.body
                raise ValueError(args)
            elif err.code == 404:
                raise ResourceNotFound(uri)
            elif err.code == 409:
                raise ResourceConflict(uri)
            elif err.code == 500:
                try:
                    args = json.loads(err.response.body)
                except ValueError:
                    args = err.response.body
                raise SystemError(args)
            elif err.code == 599:
                raise NoResponse(uri)
            else:
                raise err

    def _get(self, uri, **kwargs):
        """Issue HTTP GET request.
        """
        return self._request('GET', uri, **kwargs)

    def _post(self, uri, data, **kwargs):
        """Issue HTTP POST request.
        """
        return self._request('POST', uri, json.dumps(data), **kwargs)

    def _put(self, uri, data, **kwargs):
        """Issue HTTP PUT request.
        """
        return self._request('PUT', uri, json.dumps(data), **kwargs)

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


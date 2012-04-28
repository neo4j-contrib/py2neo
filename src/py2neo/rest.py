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

"""
Generic REST client
"""

__author__    = "Nigel Small <py2neo@nigelsmall.org>"
__copyright__ = "Copyright 2011 Nigel Small"
__license__   = "Apache License, Version 2.0"


from tornado import httpclient, curl_httpclient

try:
    import json
except ImportError:
    import simplejson as json


class Resource(object):
    """
    RESTful web service resource class, designed to work with a well-behaved
    hypermedia web service.

    :param uri:           the URI identifying this resource
    :param content_type:  the content type required for data exchange
    :param index:         a previously obtained resource index for endpoint discovery
    :param http:          HTTP object to use for requests
    """

    SUPPORTED_CONTENT_TYPES = ['application/json']

    def __init__(self, uri, content_type='application/json', index=None, http=None, **request_params):
        if content_type not in self.SUPPORTED_CONTENT_TYPES:
            raise NotImplementedError("Content type {0} not supported".format(content_type))
        self._uri = uri
        self._base_uri = None
        self._relative_uri = None
        self._content_type = content_type
        self._http = http or httpclient.HTTPClient(curl_httpclient.CurlAsyncHTTPClient)
        self._request_params = {"user_agent": "py2neo"}
        self._request_params.update(request_params)
        self._index = index
        self.__request_count = 0

    def __repr__(self):
        """
        Return a valid Python representation of this object.
        """
        return '%s(%s)' % (self.__class__.__name__, repr(self._uri))

    def __eq__(self, other):
        """
        Determine equality of two objects based on URI.
        """
        return self._uri == other._uri

    def __ne__(self, other):
        """
        Determine inequality of two objects based on URI.
        """
        return self._uri != other._uri

    def _spawn(self, class_, *args, **kwargs):
        """
        Spawn a new resource, reusing HTTP connection.
        """
        k = {"http": self._http}
        k.update(self._request_params)
        k.update(kwargs)
        return class_(*args, **k)
    
    def __get_request_headers(self, *keys):
        return dict([
            (key, self._content_type)
            for key in keys
            if key in ['Accept', 'Content-Type']
        ])

    def _request(self, method, uri, data=None, **request_params):
        """
        Issue an HTTP request.
        
        :param method: the HTTP method to use for this call
        :param uri: the URI of the resource to access
        :param data: raw data to be passed in request payload (optional)
        :return: object created from returned content (200), C{Location} header value (201) or C{None} (204)
        :raise ValueError: when supplied data is not appropriate (400)
        :raise KeyError: when URI is not found (404)
        :raise SystemError: when a conflict occurs (409) or when an unexpected HTTP status code is received
        """
        if data is not None:
            headers = self.__get_request_headers('Accept', 'Content-Type')
        else:
            headers = self.__get_request_headers('Accept')
        params = self._request_params.copy()
        params.update(request_params)
        params.update({
            "method": method,
            "headers": headers,
            "body": data
        })
        self.__response = self._http.fetch(uri, **params)
        self.__request_count += 1
        # for py3k compatibility...
#            if not isinstance(self.__content, str):
#                self.__content = self.__content.decode()
        if self.__response.code == 200:
            if self.__response.body:
                return json.loads(self.__response.body)
            else:
                return None
        elif self.__response.code == 201:
            return self.__response.headers['location']
        elif self.__response.code == 204:
            return None
        elif self.__response.code == 400:
            raise ValueError({
                "response": self.__response,
                "uri": uri,
                "data": data
            })
        elif self.__response.code == 404:
            raise LookupError(uri)
        elif self.__response.code == 409:
            raise SystemError(uri)
        else:
            raise SystemError(self.__response)

    def _get(self, uri, **kwargs):
        """
        Issue an HTTP GET request.
        
        :param uri: the URI of the resource to GET
        :return: object created from returned content (200) or C{None} (204)
        :raise KeyError: when URI is not found (404)
        :raise SystemError: when an unexpected HTTP status code is received
        """
        return self._request('GET', uri, **kwargs)

    def _post(self, uri, data, **kwargs):
        """
        Issue an HTTP POST request.
        
        :param uri: the URI of the resource to POST to
        :param data: unserialised object to be converted to JSON and passed in request payload
        :return: object created from returned content (200), C{Location} header value (201) or C{None} (204)
        :raise ValueError: when supplied data is not appropriate (400)
        :raise KeyError: when URI is not found (404)
        :raise SystemError: when an unexpected HTTP status code is received
        """
        return self._request('POST', uri, json.dumps(data), **kwargs)

    def _put(self, uri, data, **kwargs):
        """
        Issue an HTTP PUT request.
        
        :param uri: the URI of the resource to PUT
        :param data: unserialised object to be converted to JSON and passed in request payload
        :return: C{None} (204)
        :raise ValueError: when supplied data is not appropriate (400)
        :raise KeyError: when URI is not found (404)
        :raise SystemError: when an unexpected HTTP status code is received
        """
        return self._request('PUT', uri, json.dumps(data), **kwargs)

    def _delete(self, uri, **kwargs):
        """
        Issue an HTTP DELETE request.
        
        :param uri: the URI of the resource to PUT
        :return: C{None} (204)
        :raise KeyError: when URI is not found (404)
        :raise SystemError: when an unexpected HTTP status code is received
        """
        return self._request('DELETE', uri, **kwargs)

    def _lookup(self, key):
        """
        Look up a value in the resource index by key; will lazily load
        resource index if required and auto-correct URI from Content-Location
        header.
        
        :param key: the key of the value to look up in the resource index
        """
        if self._index is None:
            self._index = self._get(self._uri)
            if self.__response and 'content-location' in self.__response.headers:
                self._uri = self.__response.headers['content-location']
        if key in self._index:
            return self._index[key]
        else:
            raise KeyError(key)


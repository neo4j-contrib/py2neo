#!/usr/bin/env python

# Copyright 2011 Nigel Small
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
# 	http://www.apache.org/licenses/LICENSE-2.0
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


import httplib2
try:
	import json
except ImportError:
	import simplejson as json


class Resource(object):
	"""
	RESTful web service resource class, designed to work with a well-behaved
	hypermedia web service.

	:param uri:          the URI identifying this resource
	:param content_type: the content type required for data exchange
	:param index:        a previously obtained resource index for endpoint discovery
	:param http:         httplib2.Http object to use for requests
	:param user_name:    the user name to use for authentication
	:param password:     the password to use for authentication
	"""

	SUPPORTED_CONTENT_TYPES = ['application/json']

	def __init__(self, uri, content_type='application/json', index=None, http=None, user_name=None, password=None):
		if content_type not in self.SUPPORTED_CONTENT_TYPES:
			raise NotImplementedError("Content type {0} not supported".format(content_type))
		self._uri = uri
		self._base_uri = None
		self._relative_uri = None
		self._content_type = content_type
		self._http = http or httplib2.Http()
		if user_name is not None and password is not None:
			self._http.add_credentials(user_name, password)
		self._index = index

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

	def __get_request_headers(self, *keys):
		return dict([
			(key, self._content_type)
			for key in keys
			if key in ['Accept', 'Content-Type']
		])

	def _request(self, method, uri, data=None):
		"""
		Issue an HTTP request.
		
		:param method: the HTTP method to use for this call
		:param uri: the URI of the resource to access
		:param data: raw data to be passed in request payload (optional)
		:return: object created from returned content (200), C{Location} header value (201) or C{None} (204)
		:raise ValueError: when supplied data is not appropriate (400)
		:raise KeyError: when URI is not found (404)
		:raise SystemError: when a conflict occurs (409) or when an unexpected HTTP status is received
		"""
		if data is not None:
			headers = self.__get_request_headers('Accept', 'Content-Type')
		else:
			headers = self.__get_request_headers('Accept')
		try:
			self.__response, self.__content = self._http.request(
				uri, method, data, headers
			)
		except:
			raise IOError("Cannot send {0} request".format(method))
		if self.__response.status == 200:
			return json.loads(self.__content)
		elif self.__response.status == 201:
			return self.__response['location']
		elif self.__response.status == 204:
			return None
		elif self.__response.status == 400:
			raise ValueError((uri, data))
		elif self.__response.status == 404:
			raise LookupError(uri)
		elif self.__response.status == 409:
			raise SystemError(uri)
		else:
			raise SystemError({
				"response": self.__response,
				"content":  self.__content
			})

	def _get(self, uri):
		"""
		Issue an HTTP GET request.
		
		:param uri: the URI of the resource to GET
		:return: object created from returned content (200) or C{None} (204)
		:raise KeyError: when URI is not found (404)
		:raise SystemError: when an unexpected HTTP status is received
		"""
		return self._request('GET', uri)

	def _post(self, uri, data):
		"""
		Issue an HTTP POST request.
		
		:param uri: the URI of the resource to POST to
		:param data: unserialised object to be converted to JSON and passed in request payload
		:return: object created from returned content (200), C{Location} header value (201) or C{None} (204)
		:raise ValueError: when supplied data is not appropriate (400)
		:raise KeyError: when URI is not found (404)
		:raise SystemError: when an unexpected HTTP status is received
		"""
		return self._request('POST', uri, json.dumps(data))

	def _put(self, uri, data):
		"""
		Issue an HTTP PUT request.
		
		:param uri: the URI of the resource to PUT
		:param data: unserialised object to be converted to JSON and passed in request payload
		:return: C{None} (204)
		:raise ValueError: when supplied data is not appropriate (400)
		:raise KeyError: when URI is not found (404)
		:raise SystemError: when an unexpected HTTP status is received
		"""
		return self._request('PUT', uri, json.dumps(data))

	def _delete(self, uri):
		"""
		Issue an HTTP DELETE request.
		
		:param uri: the URI of the resource to PUT
		:return: C{None} (204)
		:raise KeyError: when URI is not found (404)
		:raise SystemError: when an unexpected HTTP status is received
		"""
		return self._request('DELETE', uri)

	def _lookup(self, key):
		"""
		Look up a value in the resource index by key; will lazily load
		resource index if required and auto-correct URI from Content-Location
		header.
		
		:param key: the key of the value to look up in the resource index
		"""
		if self._index is None:
			self._index = self._get(self._uri)
			if self.__response and 'content-location' in self.__response:
				self._uri = self.__response['content-location']
		if key in self._index:
			return self._index[key]
		else:
			raise KeyError(key)


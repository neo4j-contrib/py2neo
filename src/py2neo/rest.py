#!/usr/bin/env python

"""

Generic REST client

---

Copyright 2011 Three Steps Beyond LLP

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

"""


import httplib2
import json


__version__   = "0.8"
__author__    = "Nigel Small <nigel@nigelsmall.name>"
__copyright__ = "Copyright 2011 Three Steps Beyond LLP"
__license__   = "Apache License, Version 2.0"


class Resource(object):
	"""
	RESTful web service resource class, designed to work with a well-behaved
	hypermedia web service
	"""

	SUPPORTED_CONTENT_TYPES = ['application/json']

	def __init__(self, uri, content_type='application/json', http=None):
		"""
		Creates a new Resource instance
		
		@param uri:  the root URI for this resource
		@param http: httplib2.Http() object to use for requests (optional)
		
		"""
		if content_type not in self.SUPPORTED_CONTENT_TYPES:
			raise NotImplementedError("Content type %s not supported" % content_type)
		self._uri = unicode(uri)
		self._content_type = content_type
		self._http = http or httplib2.Http()
		self._index = None

	def __repr__(self):
		"""
		Returns a valid Python representation of this object
		"""
		return '%s(%s)' % (self.__class__.__name__, repr(self._uri))

	def __eq__(self, other):
		"""
		Determines equality of two objects based on URI
		"""
		return self._uri == other._uri

	def __ne__(self, other):
		"""
		Determines inequality of two objects based on URI
		"""
		return self._uri != other._uri

	def __get_request_headers(self, *keys):
		return dict([
			(key, self._content_type)
			for key in keys
			if key in ['Accept', 'Content-Type']
		])

	def _get(self, uri):
		"""
		Issues an HTTP GET request
		
		@param uri: the URI of the resource to GET
		@return: object created from returned content (200) or C{None} (204)
		@raise KeyError: when URI is not found (404)
		@raise SystemError: when an unexpected HTTP status is received
		
		"""
		try:
			(response, content) = self._http.request(uri, 'GET', None, self.__get_request_headers('Accept'))
		except:
			raise IOError("Cannot GET resource");
		if response.status == 200:
			return json.loads(content)
		elif response.status == 204:
			return None
		elif response.status == 404:
			raise LookupError(uri)
		else:
			raise SystemError(response)

	def _post(self, uri, data):
		"""
		Issues an HTTP POST request
		
		@param uri: the URI of the resource to POST to
		@param data: unserialised object to be converted to JSON and passed in request payload
		@return: object created from returned content (200), C{Location} header value (201) or C{None} (204)
		@raise ValueError: when supplied data is not appropriate (400)
		@raise KeyError: when URI is not found (404)
		@raise SystemError: when an unexpected HTTP status is received
		
		"""
		data = {} if data == None else json.dumps(data)
		try:
			(response, content) = self._http.request(uri, 'POST', data, self.__get_request_headers('Accept', 'Content-Type'))
		except:
			raise IOError("Cannot POST to resource");
		if response.status == 200:
			return json.loads(content)
		elif response.status == 201:
			return response['location']
		elif response.status == 204:
			return None
		elif response.status == 400:
			raise ValueError(data)
		elif response.status == 404:
			raise LookupError(uri)
		else:
			raise SystemError(response)

	def _put(self, uri, data):
		"""
		Issues an HTTP PUT request
		
		@param uri: the URI of the resource to PUT
		@param data: unserialised object to be converted to JSON and passed in request payload
		@return: C{None} (204)
		@raise ValueError: when supplied data is not appropriate (400)
		@raise KeyError: when URI is not found (404)
		@raise SystemError: when an unexpected HTTP status is received
		
		"""
		data = {} if data == None else json.dumps(data)
		try:
			(response, content) = self._http.request(uri, 'PUT', data, self.__get_request_headers('Accept', 'Content-Type'))
		except:
			raise IOError("Cannot PUT resource");
		if response.status == 204:
			return None
		elif response.status == 400:
			raise ValueError(data)
		elif response.status == 404:
			raise LookupError(uri)
		else:
			raise SystemError(response)

	def _delete(self, uri):
		"""
		Issues an HTTP DELETE request
		
		@param uri: the URI of the resource to PUT
		@return: C{None} (204)
		@raise KeyError: when URI is not found (404)
		@raise SystemError: when an unexpected HTTP status is received
		
		"""
		try:
			(response, content) = self._http.request(uri, 'DELETE', None, self.__get_request_headers('Accept'))
		except:
			raise IOError("Cannot DELETE resource");
		if response.status == 204:
			None
		elif response.status == 404:
			raise LookupError(uri)
		elif response.status == 409:
			raise SystemError(uri)
		else:
			raise SystemError(response)

	def _lookup(self, key):
		"""
		Looks up a value in the resource index by key; will lazily load
		resource index if required
		"""
		if(self._index == None):
			self._index = self._get(self._uri)
		if key in self._index:
			return self._index[key]
		else:
			raise KeyError(key)



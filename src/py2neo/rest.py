#!/usr/bin/env python

"""

py2neo - Python library for accessing Neo4j via REST interface

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

---

rest.py

"""

import httplib2
import json


class Resource:
	"""
	RESTful web service resource class, designed to work with a well-behaved
	hypermedia web service
	"""

	SUPPORTED_CONTENT_TYPES = ['application/json']

	def __init__(self, uri, content_type='application/json', http=None):
		"""
		Creates a new Resource instance
		
		Parameters:
			uri  - the root URI for this resource
			http - optional httplib2.Http() object to use for requests
		
		"""
		if content_type not in self.SUPPORTED_CONTENT_TYPES:
			raise NotImplementedError("Content type %s not supported" % content_type)
		self._uri = unicode(uri)
		self._content_type = content_type
		self._http = http or httplib2.Http()
		self._index = None

	def __repr__(self):
		return '%s(%s)' % (self.__class__.__name__, repr(self._uri))

	def _get_request_headers(self, *keys):
		return dict([
			(key, self._content_type)
			for key in keys
			if key in ['Accept', 'Content-Type']
		])

	def _get(self, uri):
		"""
		Issues an HTTP GET request
		
		Parameters:
			uri  - the URI of the resource to GET
			args - optional dictionary of URI template substitution values
		
		Result (dependent on HTTP status code):
			200 - returns value created from returned json content
			204 - returns None
			404 - raises a KeyError against the specified URI
			??? - raises a SystemError with the HTTP response
		
		"""
		response, content = self._http.request(uri, 'GET', None, self._get_request_headers('Accept'))
		if response.status == 200:
			return json.loads(content)
		elif response.status == 204:
			return None
		elif response.status == 404:
			raise KeyError(uri)
		else:
			raise SystemError(response)

	def _post(self, uri, data):
		"""
		Issues an HTTP POST request
		
		Parameters:
			uri  - the URI of the resource to POST to
			data - value to be converted to json and passed in request payload
			args - optional dictionary of URI template substitution values
		
		Result (dependent on HTTP status code):
			200 - returns value created from returned json content
			201 - returns value from "Location" header
			400 - raises a ValueError against the specified data
			404 - raises a KeyError against the specified URI
			??? - raises a SystemError with the HTTP response
		
		"""
		data = {} if data == None else json.dumps(data)
		response, content = self._http.request(uri, 'POST', data, self._get_request_headers('Accept', 'Content-Type'))
		if response.status == 200:
			return json.loads(content)
		elif response.status == 201:
			return response['location']
		elif response.status == 400:
			raise ValueError(data)
		elif response.status == 404:
			raise KeyError(uri)
		else:
			raise SystemError(response)

	def _put(self, uri, data):
		"""
		Issues an HTTP PUT request
		
		Parameters:
			uri  - the URI of the resource to PUT
			data - value to be converted to json and passed in request payload
			args - optional dictionary of URI template substitution values
		
		Result (dependent on HTTP status code):
			204 - success (no return value)
			400 - raises a ValueError against the specified data
			404 - raises a KeyError against the specified URI
			??? - raises a SystemError with the HTTP response
		
		"""
		data = {} if data == None else json.dumps(data)
		response, content = self._http.request(uri, 'PUT', data, self._get_request_headers('Accept', 'Content-Type'))
		if response.status == 204:
			pass
		elif response.status == 400:
			raise ValueError(data)
		elif response.status == 404:
			raise KeyError(uri)
		else:
			raise SystemError(response)

	def _delete(self, uri):
		"""
		Issues an HTTP DELETE request
		
		Parameters:
			uri  - the URI of the resource to DELETE
			args - optional dictionary of URI template substitution values
		
		Result (dependent on HTTP status code):
			204 - success (no return value)
			404 - raises a KeyError against the specified URI
			??? - raises a SystemError with the HTTP response
		
		"""
		response, content = self._http.request(uri, 'DELETE', None, self._get_request_headers('Accept'))
		if response.status == 204:
			pass
		elif response.status == 404:
			raise KeyError(uri)
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
			return None



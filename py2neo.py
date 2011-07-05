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

"""

import httplib2
import json


class Resource:
	"""
	Web service resource class, nothing here specific to neo4j but designed to
	work with a well-behaved hypermedia web service; used here as a base class
	for objects represented within the neo4j web service
	"""

	_read_headers = {
		"Accept": "application/json"
	}
	_write_headers = {
		"Accept": "application/json",
		"Content-Type": "application/json"
	}

	def __init__(self, uri, http=None):
		"""
		Creates a new Resource instance
		
		Parameters:
			uri  - the root URI for this resource
			http - optional httplib2.Http() object to use for requests
		
		"""
		if http == None:
			self._http = httplib2.Http()
		else:
			self._http = http
		self.uri = uri
		self._index = None

	def lookup(self, key):
		"""
		Looks up a value in the resource index by key; will lazily load
		resource index if required
		"""
		if(self._index == None):
			self._index = self._get(self.uri)
		return self._index[key]

	def _get(self, uri, args={}):
		"""
		Issues an HTTP GET request
		
		Parameters:
			uri  - the URI of the resource to GET
			args - optional dictionary containing URI template substitution values
		
		Result (dependent on HTTP status code):
			200 - returns value created from returned json content
			204 - returns None
			404 - raises a KeyError against the specified URI
			??? - raises a SystemError with the HTTP response
		
		"""
		response, content = self._http.request(uri.format(**args), "GET", None, self._read_headers)
		if response.status == 200:
			return json.loads(content)
		elif response.status == 204:
			return None
		elif response.status == 404:
			raise KeyError(uri)
		else:
			raise SystemError(response)

	def _post(self, uri, data, args={}):
		"""
		Issues an HTTP POST request
		
		Parameters:
			uri  - the URI of the resource to POST to
			data - a value to be converted to json and passed in request payload
			args - optional dictionary containing URI template substitution values
		
		Result (dependent on HTTP status code):
			201 - returns value from "Location" header
			400 - raises a ValueError against the specified data
			404 - raises a KeyError against the specified URI
			??? - raises a SystemError with the HTTP response
		
		"""
		data = {} if data == None else json.dumps(data)
		response, content = self._http.request(uri.format(**args), "POST", data, self._write_headers)
		if response.status == 201:
			return response["location"]
		elif response.status == 400:
			raise ValueError(data)
		elif response.status == 404:
			raise KeyError(uri)
		else:
			raise SystemError(response)

	def _put(self, uri, data, args={}):
		"""
		Issues an HTTP PUT request
		
		Parameters:
			uri  - the URI of the resource to PUT
			data - a value to be converted to json and passed in request payload
			args - optional dictionary containing URI template substitution values
		
		Result (dependent on HTTP status code):
			204 - success (no return value)
			400 - raises a ValueError against the specified data
			404 - raises a KeyError against the specified URI
			??? - raises a SystemError with the HTTP response
		
		"""
		data = {} if data == None else json.dumps(data)
		response, content = self._http.request(uri.format(**args), "PUT", data, self._write_headers)
		if response.status == 204:
			pass
		elif response.status == 400:
			raise ValueError(data)
		elif response.status == 404:
			raise KeyError(uri)
		else:
			raise SystemError(response)

	def _delete(self, uri, args={}):
		"""
		Issues an HTTP DELETE request
		
		Parameters:
			uri  - the URI of the resource to DELETE
			args - optional dictionary containing URI template substitution values
		
		Result (dependent on HTTP status code):
			204 - success (no return value)
			404 - raises a KeyError against the specified URI
			??? - raises a SystemError with the HTTP response
		
		"""
		response, content = self._http.request(uri.format(**args), "DELETE", None, self._read_headers)
		if response.status == 204:
			pass
		elif response.status == 404:
			raise KeyError(uri)
		else:
			raise SystemError(response)

	def __repr__(self):
		return repr(self.uri)

	def __str__(self):
		return str(self.uri)


class Direction:

	INCOMING = -1
	BOTH     =  0
	OUTGOING =  1

	@staticmethod
	def get_label(direction):
		if direction > 0:
			return "outgoing"
		elif direction < 0:
			return "incoming"
		else:
			return "all"


class GraphDatabaseService(Resource):

	def __init__(self, uri, http=None):
		Resource.__init__(self, uri, http)

	def create_node(self, properties=None):
		return Node(self._post(self.lookup("node"), properties), self._http)

	def get_reference_node(self):
		return Node(self.lookup("reference_node"), self._http)

	def get_subreference_node(self, type):
		"""
		Returns subreference node for given relationship type as defined by:
		http://wiki.neo4j.org/content/Design_Guide#Subreferences
		"""
		ref_node = self.get_reference_node()
		subref_nodes = ref_node.get_related_nodes(Direction.OUTGOING, type)
		if len(subref_nodes) == 0:
			subref_node = gdb.create_node()
			ref_node.create_relationship_to(subref_node, type)
		else:
			subref_node = subref_nodes[0]
		return subref_node

	def get_relationship_types(self):
		return self._get(self.lookup("relationship_types"))

	def get_node_indexes(self):
		indexes = self._get(self.lookup("node_index"))
		# replace with map()
		for i in indexes:
			index = indexes[i]
			indexes[i] = NodeIndex(index["type"], index["template"], index["provider"], self._http)
		return indexes

	def get_relationship_indexes(self):
		indexes = self._get(self.lookup("relationship_index"))
		# replace with map()
		for i in indexes:
			index = indexes[i]
			indexes[i] = RelationshipIndex(index["type"], index["template"], index["provider"], self._http)
		return indexes


class PropertyContainer(Resource):

	def __init__(self, uri, http=None):
		Resource.__init__(self, uri, http)

	def set_properties(self, properties):
		self._put(self.lookup("properties"), properties)

	def get_properties(self):
		return self._get(self.lookup("properties"))

	def remove_properties(self):
		self._delete(self.lookup("properties"))

	def __setitem__(self, key, value):
		self._put(self.lookup("property").format(key=key), value)

	def __getitem__(self, key):
		return self._get(self.lookup("property").format(key=key))

	def __delitem__(self, key):
		self._delete(self.lookup("property").format(key=key))


class Node(PropertyContainer):

	def __init__(self, uri, http=None):
		PropertyContainer.__init__(self, uri, http)

	def delete(self):
		self._delete(self._index["self"])

	def create_relationship_to(self, other_node, type, data=None):
		return Relationship(self._post(self.lookup("create_relationship"), {
			"to": str(other_node),
			"type": type,
			"data": data
		}), self._http)

	def get_relationships(self, direction, *types):
		prefix = Direction.get_label(direction)
		if len(types) == 0:
			uri = self.lookup(prefix + "_relationships")
		else:
			uri = self.lookup(prefix + "_typed_relationships").replace("{-list|&|types}", "&".join(types))
		return [Relationship(rel["self"], self._http) for rel in self._get(uri)]

	def get_related_nodes(self, direction, *types):
		prefix = Direction.get_label(direction)
		if len(types) == 0:
			uri = self.lookup(prefix + "_relationships")
		else:
			uri = self.lookup(prefix + "_typed_relationships").replace("{-list|&|types}", "&".join(types))
		return [Node(rel["start"] if rel["end"] == self.uri else rel["end"], self._http) for rel in self._get(uri)]


class Relationship(PropertyContainer):

	def __init__(self, uri, http=None):
		PropertyContainer.__init__(self, uri, http)
		self.type = self.lookup("type")
		self.data = self.lookup("data")

	def delete(self):
		self._delete(self.lookup("self"))

	def get_start_node(self):
		return Node(self.lookup("start"), self._http)

	def get_end_node(self):
		return Node(self.lookup("end"), self._http)


class Index(Resource):

	def __init__(self, type, template, provider, http=None):
		Resource.__init__(self, template, http)
		self._type = type
		self._provider = provider

	def _search(self, key, value):
		return self._get(self.uri, {"key": key, "value": value})

	def add(self, entity, key, value):
		self._post(self.uri, entity.uri, {"key": key, "value": value})


class NodeIndex(Index):

	def __init__(self, type, template, provider, http=None):
		Index.__init__(self, type, template, provider, http)

	def search(self, key, value):
		return [Node(item["self"], self._http) for item in self._search(key, value)]


class RelationshipIndex(Index):

	def __init__(self, type, template, provider, http=None):
		Index.__init__(self, type, template, provider, http)

	def search(self, key, value):
		return [Relationship(item["self"], self._http) for item in self._search(key, value)]



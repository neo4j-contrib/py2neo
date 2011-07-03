#!/usr/bin/env python

import httplib2
import json


class Resource:

	_read_headers = {
		"Accept": "application/json"
	}
	_write_headers = {
		"Accept": "application/json",
		"Content-Type": "application/json"
	}

	def __init__(self, uri, http=None):
		if http == None:
			self._http = httplib2.Http()
		else:
			self._http = http
		self.uri = uri
		# Grab the resource index if the URI isn't a template
		if "{" not in self.uri:
			self._index = self._get(self.uri)

	def _get(self, uri, args={}):
		if args == None:
			args = {}
		response, content = self._http.request(uri.format(**args), "GET", None, self._read_headers)
		if response.status == 200:
			return json.loads(content)
		elif response.status == 204:
			return None
		elif response.status == 404:
			raise KeyError(uri)
		else:
			raise SystemError("%s %s" % (response.status, response.reason))

	def _post(self, uri, data, args={}):
		if args == None:
			args = {}
		data = {} if data == None else json.dumps(data)
		response, content = self._http.request(uri.format(**args), "POST", data, self._write_headers)
		if response.status == 201:
			return response["location"]
		elif response.status == 400:
			raise ValueError(data)
		else:
			raise SystemError("%s %s" % (response.status, response.reason))

	def _put(self, uri, data, args={}):
		if args == None:
			args = {}
		data = {} if data == None else json.dumps(data)
		response, content = self._http.request(uri.format(**args), "PUT", data, self._write_headers)
		if response.status == 204:
			pass
		elif response.status == 400:
			raise ValueError(data)
		elif response.status == 404:
			raise KeyError(uri)
		else:
			raise SystemError("%s %s" % (response.status, response.reason))

	def _delete(self, uri, args={}):
		if args == None:
			args = {}
		response, content = self._http.request(uri.format(**args), "DELETE", None, self._read_headers)
		if response.status == 204:
			pass
		elif response.status == 404:
			raise KeyError(uri)
		else:
			raise SystemError("%s %s" % (response.status, response.reason))

	def __repr__(self):
		return self.uri


class GraphDatabaseService(Resource):

	def __init__(self, uri, http=None):
		Resource.__init__(self, uri, http)

	def create_node(self, properties=None):
		return Node(self._post(self._index["node"], properties), self._http)

	def get_reference_node(self):
		return Node(self._index["reference_node"], self._http)

	def get_relationship_types(self):
		return self._get(self._index["relationship_types"])

	def get_node_indexes(self):
		indexes = self._get(self._index["node_index"])
		# replace with map()
		for i in indexes:
			index = indexes[i]
			indexes[i] = NodeIndex(index["type"], index["template"], index["provider"], self._http)
		return indexes

	def get_relationship_indexes(self):
		indexes = self._get(self._index["relationship_index"])
		# replace with map()
		for i in indexes:
			index = indexes[i]
			indexes[i] = RelationshipIndex(index["type"], index["template"], index["provider"], self._http)
		return indexes


class PropertyContainer(Resource):

	def __init__(self, uri, http=None):
		Resource.__init__(self, uri, http)

	def set_properties(self, properties):
		self._put(self._index["properties"], properties)

	def get_properties(self):
		return self._get(self._index["properties"])

	def remove_properties(self):
		self._delete(self._index["properties"])

	def __setitem__(self, key, value):
		self._put(self._index["property"].format(key=key), value)

	def __getitem__(self, key):
		return self._get(self._index["property"].format(key=key))

	def __delitem__(self, key):
		self._delete(self._index["property"].format(key=key))


class Node(PropertyContainer):

	def __init__(self, uri, http=None):
		PropertyContainer.__init__(self, uri, http)

	def delete(self):
		self._delete(self._index["self"])

	def create_relationship_to(self, other_node, type, data=None):
		return Relationship(self._post(self._index["create_relationship"], {
			"to": str(other_node),
			"type": type,
			"data": data
		}), self._http)

	def get_all_relationships(self, *types):
		if len(types) == 0:
			uri = self._index["all_relationships"]
		else:
			uri = self._index["all_typed_relationships"].replace("{-list|&|types}", "&".join(types))
		return self._get(uri)

	def get_incoming_relationships(self, *types):
		if len(types) == 0:
			uri = self._index["incoming_relationships"]
		else:
			uri = self._index["incoming_typed_relationships"].replace("{-list|&|types}", "&".join(types))
		return self._get(uri)

	def get_outgoing_relationships(self, *types):
		if len(types) == 0:
			uri = self._index["outgoing_relationships"]
		else:
			uri = self._index["outgoing_typed_relationships"].replace("{-list|&|types}", "&".join(types))
		return self._get(uri)


class Relationship(PropertyContainer):

	def __init__(self, uri, http=None):
		PropertyContainer.__init__(self, uri, http)
		self.type = self._index["type"]
		self.data = self._index["data"]

	def delete(self):
		self._delete(self._index["self"])

	def get_start_node(self):
		return Node(self._index["start"], self._http)

	def get_end_node(self):
		return Node(self._index["end"], self._http)


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



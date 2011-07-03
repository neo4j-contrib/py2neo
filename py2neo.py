#!/usr/bin/env python

import httplib2
import json


class Resource:

	_GET_headers = {
		"Accept": "application/json"
	}
	_POST_headers = {
		"Accept": "application/json",
		"Content-Type": "application/json"
	}

	def __init__(self, uri):
		self._http = httplib2.Http()
		self.uri = uri
		self._index = self._get(self.uri)

	def _get(self, uri, args={}):
		if args == None:
			args = {}
		response, content = self._http.request(uri.format(**args), "GET", None, self._GET_headers)
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
		response, content = self._http.request(uri.format(**args), "POST", data, self._POST_headers)
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
		response, content = self._http.request(uri.format(**args), "PUT", data, self._POST_headers)
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
		response, content = self._http.request(uri.format(**args), "DELETE", None, self._GET_headers)
		if response.status == 204:
			pass
		elif response.status == 404:
			raise KeyError(uri)
		else:
			raise SystemError("%s %s" % (response.status, response.reason))

	def __repr__(self):
		return self.uri


class GraphDatabaseService(Resource):

	def __init__(self, uri):
		Resource.__init__(self, uri)

	def create_node(self, properties=None):
		return Node(self._post(self._index["node"], properties))

	def get_reference_node(self):
		return Node(self._index["reference_node"])

	def get_relationship_types(self):
		return self._get(self._index["relationship_types"])

	def get_node_indexes(self):
		indexes = self._get(self._index["node_index"])
		# replace with map()
		for i in indexes:
			index = indexes[i]
			indexes[i] = NodeIndex(index["type"], index["template"], index["provider"])
		return indexes

	def get_relationship_indexes(self):
		indexes = self._get(self._index["relationship_index"])
		# replace with map()
		for i in indexes:
			index = indexes[i]
			indexes[i] = RelationshipIndex(index["type"], index["template"], index["provider"])
		return indexes


class Node(Resource):

	def __init__(self, uri):
		Resource.__init__(self, uri)

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

	def delete(self):
		self._delete(self._index["self"])

	def create_relationship(self, to, data, type):
		return Relationship(self._post(self._index["create_relationship"], {
			"to": str(to),
			"data": data,
			"type": type
		}))

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


class Relationship(Resource):

	def __init__(self, uri):
		Resource.__init__(self, uri)

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

	def delete(self):
		self._delete(self._index["self"])


class NodeIndex(Resource):

	def __init__(self, type, template, provider):
		self._http = httplib2.Http()
		self._type = type
		self.uri = template
		self._provider = provider

	def search(self, key, value):
		return self._get(self.uri, {"key": key, "value": value})

class RelationshipIndex(Resource):

	def __init__(self, type, template, provider):
		self._http = httplib2.Http()
		self._type = type
		self.uri = template
		self._provider = provider

	def search(self, key, value):
		return self._get(self.uri, {"key": key, "value": value})



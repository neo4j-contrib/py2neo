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
Neo4j client using REST interface
"""


import rest


__version__   = "0.8"
__author__    = "Nigel Small <py2neo@nigelsmall.org>"
__copyright__ = "Copyright 2011 Nigel Small"
__license__   = "Apache License, Version 2.0"


class Direction(object):
	"""
	Used to define the direction of a C{Relationship}.
	"""

	BOTH     = 'all'
	INCOMING = 'incoming'
	OUTGOING = 'outgoing'


class GraphDatabaseService(rest.Resource):
	"""
	Represents an instance of a U{Neo4j <http://neo4j.org/>} database
	identified by its base URI. Generally speaking, this is the only URI which
	a system attaching to this service should need to be aware of; all further
	entity URIs will be discovered automatically from within response content
	(see U{Hypermedia <http://en.wikipedia.org/wiki/Hypermedia>}).
	
	The following code illustrates how to attach to a database and obtain its
	reference node:
	
		>>> from py2neo import neo4j
		>>> gdb = neo4j.GraphDatabaseService("http://localhost:7474/db/data")
		>>> gdb.get_reference_node()
		Node(u'http://localhost:7474/db/data/node/0')
	
	"""

	def __init__(self, uri, http=None):
		"""
		Creates a representation of a U{Neo4j <http://neo4j.org/>} database
		instance identified by URI.
		
		@param uri:  the base URI of the database
		@param http: httplib2.Http object to use for requests (optional)
		
		"""
		rest.Resource.__init__(self, uri, http=http)
		self._extensions = self._lookup('extensions')

	def create_node(self, properties=None):
		"""
		Creates a new C{Node} within the database instance with specific
		properties, if supplied.
		
		@param properties: a dictionary of properties to attach to this C{Node} (optional)
		@return: a C{Node} instance representing the newly created node
		
		"""
		return Node(self._post(self._lookup('node'), properties), http=self._http)

	def get_reference_node(self):
		"""
		Returns a C{Node} object representing the reference node for this
		database instance.
		
		@return: a C{Node} instance representing the database reference node
		
		"""
		return Node(self._lookup('reference_node'), http=self._http)

	def get_relationship_types(self):
		"""
		Returns a list of C{Relationship} names currently defined within this
		database instance.
		
		@return: a list of C{Relationship} names
		
		"""
		return self._get(self._lookup('relationship_types'))

	def create_node_index(self, name, config=None):
		"""
		Creates a new C{Node} C{Index} with the supplied name and
		configuration.
		
		@return: an C{Index} instance representing the newly created index
		
		"""
		return Index(Node, uri=self._post(self._lookup('node_index'), {
			'name': name,
			'config': config or {}
		}), http=self._http)

	def get_node_indexes(self):
		"""
		Returns a dictionary of all available C{Node} C{Index}es within this
		database instance.
		
		@return: a dictionary of Name : C{Index} mappings for all C{Node} C{Index}es
		
		"""
		indexes = self._get(self._lookup('node_index')) or {}
		return dict([
			(index, Index(Node, template_uri=indexes[index]['template'], http=self._http))
			for index in indexes
		])

	def create_relationship_index(self, name, config=None):
		"""
		Creates a new C{Relationship} C{Index} with the supplied name and
		configuration.
		
		@return: an C{Index} instance representing the newly created index
		
		"""
		return Index(Relationship, uri=self._post(self._lookup('relationship_index'), {
			'name': name,
			'config': config or {}
		}), http=self._http)

	def get_relationship_indexes(self):
		"""
		Returns a dictionary of all available C{Relationship} C{Index}es within
		this database instance.
		
		@return: a dictionary of Name : C{Index} mappings for all C{Relationship} C{Index}es
		
		"""
		indexes = self._get(self._lookup('relationship_index')) or {}
		return dict([
			(index, Index(Relationship, template_uri=indexes[index]['template'], http=self._http))
			for index in indexes
		])

	def execute(self, plugin_name, function_name, data):
		"""
		Executes a POST request against the specified plugin function using the
		supplied data.
		
		@param plugin_name: the name of the plugin to call
		@param function_name: the name of the function to call within in the specified plugin
		@param data: the data to pass to the function call
		@raise NotImplementedError: when the specfifed plugin or function is not available
		@return: the data returned from the function call
		
		"""
		if plugin_name not in self._extensions:
			raise NotImplementedError(plugin_name)
		plugin = self._extensions[plugin_name]
		if function_name not in plugin:
			raise NotImplementedError(plugin_name + "." + function_name)
		function_uri = self._extensions[plugin_name][function_name]
		return self._post(function_uri, data)

	def execute_cypher_query(self, query):
		"""
		Executes the supplied query using the CypherPlugin, if available.
		
		@param query: a string containing the Cypher query to execute
		@raise NotImplementedError: if the Cypher plugin is not available
		@return: the result of the Cypher query
		
		"""
		return self.execute('CypherPlugin', 'execute_query', {
			'query': query
		});

	def execute_gremlin_script(self, script):
		"""
		Executes the supplied script using the GremlinPlugin, if available.
		
		@param script: a string containing the Gremlin script to execute
		@raise NotImplementedError: if the Gremlin plugin is not available
		@return: the result of the Gremlin script
		
		"""
		return self.execute('GremlinPlugin', 'execute_script', {
			'script': script
		});


class IndexableResource(rest.Resource):
	"""
	Base class from which C{Node} and C{Relationship} classes inherit.
	Extends a C{rest.Resource} by allowing additional URIs to be stored which
	represent both an C{Index} and the entry within that C{Index} which points
	to this resource. Additionally, provides property containment
	functionality.
	"""

	def __init__(self, uri, index_entry_uri=None, index_uri=None, http=None):
		"""
		Creates a representation of an indexable resource (C{Node} or
		C{Relationship}) identified by URI; optionally accepts further URIs
		representing both an C{Index} for this resource type plus the specific
		entry within that C{Index}.
		
		@param uri:  the URI identifying this resource
		@param index_entry_uri:  the URI of the entry in an C{Index} pointing to this resource (optional)
		@param index_uri:  the URI of the C{Index} containing the above entry (optional)
		@param http: httplib2.Http object to use for requests (optional)
		
		"""
		rest.Resource.__init__(self, uri, http=http)
		self._index_entry_uri = index_entry_uri
		self._index_uri = index_uri
		self._id = int('0' + uri.rpartition('/')[-1])

	def __repr__(self):
		if self._index_entry_uri is None:
			return '%s(%s)' % (self.__class__.__name__, repr(self._uri))
		else:
			return '%s(%s)' % (self.__class__.__name__, repr(self._index_entry_uri))

	def __eq__(self, other):
		"""
		Determines equality of two resource representations based on both URI
		and C{Index} entry URI, if available.
		"""
		return self._uri == other._uri and self._index_entry_uri == other._index_entry_uri

	def __ne__(self, other):
		"""
		Determines inequality of two resource representations based on both URI
		and C{Index} entry URI, if available.
		"""
		return self._uri != other._uri or self._index_entry_uri != other._index_entry_uri

	def __getitem__(self, key):
		"""
		Returns a named property for this resource.
		"""
		return self._get(self._lookup('property').format(key=key))

	def __setitem__(self, key, value):
		"""
		Sets a named property for this resource to the supplied value.
		"""
		self._put(self._lookup('property').format(key=key), value)

	def __delitem__(self, key):
		"""
		Deletes a named property for this resource.
		"""
		self._delete(self._lookup('property').format(key=key))

	def get_properties(self):
		"""
		Returns all properties for this resource.
		"""
		return self._get(self._lookup('properties'))

	def set_properties(self, properties):
		"""
		Sets all properties for this resource to the supplied values.
		"""
		self._put(self._lookup('properties'), properties)

	def remove_properties(self):
		"""
		Deletes all properties for this resource.
		"""
		self._delete(self._lookup('properties'))

	def delete(self):
		"""
		Deletes this resource from the database instance.
		"""
		self._delete(self._lookup('self'))


class Node(IndexableResource):
	"""
	Represents a C{Node} within a U{Neo4j <http://neo4j.org/>} database instance
	identified by a URI. This class is a subclass of C{IndexableResource} and,
	as such, may also contain URIs identifying how this C{Node} is represented
	within an C{Index}.
	"""

	def __init__(self, uri, index_entry_uri=None, index_uri=None, http=None):
		"""
		Creates a representation of a C{Node} identified by URI; optionally
		accepts further URIs representing both an C{Index} for C{Node}s plus
		the specific entry within that C{Index}.
		
		@param uri:  the URI identifying this C{Node}
		@param index_entry_uri:  the URI of the entry in an C{Index} pointing to this C{Node} (optional)
		@param index_uri:  the URI of the C{Index} containing the above C{Node} entry (optional)
		@param http: httplib2.Http object to use for requests (optional)
		
		"""
		IndexableResource.__init__(self, uri, index_entry_uri=index_entry_uri, index_uri=index_uri, http=http)

	def __str__(self):
		"""
		Returns a human-readable string representation of this C{Node}
		object, e.g.:
		
			>>> print str(my_node)
			'(42)'
		
		"""
		return "(%d)" % self._id

	def create_relationship_to(self, other_node, type, data=None):
		"""
		Creates a new C{Relationship} of type C{type} from the C{Node}
		represented by the current instance to the C{Node} represented by
		C{other_node}.
		
		@param other_node: an end C{Node} for the new C{Relationship}
		@param type: the type of the new C{Relationship}
		@param data: the data to attach to the new C{Relationship} (optional)
		@return: the newly created C{Relationship}
		
		"""
		return Relationship(self._post(self._lookup('create_relationship'), {
			'to': other_node._uri,
			'type': type,
			'data': data
		}), http=self._http)

	def get_relationships(self, direction, *types):
		"""
		Returns all C{Relationship}s from the current C{Node} in a given
		C{direction} of a specific C{type} (if supplied).
		
		@param direction: a string constant from the C{Direction} class
		@param types: the types of C{Relationship}s to include (optional)
		@return: a list of C{Relationship}s matching the specified criteria
		
		"""
		if len(types) == 0:
			uri = self._lookup(direction + '_relationships')
		else:
			uri = self._lookup(direction + '_typed_relationships').replace('{-list|&|types}', '&'.join(types))
		return [
			Relationship(rel['self'], http=self._http)
			for rel in self._get(uri)
		]

	def get_related_nodes(self, direction, *types):
		"""
		Returns all C{Node}s related to the current C{Node} by a
		C{Relationship} in a given C{direction} of a specific C{type}
		(if supplied).
		
		@param direction: a string constant from the C{Direction} class
		@param types: the types of C{Relationship}s to include (optional)
		@return: a list of C{Node}s matching the specified criteria
		
		"""
		if len(types) == 0:
			uri = self._lookup(direction + '_relationships')
		else:
			uri = self._lookup(direction + '_typed_relationships').replace('{-list|&|types}', '&'.join(types))
		return [
			Node(
				rel['start'] if rel['end'] == self._uri else rel['end'],
				http=self._http
			)
			for rel in self._get(uri)
		]

	def get_traverser(self):
		"""
		Returns a C{Traverser} instance for the current C{Node}.
		
		@return: a C{Traverser} for this C{Node}
		
		"""
		return Traverser(self._lookup('traverse').format(returnType='path'), http=self._http)


class Relationship(IndexableResource):
	"""
	Represents a C{Relationship} within a U{Neo4j <http://neo4j.org/>} database
	instance identified by a URI. This class is a subclass of
	C{IndexableResource} and, as such, may also contain URIs identifying how
	this C{Relationship} is represented within an C{Index}.
	"""

	def __init__(self, uri, index_entry_uri=None, index_uri=None, http=None):
		"""
		Creates a representation of a C{Relationship} identified by URI;
		optionally accepts further URIs representing both an C{Index} for
		C{Relationship}s plus the specific entry within that C{Index}.
		
		@param uri:  the URI identifying this C{Relationship}
		@param index_entry_uri:  the URI of the entry in an C{Index} pointing to this C{Relationship} (optional)
		@param index_uri:  the URI of the C{Index} containing the above C{Relationship} entry (optional)
		@param http: httplib2.Http object to use for requests (optional)
		
		"""
		IndexableResource.__init__(self, uri, index_entry_uri=index_entry_uri, index_uri=index_uri, http=http)
		self.type = self._lookup('type')
		self.data = self._lookup('data')

	def __str__(self):
		"""
		Returns a human-readable string representation of this C{Relationship}
		object, e.g.:
		
			>>> print str(my_rel)
			'-[:KNOWS]->'
		
		"""
		return "-[:%s]->" % self.type

	def get_start_node(self):
		"""
		Returns a C{Node} object representing the start node within this
		C{Relationship}.
		
		@return: the start C{Node} of this C{Relationship}
		
		"""
		return Node(self._lookup('start'), http=self._http)

	def get_end_node(self):
		"""
		Returns a C{Node} object representing the end node within this
		C{Relationship}.
		
		@return: the end C{Node} of this C{Relationship}
		
		"""
		return Node(self._lookup('end'), http=self._http)


class Path(object):
	"""
	A string of C{Node}s connected by C{Relationships}. A list of C{Path}s
	may be obtained as the result of a traversal:
	
		>>> from py2neo import neo4j
		>>> gdb = neo4j.GraphDatabaseService("http://localhost:7474/db/data")
		>>> ref_node = gdb.get_reference_node()
		>>> t = ref_node.get_traverser()
		>>> t.set_max_depth(3)
		>>> for path in t.traverse_depth_first():
		... 	print path
		(0)-[:CUSTOMERS]->(1)
		(0)-[:CUSTOMERS]->(1)-[:CUSTOMER]->(42)
		(0)-[:CUSTOMERS]->(1)-[:CUSTOMER]->(43)
		(0)-[:CUSTOMERS]->(1)-[:CUSTOMER]->(44)
	
	"""

	def __init__(self, nodes, relationships):
		"""
		Constructs a new C{Path} object from a list of C{Node}s and a list of
		C{Relationship}s. There should always be exactly one more C{Node} than
		there are C{Relationship}s.
		
		@raise KeyError: when number of C{Node}s is not exactly one more than number of C{Relationship}s
		
		"""
		if len(nodes) - len(relationships) == 1:
			self._nodes = nodes
			self._relationships = relationships
		else:
			raise ValueError

	def __str__(self):
		"""
		Returns a human-readable string representation of this C{Path}
		object, e.g.:
		
			>>> print str(my_path)
			'(0)-[:CUSTOMERS]->(1)-[:CUSTOMER]->(42)'
		
		"""
		return "".join([
			str(self._nodes[i]) + str(self._relationships[i])
			for i in range(len(self._relationships))
		]) + str(self._nodes[-1])

	def __len__(self):
		"""
		Returns the length of this C{Path} (equivalent to the number of
		C{Relationship}s).
		"""
		return len(self._relationships)

	def get_nodes(self):
		"""
		Returns a list of all the C{Node}s which make up this C{Path}.
		
		@return: a list of C{Node}s for this C{Path}
		
		"""
		return self._nodes

	def get_relationships(self):
		"""
		Returns a list of all the C{Relationship}s which make up this C{Path}.
		
		@return: a list of C{Relationship}s for this C{Path}
		
		"""
		return self._relationships

	def get_start_node(self):
		"""
		Returns a C{Node} object representing the first node within this
		C{Path}.
		
		@return: the first C{Node} in this C{Path}
		
		"""
		return self._nodes[0]

	def get_end_node(self):
		"""
		Returns a C{Node} object representing the last node within this
		C{Path}.
		
		@return: the last C{Node} in this C{Path}
		
		"""
		return self._nodes[-1]

	def get_last_relationship(self):
		"""
		Returns a C{Relationship} object representing the last relationship
		within this C{Path}.
		
		@return: the last C{Relationship} in this C{Path} or C{None} if zero length
		
		"""
		return self._relationships[-1] if len(self._relationships) > 0 else None


class Index(rest.Resource):
	"""
	Represents an C{Index} within a U{Neo4j <http://neo4j.org/>} database
	instance identified by a URI and/or a template URI. With a nod to Java
	generics, an C{Index} instance may hold either C{Node}s or C{Relationship}s
	by supplying the appropriate class directly to the constructor. For
	example:
	
		>>> from py2neo import neo4j
		>>> Index(neo4j.Node, "http://localhost:7474/db/data/index/node/index1")
		Index<Node>(u'http://localhost:7474/db/data/index/node/index1')
	
	"""

	def __init__(self, T, uri=None, template_uri=None, http=None):
		rest.Resource.__init__(
			self,
			uri or template_uri.rpartition("/{key}/{value}")[0],
			http=http
		)
		self.__T = T
		self._template_uri = template_uri or "%s/{key}/{value}" % uri

	def __repr__(self):
		return '%s<%s>(%s)' % (
			self.__class__.__name__,
			self.__T.__name__,
			repr(self._uri)
		)

	def add(self, indexable_resource, key, value):
		return Node(self._post(self._template_uri.format(key=key, value=value), indexable_resource._uri))

	def remove(self, indexable_resource):
		if indexable_resource._index_uri == self._uri and indexable_resource._index_entry_uri is not None:
			self._delete(indexable_resource._index_entry_uri)
		else:
			raise LookupError(indexable_resource)

	def search(self, key, value):
		return [
			self.__T(
				item['self'],
				index_entry_uri=item['indexed'],
				index_uri=self._uri,
				http=self._http
			)
			for item in self._get(self._template_uri.format(key=key, value=value))
		]


class Traverser(rest.Resource):
	"""
	An engine designed to traverse a U{Neo4j <http://neo4j.org/>} database
	starting at a specific C{Node}. Allows traversal parameters to be set
	before calling one of the supplied traversal routines.
	"""

	class Order:

		BREADTH_FIRST = 'breadth_first'
		DEPTH_FIRST   = 'depth_first'

	def __init__(self, uri, http=None):
		rest.Resource.__init__(self, uri, http=http)
		self._criteria = {
			'order': Traverser.Order.DEPTH_FIRST,
			'uniqueness': 'node_path'
		}

	def set_max_depth(self, max_depth):
		if 'prune_evaluator' in self._criteria:
			del self._criteria['prune_evaluator']
		self._criteria['max_depth'] = max_depth

	def set_prune_evaluator(self, language, body):
		if 'max_depth' in self._criteria:
			del self._criteria['max_depth']
		self._criteria['prune_evaluator'] = {
			'language': language,
			'body': body
		}

	def set_return_filter(self, language, name):
		self._criteria['return_filter'] = {
			'language': language,
			'name': name
		}

	def add_relationship(self, type, direction=None):
		if 'relationships' not in self._criteria:
			self._criteria['relationships'] = []
		if direction:
			self._criteria['relationships'].append({
				'type': type,
				'direction': direction
			})
		else:
			self._criteria['relationships'].append({
				'type': type
			})

	def remove_relationship(self, type):
		if 'relationships' in self._criteria:
			self._criteria['relationships'] = [
				item
				for item in self._criteria['relationships']
				if item['type'] != type
			]
			if self._criteria['relationships'] == []:
				del self._criteria['relationships']

	def traverse(self, order):
		self._criteria['order'] = order
		return [
			Path([
				Node(uri)
				for uri in path['nodes']
			], [
				Relationship(uri)
				for uri in path['relationships']
			])
			for path in self._post(self._uri, self._criteria)
		]

	def traverse_depth_first(self):
		return self.traverse(Traverser.Order.DEPTH_FIRST)

	def traverse_breadth_first(self):
		return self.traverse(Traverser.Order.BREADTH_FIRST)



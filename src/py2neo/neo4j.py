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


__version__   = "0.94"
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

	def __init__(self, uri, http=None, user_name=None, password=None):
		"""
		Creates a representation of a U{Neo4j <http://neo4j.org/>} database
		instance identified by URI.
		
		@param uri:       the base URI of the database
		@param http:      httplib2.Http object to use for requests (optional)
		@param user_name: the user name to use for authentication (optional)
		@param password:  the password to use for authentication (optional)
		
		"""
		rest.Resource.__init__(self, uri, http=http, user_name=user_name, password=password)
		if self._uri.endswith("/"):
			self._base_uri, self._relative_uri = self._uri.rpartition("/")[0:2]
		else:
			self._base_uri, self._relative_uri = self._uri, "/"
		self._batch_uri = self._base_uri + "/batch"
		self._extensions = self._lookup('extensions')

	def create_node(self, properties=None):
		"""
		Creates a new C{Node} within the database instance with specific
		properties, if supplied.
		
		@param properties: a dictionary of properties to attach to this C{Node} (optional)
		@return: a C{Node} instance representing the newly created node
		
		"""
		return Node(
			self._post(self._lookup('node'), properties),
			http=self._http
		)

	def create_nodes(self, *properties):
		"""
		Creates new C{Node}s for all supplied properties as part of a single
		batch.
		
		@param properties: a dictionary of properties to attach to a C{Node} (multiple)
		@return: a list of C{Node} instances representing the newly created nodes
		
		"""
		node_uri = "".join(self._lookup('node').partition("/node")[1:3])
		return [
			Node(result['location'], http=self._http)
			for result in self._post(self._batch_uri, [
				{"method": "POST", "to": node_uri, "body": properties[i], "id": i}
				for i in range(len(properties))
			])
		]

	def get_reference_node(self):
		"""
		Returns a C{Node} object representing the reference node for this
		database instance.
		
		@return: a C{Node} instance representing the database reference node
		
		"""
		return Node(self._lookup('reference_node'), http=self._http)

	def get_subreference_node(self, name):
		"""
		Returns a C{Node} object representing a named subreference node
		within this database instance. If such a node does not exist, one is
		created.
		
		@return: a C{Node} instance representing the named subreference node
		
		"""
		ref_node = self.get_reference_node()
		subref_node = ref_node.get_single_related_node(Direction.OUTGOING, name)
		if subref_node is None:
			subref_node = self.create_node()
			ref_node.create_relationship_to(subref_node, name)
		return subref_node

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

	def __init__(self, uri, index_entry_uri=None, index_uri=None, http=None, user_name=None, password=None):
		"""
		Creates a representation of an indexable resource (C{Node} or
		C{Relationship}) identified by URI; optionally accepts further URIs
		representing both an C{Index} for this resource type plus the specific
		entry within that C{Index}.
		
		@param uri:             the URI identifying this resource
		@param index_entry_uri: the URI of the entry in an C{Index} pointing to this resource (optional)
		@param index_uri:       the URI of the C{Index} containing the above entry (optional)
		@param http:            httplib2.Http object to use for requests (optional)
		@param user_name:       the user name to use for authentication (optional)
		@param password:        the password to use for authentication (optional)
		
		"""
		rest.Resource.__init__(self, uri, http=http, user_name=user_name, password=password)
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

	def get_id(self):
		"""
		Returns the unique ID of this resource.
		"""
		return self._id

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

	def __init__(self, uri, index_entry_uri=None, index_uri=None, http=None, user_name=None, password=None):
		"""
		Creates a representation of a C{Node} identified by URI; optionally
		accepts further URIs representing both an C{Index} for C{Node}s plus
		the specific entry within that C{Index}.
		
		@param uri:             the URI identifying this C{Node}
		@param index_entry_uri: the URI of the entry in an C{Index} pointing to this C{Node} (optional)
		@param index_uri:       the URI of the C{Index} containing the above C{Node} entry (optional)
		@param http:            httplib2.Http object to use for requests (optional)
		@param user_name:       the user name to use for authentication (optional)
		@param password:        the password to use for authentication (optional)
		
		"""
		IndexableResource.__init__(self, uri, index_entry_uri=index_entry_uri,
		                           index_uri=index_uri, http=http,
		                           user_name=user_name, password=password)
		self._base_uri, u0, u1 = self._uri.partition("/node")
		self._relative_uri = u0 + u1

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

	def get_single_relationship(self, direction, type):
		"""
		Returns only one C{Relationship} from the current C{Node} in the given
		C{direction} of the specified C{type}, if any such relationships exist.
		
		@param direction: a string constant from the C{Direction} class
		@param type: the type of C{Relationship} to return
		@return: a single C{Relationship} matching the specified criteria or C{None}
		
		"""
		relationships = self.get_relationships(direction, type)
		return relationships[0] if len(relationships) > 0 else None

	def has_relationship(self, direction, *types):
		"""
		Returns C{True} if this C{Node} has any C{Relationship}s with the
		specified criteria, C{False} otherwise.
		
		@param direction: a string constant from the C{Direction} class
		@param types: the types of C{Relationship}s to include (optional)
		@return: C{True} if this C{Node} has any matching C{Relationship}s
		
		"""
		relationships = self.get_relationships(direction, *types)
		return True if len(relationships) > 0 else False

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

	def get_single_related_node(self, direction, type):
		"""
		Returns only one C{Node} related to the current C{Node} by a
		C{Relationship} in the given C{direction} of the specified C{type}, if
		any such relationships exist.
		
		@param direction: a string constant from the C{Direction} class
		@param type: the type of C{Relationship} to include
		@return: a single C{Node} matching the specified criteria or C{None}
		
		"""
		nodes = self.get_related_nodes(direction, type)
		return nodes[0] if len(nodes) > 0 else None

	def traverse(self, order=None, uniqueness=None, relationships=None, prune=None, filter=None, max_depth=None):
		"""
		Returns a C{Traverser} instance for the current C{Node}.
		
			>>> t = t1.traverse(order="depth_first",
			...                 max_depth=2,
			...                 relationships=[("KNOWS","out"), "LOVES"],
			...                 prune=("javascript", "position.endNode().getProperty('foo') == 'bar';")
			... )
		
		@return: a C{Traverser} for this C{Node}
		
		"""
		td = TraversalDescription()
		if order:
			td = td.order(order)
		if uniqueness:
			td = td.uniqueness(uniqueness)
		if relationships:
			for relationship in (relationships or []):
				if isinstance(relationship, (str,unicode)):
					td = td.relationships(relationship)
				else:
					td = td.relationships(*relationship)
		if prune:
			td = td.prune(prune[0], prune[1])
		if filter:
			td = td.filter(filter[0], filter[1])
		if max_depth:
			td = td.max_depth(max_depth)
		return td.traverse(self)


class Relationship(IndexableResource):
	"""
	Represents a C{Relationship} within a U{Neo4j <http://neo4j.org/>} database
	instance identified by a URI. This class is a subclass of
	C{IndexableResource} and, as such, may also contain URIs identifying how
	this C{Relationship} is represented within an C{Index}.
	"""

	def __init__(self, uri, index_entry_uri=None, index_uri=None, http=None, user_name=None, password=None):
		"""
		Creates a representation of a C{Relationship} identified by URI;
		optionally accepts further URIs representing both an C{Index} for
		C{Relationship}s plus the specific entry within that C{Index}.
		
		@param uri:             the URI identifying this C{Relationship}
		@param index_entry_uri: the URI of the entry in an C{Index} pointing to this C{Relationship} (optional)
		@param index_uri:       the URI of the C{Index} containing the above C{Relationship} entry (optional)
		@param http:            httplib2.Http object to use for requests (optional)
		@param user_name:       the user name to use for authentication (optional)
		@param password:        the password to use for authentication (optional)
		
		"""
		IndexableResource.__init__(self, uri, index_entry_uri=index_entry_uri,
		                           index_uri=index_uri, http=http,
		                           user_name=user_name, password=password)
		self._base_uri, u0, u1 = self._uri.partition("/relationship")
		self._relative_uri = u0 + u1
		self._type = self._lookup('type')
		self._data = self._lookup('data')

	def __str__(self):
		"""
		Returns a human-readable string representation of this C{Relationship}
		object, e.g.:
		
			>>> print str(my_rel)
			'-[:KNOWS]->'
		
		"""
		return "-[:%s]->" % self._type

	def get_type(self):
		"""
		Returns the type of this C{Relationship}.
		
		@return: the type of this C{Relationship}
		
		"""
		return self._type

	def is_type(self, type):
		"""
		Returns C{True} if this C{Relationship} is of the given type.
		
		@return: C{True} if this C{Relationship} is of the given type
		
		"""
		return self._type == type

	def get_nodes(self):
		"""
		Returns a list of the two C{Node}s attached to this C{Relationship}.
		
		@return: list of the two C{Node}s attached to this C{Relationship}
		
		"""
		return [
			Node(self._lookup('start'), http=self._http),
			Node(self._lookup('end'), http=self._http)
		]

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

	def get_other_node(self, node):
		"""
		Returns a C{Node} object representing the node within this
		C{Relationship} which is not the one supplied.
		
		@param node: the C{Node} not required to be returned
		@return: the other C{Node} within this C{Relationship}
		
		"""
		return Node(
			self._lookup('start') if self._lookup('end') == node._uri else self._lookup('end'),
			http=self._http
		)


class Path(object):
	"""
	A string of C{Node}s connected by C{Relationships}. A list of C{Path}s
	may be obtained as the result of a traversal:
	
		>>> from py2neo import neo4j
		>>> gdb = neo4j.GraphDatabaseService("http://localhost:7474/db/data")
		>>> ref_node = gdb.get_reference_node()
		>>> traverser = ref_node.traverse(order="depth_first", max_depth=2)
		>>> for path in traverser.paths:
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

	@property
	def nodes(self):
		"""
		List of all the C{Node}s which make up this C{Path}.
		"""
		return self._nodes

	@property
	def relationships(self):
		"""
		List of all the C{Relationship}s which make up this C{Path}.
		"""
		return self._relationships

	@property
	def start_node(self):
		"""
		C{Node} object representing the first node within this C{Path}.
		"""
		return self._nodes[0]

	@property
	def end_node(self):
		"""
		C{Node} object representing the last node within this C{Path}.
		"""
		return self._nodes[-1]

	@property
	def last_relationship(self):
		"""
		C{Relationship} object representing the last relationship within this
		C{Path}.
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

	def __init__(self, T, uri=None, template_uri=None, http=None, user_name=None, password=None):
		rest.Resource.__init__(
			self,
			uri or template_uri.rpartition("/{key}/{value}")[0],
			http=http,
			user_name=user_name,
			password=password
		)
		self.__T = T
		self._base_uri, u0, u1 = self._uri.partition("/index")
		self._relative_uri = u0 + u1
		self._template_uri = template_uri or "%s%s{key}/{value}" % (
			uri,
			"" if uri.endswith("/") else "/"
		)

	def __repr__(self):
		return '%s<%s>(%s)' % (
			self.__class__.__name__,
			self.__T.__name__,
			repr(self._uri)
		)

	def add(self, indexable_resource, key, value):
		"""
		Adds an entry to this C{Index} under the specified C{key} and C{value}.
		
		@param indexable_resource: the resource to add to the C{Index}
		@param key: the key of the key-value pair under which to index this resource
		@param value: the value of the key-value pair under which to index this resource
		
		"""
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


class TraversalDescription(object):
	"""
	Describes a graph traversal.
	"""

	def __init__(self):
		self._description = {}

	def traverse(self, start_node):
		return Traverser(
			template_uri=start_node._lookup('traverse'),
			traversal_description=self._description,
			http=start_node._http
		)

	def order(self, selector):
		td = TraversalDescription()
		td._description = self._description
		td._description['order'] = selector
		return td

	def breadth_first(self):
		return self.order('breadth_first')

	def depth_first(self):
		return self.order('depth_first')

	def uniqueness(self, uniqueness):
		td = TraversalDescription()
		td._description = self._description
		td._description['uniqueness'] = uniqueness
		return td

	def relationships(self, type, direction=None):
		td = TraversalDescription()
		td._description = self._description
		if 'relationships' not in td._description:
			td._description['relationships'] = []
		if direction in ['in', 'incoming']:
			direction = 'in'
		elif direction in ['out', 'outgoing']:
			direction = 'out'
		elif direction:
			raise ValueError(direction)
		if direction:
			td._description['relationships'].append({
				'type': type,
				'direction': direction
			})
		else:
			td._description['relationships'].append({
				'type': type
			})
		return td

	def prune(self, language, body):
		td = TraversalDescription()
		td._description = self._description
		td._description['prune_evaluator'] = {
			'language': language,
			'body': body
		}
		return td

	def filter(self, language, name):
		td = TraversalDescription()
		td._description = self._description
		td._description['return_filter'] = {
			'language': language,
			'name': name
		}
		return td

	def max_depth(self, depth):
		td = TraversalDescription()
		td._description = self._description
		td._description['max_depth'] = depth
		return td


class Traverser(rest.Resource):
	"""
	An engine designed to traverse a U{Neo4j <http://neo4j.org/>} database
	starting at a specific C{Node}.
	"""

	class Order:

		BREADTH_FIRST = 'breadth_first'
		DEPTH_FIRST   = 'depth_first'

	def __init__(self, template_uri=None, traversal_description=None, http=None, user_name=None, password=None):
		rest.Resource.__init__(self, None, http=http, user_name=user_name, password=password)
		self._template_uri = template_uri
		self._traversal_description = traversal_description

	@property
	def paths(self):
		"""
		Returns all C{Path}s from this traversal.
		"""
		return [
			Path([
				Node(uri)
				for uri in path['nodes']
			], [
				Relationship(uri)
				for uri in path['relationships']
			])
			for path in self._post(
				self._template_uri.format(returnType='path'),
				self._traversal_description
			)
		]

	@property
	def nodes(self):
		"""
		Returns all C{Node}s from this traversal.
		"""
		return [
			Node(node['self'])
			for node in self._post(
				self._template_uri.format(returnType='node'),
				self._traversal_description
			)
		]

	@property
	def relationships(self):
		"""
		Returns all C{Relationship}s from this traversal.
		"""
		return [
			Relationship(relationship['self'])
			for relationship in self._post(
				self._template_uri.format(returnType='relationship'),
				self._traversal_description
			)
		]


class PersistentObject(object):
	"""
	A base object from which persistent objects may inherit. Unlike some
	persistence models, object attributes are updated "live" and therefore do
	not require an explicit I{save} or I{load}. Simple-typed attributes are
	mapped to C{Node} properties where possible and attributes pointing to
	other C{PersistentObject} instances are mapped to C{Relationship}s. No
	direct mapping to C{Relationship} properties exists within this framework.
	All attributes begining with an underscore character are I{not} mapped to
	the database and will be stored within the object instance as usual.
	
		>>> from py2neo import neo4j
		>>> gdb = neo4j.GraphDatabaseService("http://localhost:7474/db/data")
		>>> class Country(neo4j.PersistentObject):
		... 	def __init__(self, node, name, population):
		... 		neo4j.PersistentObject.__init__(self, node)
		... 		self.name = name
		... 		self.population = population
		... 		self.currency = None
		... 		self.continent = None
		...
		>>> uk = Country(gdb.create_node(), "United Kingdom", 62698362)
	
	"""
	
	__node__ = None
	"""
	Property holding the database C{Node} to which this instance is attached.
	"""
	
	def __init__(self, node):
		"""
		All subclass constructors should call this constructor and pass a valid
		C{Node} object to which this instance becomes attached.
		"""
		self.__node__ = node
	
	def __getattr__(self, name):
		"""
		Returns either a C{Node} property value with this name, a C{Node}
		connected by a C{Relationship} of type C{name} or C{None} if neither
		exist.
		"""
		if name.startswith("_"):
			return object.__getattr__(self, name)
		try:
			return self.__node__[name]
		except LookupError:
			pass
		except:
			raise AttributeError("Cannot access node")
		try:
			return self.__node__.get_single_related_node(Direction.OUTGOING, name)
		except LookupError:
			pass
		except:
			raise AttributeError("Cannot access node")
		return None

	def __setattr__(self, name, value):
		"""
		Creates or updates a C{Node} property with this name or a
		C{Relationship} of type C{name} if C{value} is another
		C{PersistentObject} instance.
		"""
		if name.startswith("_"):
			object.__setattr__(self, name, value)
		elif value is None:
			self.__delattr__(name)
		elif isinstance(value, PersistentObject):
			try:
				self.__node__.create_relationship_to(value.__node__, name)
			except:
				raise AttributeError("Cannot set relationship")
		else:
			try:
				self.__node__[name] = value
			except ValueError:
				raise AttributeError("Illegal property value: %s" % (repr(value)))
			except:
				raise AttributeError("Cannot set node property")

	def __delattr__(self, name):
		"""
		Removes the C{Node} property with this name and any C{Relationship}s of
		type C{name}. This is also equivalent to setting a C{Node} property
		to C{None}.
		"""
		if name.startswith("_"):
			object.__delattr__(self, name)
		else:
			try:
				del self.__node__[name]
			except LookupError:
				pass
			except:
				raise AttributeError("Cannot delete node property")
			try:
				rels = self.__node__.get_relationships(Direction.OUTGOING, name)
				for rel in rels:
					rel.delete()
			except:
				raise AttributeError("Cannot delete relationship")



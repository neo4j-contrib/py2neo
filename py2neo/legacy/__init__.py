#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright 2011-2014, Nigel Small
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


from __future__ import division, unicode_literals

from py2neo.packages.httpstream import (http,
                                  Resource as _Resource,
                                  ResourceTemplate as _ResourceTemplate,
                                  ClientError as _ClientError,
                                  ServerError as _ServerError)
from py2neo.packages.jsonstream import assembled, grouped
from py2neo.packages.httpstream.numbers import CREATED, NOT_FOUND, CONFLICT, BAD_REQUEST
from py2neo.packages.urimagic import percent_encode, URI, URITemplate

from py2neo.neo4j import Graph, Node, Relationship, Resource, Cacheable, \
    ResourceTemplate, _hydrated, BatchRequestList, Path, _cast, _rel, ReadBatch as _ReadBatch, WriteBatch as _WriteBatch
from py2neo.util import *


class IndexTypeError(TypeError):
    pass


class GraphDatabaseService(Graph):

    def __init__(self, uri=None):
        super(GraphDatabaseService, self).__init__(uri=uri)
        self._indexes = {Node: {}, Relationship: {}}

    @property
    def supports_index_uniqueness_modes(self):
        """ Indicates whether the server supports `get_or_create` and
        `create_or_fail` uniqueness modes on batched index methods.
        """
        return self.neo4j_version >= (1, 9)

    def _index_manager(self, content_type):
        """ Fetch the index management resource for the given `content_type`.

        :param content_type:
        :return:
        """
        if content_type is Node:
            uri = self.__metadata__["node_index"]
        elif content_type is Relationship:
            uri = self.__metadata__["relationship_index"]
        else:
            raise IndexTypeError(content_type.__class__.__name__)
        return Resource(uri)

    def get_indexes(self, content_type):
        """ Fetch a dictionary of all available indexes of a given type.

        :param content_type: either :py:class:`neo4j.Node` or
            :py:class:`neo4j.Relationship`
        :return: a list of :py:class:`Index` instances of the specified type
        """
        index_manager = self._index_manager(content_type)
        index_index = index_manager._get().content
        if index_index:
            self._indexes[content_type] = dict(
                (key, Index(content_type, value["template"]))
                for key, value in index_index.items()
            )
        else:
            self._indexes[content_type] = {}
        return self._indexes[content_type]

    def get_index(self, content_type, index_name):
        """ Fetch a specific index from the current database, returning an
        :py:class:`Index` instance. If an index with the supplied `name` and
        content `type` does not exist, :py:const:`None` is returned.

        :param content_type: either :py:class:`neo4j.Node` or
            :py:class:`neo4j.Relationship`
        :param index_name: the name of the required index
        :return: an :py:class:`Index` instance or :py:const:`None`

        .. seealso:: :py:func:`get_or_create_index`
        .. seealso:: :py:class:`Index`
        """
        if index_name not in self._indexes[content_type]:
            self.get_indexes(content_type)
        if index_name in self._indexes[content_type]:
            return self._indexes[content_type][index_name]
        else:
            return None

    def get_or_create_index(self, content_type, index_name, config=None):
        """ Fetch a specific index from the current database, returning an
        :py:class:`Index` instance. If an index with the supplied `name` and
        content `type` does not exist, one is created with either the
        default configuration or that supplied in `config`::

            # get or create a node index called "People"
            people = graph.get_or_create_index(neo4j.Node, "People")

            # get or create a relationship index called "Friends"
            friends = graph.get_or_create_index(neo4j.Relationship, "Friends")

        :param content_type: either :py:class:`neo4j.Node` or
            :py:class:`neo4j.Relationship`
        :param index_name: the name of the required index
        :return: an :py:class:`Index` instance

        .. seealso:: :py:func:`get_index`
        .. seealso:: :py:class:`Index`
        """
        index = self.get_index(content_type, index_name)
        if index:
            return index
        index_manager = self._index_manager(content_type)
        rs = index_manager._post({"name": index_name, "config": config or {}})
        index = Index(content_type, assembled(rs)["template"])
        self._indexes[content_type].update({index_name: index})
        return index

    def delete_index(self, content_type, index_name):
        """ Delete the entire index identified by the type and name supplied.

        :param content_type: either :py:class:`neo4j.Node` or
            :py:class:`neo4j.Relationship`
        :param index_name: the name of the index to delete
        :raise LookupError: if the specified index does not exist
        """
        if index_name not in self._indexes[content_type]:
            self.get_indexes(content_type)
        if index_name in self._indexes[content_type]:
            index = self._indexes[content_type][index_name]
            index._delete()
            del self._indexes[content_type][index_name]
        else:
            raise LookupError("Index not found")

    def get_indexed_node(self, index_name, key, value):
        """ Fetch the first node indexed with the specified details, returning
        :py:const:`None` if none found.

        :param index_name: the name of the required index
        :param key: the index key
        :param value: the index value
        :return: a :py:class:`Node` instance
        """
        index = self.get_index(Node, index_name)
        if index:
            nodes = index.get(key, value)
            if nodes:
                return nodes[0]
        return None

    def get_or_create_indexed_node(self, index_name, key, value, properties=None):
        """ Fetch the first node indexed with the specified details, creating
        and returning a new indexed node if none found.

        :param index_name: the name of the required index
        :param key: the index key
        :param value: the index value
        :param properties: properties for the new node, if one is created
            (optional)
        :return: a :py:class:`Node` instance
        """
        index = self.get_or_create_index(Node, index_name)
        return index.get_or_create(key, value, properties or {})

    def get_indexed_relationship(self, index_name, key, value):
        """ Fetch the first relationship indexed with the specified details,
        returning :py:const:`None` if none found.

        :param index_name: the name of the required index
        :param key: the index key
        :param value: the index value
        :return: a :py:class:`Relationship` instance
        """
        index = self.get_index(Relationship, index_name)
        if index:
            relationships = index.get(key, value)
            if relationships:
                return relationships[0]
        return None


class Index(Cacheable, Resource):
    """ Searchable database index which can contain either nodes or
    relationships.

    .. seealso:: :py:func:`Graph.get_or_create_index`
    """

    def __init__(self, content_type, uri, name=None):
        self._content_type = content_type
        key_value_pos = uri.find("/{key}/{value}")
        if key_value_pos >= 0:
            self._searcher = ResourceTemplate(uri)
            Resource.__init__(self, URI(uri[:key_value_pos]))
        else:
            Resource.__init__(self, uri)
            self._searcher = ResourceTemplate(uri.string + "/{key}/{value}")
        uri = URI(self)
        if self.graph.neo4j_version >= (1, 9):
            self._create_or_fail = Resource(uri.resolve("?uniqueness=create_or_fail"))
            self._get_or_create = Resource(uri.resolve("?uniqueness=get_or_create"))
        else:
            self._create_or_fail = None
            self._get_or_create = Resource(uri.resolve("?unique"))
        self._query_template = ResourceTemplate(uri.string + "{?query,order}")
        self._name = name or uri.path.segments[-1]
        self.__searcher_stem_cache = {}

    def __repr__(self):
        return "{0}({1}, {2})".format(
            self.__class__.__name__,
            self._content_type.__name__,
            repr(URI(self).string)
        )

    def _searcher_stem_for_key(self, key):
        if key not in self.__searcher_stem_cache:
            stem = self._searcher.uri_template.string.partition("{key}")[0]
            self.__searcher_stem_cache[key] = stem + percent_encode(key) + "/"
        return self.__searcher_stem_cache[key]

    def add(self, key, value, entity):
        """ Add an entity to this index under the `key`:`value` pair supplied::

            # create a node and obtain a reference to the "People" node index
            alice, = graph.create({"name": "Alice Smith"})
            people = graph.get_or_create_index(neo4j.Node, "People")

            # add the node to the index
            people.add("family_name", "Smith", alice)

        Note that while Neo4j indexes allow multiple entities to be added under
        a particular key:value, the same entity may only be represented once;
        this method is therefore idempotent.
        """
        self._post({
            "key": key,
            "value": value,
            "uri": str(URI(entity))
        })
        return entity

    def add_if_none(self, key, value, entity):
        """ Add an entity to this index under the `key`:`value` pair
        supplied if no entry already exists at that point::

            # obtain a reference to the "Rooms" node index and
            # add node `alice` to room 100 if empty
            rooms = graph.get_or_create_index(neo4j.Node, "Rooms")
            rooms.add_if_none("room", 100, alice)

        If added, this method returns the entity, otherwise :py:const:`None`
        is returned.
        """
        rs = self._get_or_create._post({
            "key": key,
            "value": value,
            "uri": str(URI(entity))
        })
        if rs.status_code == CREATED:
            return entity
        else:
            return None

    @property
    def content_type(self):
        """ Return the type of entity contained within this index. Will return
        either :py:class:`Node` or :py:class:`Relationship`.
        """
        return self._content_type

    @property
    def name(self):
        """ Return the name of this index.
        """
        return self._name

    def get(self, key, value):
        """ Fetch a list of all entities from the index which are associated
        with the `key`:`value` pair supplied::

            # obtain a reference to the "People" node index and
            # get all nodes where `family_name` equals "Smith"
            people = graph.get_or_create_index(neo4j.Node, "People")
            smiths = people.get("family_name", "Smith")

        ..
        """
        return [
            _hydrated(assembled(result))
            for i, result in grouped(self._searcher.expand(key=key, value=value)._get())
        ]

    def create(self, key, value, abstract):
        """ Create and index a new node or relationship using the abstract
        provided.
        """
        batch = WriteBatch(self.service_root.graph)
        if self._content_type is Node:
            batch.create(abstract)
            batch.add_indexed_node(self, key, value, 0)
        elif self._content_type is Relationship:
            batch.create(abstract)
            batch.add_indexed_relationship(self, key, value, 0)
        else:
            raise TypeError(self._content_type)
        entity, index_entry = batch.submit()
        return entity

    def _create_unique(self, key, value, abstract):
        """ Internal method to support `get_or_create` and `create_if_none`.
        """
        if self._content_type is Node:
            body = {
                "key": key,
                "value": value,
                "properties": abstract
            }
        elif self._content_type is Relationship:
            body = {
                "key": key,
                "value": value,
                "start": str(abstract[0].__uri__),
                "type": abstract[1],
                "end": str(abstract[2].__uri__),
                "properties": abstract[3] if len(abstract) > 3 else None
            }
        else:
            raise TypeError(self._content_type)
        return self._get_or_create._post(body)

    def get_or_create(self, key, value, abstract):
        """ Fetch a single entity from the index which is associated with the
        `key`:`value` pair supplied, creating a new entity with the supplied
        details if none exists::

            # obtain a reference to the "Contacts" node index and
            # ensure that Alice exists therein
            contacts = graph.get_or_create_index(neo4j.Node, "Contacts")
            alice = contacts.get_or_create("name", "SMITH, Alice", {
                "given_name": "Alice Jane", "family_name": "Smith",
                "phone": "01234 567 890", "mobile": "07890 123 456"
            })

            # obtain a reference to the "Friendships" relationship index and
            # ensure that Alice and Bob's friendship is registered (`alice`
            # and `bob` refer to existing nodes)
            friendships = graph.get_or_create_index(neo4j.Relationship, "Friendships")
            alice_and_bob = friendships.get_or_create(
                "friends", "Alice & Bob", (alice, "KNOWS", bob)
            )

        ..
        """
        return _hydrated(assembled(self._create_unique(key, value, abstract)))

    def create_if_none(self, key, value, abstract):
        """ Create a new entity with the specified details within the current
        index, under the `key`:`value` pair supplied, if no such entity already
        exists. If creation occurs, the new entity will be returned, otherwise
        :py:const:`None` will be returned::

            # obtain a reference to the "Contacts" node index and
            # create a node for Alice if one does not already exist
            contacts = graph.get_or_create_index(neo4j.Node, "Contacts")
            alice = contacts.create_if_none("name", "SMITH, Alice", {
                "given_name": "Alice Jane", "family_name": "Smith",
                "phone": "01234 567 890", "mobile": "07890 123 456"
            })

        ..
        """
        rs = self._create_unique(key, value, abstract)
        if rs.status_code == CREATED:
            return _hydrated(assembled(rs))
        else:
            return None

    def remove(self, key=None, value=None, entity=None):
        """ Remove any entries from the index which match the parameters
        supplied. The allowed parameter combinations are:

        `key`, `value`, `entity`
            remove a specific entity indexed under a given key-value pair

        `key`, `value`
            remove all entities indexed under a given key-value pair

        `key`, `entity`
            remove a specific entity indexed against a given key but with
            any value

        `entity`
            remove all occurrences of a specific entity regardless of
            key and value

        """
        if key and value and entity:
            t = ResourceTemplate(URI(self).string + "/{key}/{value}/{entity}")
            t.expand(key=key, value=value, entity=entity._id)._delete()
        elif key and value:
            uris = [
                URI(entity.__metadata__["indexed"])
                for entity in self.get(key, value)
            ]
            batch = WriteBatch(self.service_root.graph)
            for uri in uris:
                batch.append_delete(uri)
            batch.run()
        elif key and entity:
            t = ResourceTemplate(URI(self).string + "/{key}/{entity}")
            t.expand(key=key, entity=entity._id)._delete()
        elif entity:
            t = ResourceTemplate(URI(self).string + "/{entity}")
            t.expand(entity=entity._id)._delete()
        else:
            raise TypeError("Illegal parameter combination for index removal")

    def query(self, query):
        """ Query the index according to the supplied query criteria, returning
        a list of matched entities::

            # obtain a reference to the "People" node index and
            # get all nodes where `family_name` equals "Smith"
            people = graph.get_or_create_index(neo4j.Node, "People")
            s_people = people.query("family_name:S*")

        The query syntax used should be appropriate for the configuration of
        the index being queried. For indexes with default configuration, this
        should be Apache Lucene query syntax.
        """
        resource = self._query_template.expand(query=query)
        for i, result in grouped(resource._get()):
            yield _hydrated(assembled(result))

    def _query_with_score(self, query, order):
        resource = self._query_template.expand(query=query, order=order)
        for i, result in grouped(resource._get()):
            meta = assembled(result)
            yield _hydrated(meta), meta["score"]

    def query_by_index(self, query):
        return self._query_with_score(query, "index")

    def query_by_relevance(self, query):
        return self._query_with_score(query, "relevance")

    def query_by_score(self, query):
        return self._query_with_score(query, "score")


class ReadBatch(_ReadBatch):
    """ Generic batch execution facility for data read requests,
    """

    def get_indexed_nodes(self, index, key, value):
        """ Fetch all nodes indexed under a given key-value pair.

        :param index: index name or instance
        :type index: :py:class:`str` or :py:class:`Index`
        :param key: key under which nodes are indexed
        :type key: :py:class:`str`
        :param value: value under which nodes are indexed
        :return: batch request object
        """
        index = self._index(Node, index)
        uri = index._searcher_stem_for_key(key) + percent_encode(value)
        return self.append_get(uri)


class WriteBatch(_WriteBatch):
    """ Generic batch execution facility for data write requests. Most methods
    return a :py:class:`BatchRequest <py2neo.neo4j.BatchRequest>` object that
    can be used as a reference in other methods. See the
    :py:meth:`create <py2neo.neo4j.WriteBatch.create>` method for an example
    of this.
    """

    def __init__(self, graph):
        super(WriteBatch, self).__init__(graph)
        self.__new_uniqueness_modes = None

    @property
    def supports_index_uniqueness_modes(self):
        return self._graph.supports_index_uniqueness_modes

    def _assert_can_create_or_fail(self):
        if not self.supports_index_uniqueness_modes:
            raise NotImplementedError("Uniqueness mode `create_or_fail` "
                                      "requires version 1.9 or above")

    ### ADD TO INDEX ###

    def _add_to_index(self, cls, index, key, value, entity, query=None):
        uri = self._uri_for(self._index(cls, index), query=query)
        return self.append_post(uri, {
            "key": key,
            "value": value,
            "uri": self._uri_for(entity),
        })

    def add_to_index(self, cls, index, key, value, entity):
        """ Add an existing node or relationship to an index.

        :param cls: the type of indexed entity
        :type cls: :py:class:`Node <py2neo.neo4j.Node>` or
                   :py:class:`Relationship <py2neo.neo4j.Relationship>`
        :param index: index or index name
        :type index: :py:class:`Index <py2neo.neo4j.Index>` or :py:class:`str`
        :param key: index entry key
        :type key: :py:class:`str`
        :param value: index entry value
        :param entity: node or relationship to add to the index
        :type entity: concrete or reference
        :return: batch request object
        """
        return self._add_to_index(cls, index, key, value, entity)

    def add_to_index_or_fail(self, cls, index, key, value, entity):
        """ Add an existing node or relationship uniquely to an index, failing
        the entire batch if such an entry already exists.

        .. warning::
            Uniqueness modes for legacy indexes have been broken in recent
            server versions and therefore this method may not work as expected.

        :param cls: the type of indexed entity
        :type cls: :py:class:`Node <py2neo.neo4j.Node>` or
                   :py:class:`Relationship <py2neo.neo4j.Relationship>`
        :param index: index or index name
        :type index: :py:class:`Index <py2neo.neo4j.Index>` or :py:class:`str`
        :param key: index entry key
        :type key: :py:class:`str`
        :param value: index entry value
        :param entity: node or relationship to add to the index
        :type entity: concrete or reference
        :return: batch request object
        """
        self._assert_can_create_or_fail()
        query = "uniqueness=create_or_fail"
        return self._add_to_index(cls, index, key, value, entity, query)

    def get_or_add_to_index(self, cls, index, key, value, entity):
        """ Fetch a uniquely indexed node or relationship if one exists,
        otherwise add an existing entity to the index.

        :param cls: the type of indexed entity
        :type cls: :py:class:`Node <py2neo.neo4j.Node>` or
                   :py:class:`Relationship <py2neo.neo4j.Relationship>`
        :param index: index or index name
        :type index: :py:class:`Index <py2neo.neo4j.Index>` or :py:class:`str`
        :param key: index entry key
        :type key: :py:class:`str`
        :param value: index entry value
        :param entity: node or relationship to add to the index
        :type entity: concrete or reference
        :return: batch request object
        """
        if self.supports_index_uniqueness_modes:
            query = "uniqueness=get_or_create"
        else:
            query = "unique"
        return self._add_to_index(cls, index, key, value, entity, query)

    ### CREATE IN INDEX ###

    def _create_in_index(self, cls, index, key, value, abstract, query=None):
        uri = self._uri_for(self._index(cls, index), query=query)
        abstract = _cast(abstract, cls=cls, abstract=True)
        if cls is Node:
            return self.append_post(uri, {
                "key": key,
                "value": value,
                "properties": abstract.properties,
            })
        elif cls is Relationship:
            return self.append_post(uri, {
                "key": key,
                "value": value,
                "start": self._uri_for(abstract._start_node),
                "type": str(abstract._type),
                "end": self._uri_for(abstract._end_node),
                "properties": abstract._properties or {},
            })
        else:
            raise TypeError(cls)

    # Removed create_in_index as parameter combination not supported by server

    def create_in_index_or_fail(self, cls, index, key, value, abstract=None):
        """ Create a new node or relationship and add it uniquely to an index,
        failing the entire batch if such an entry already exists.

        .. warning::
            Uniqueness modes for legacy indexes have been broken in recent
            server versions and therefore this method may not work as expected.

        :param cls: the type of indexed entity
        :type cls: :py:class:`Node <py2neo.neo4j.Node>` or
                   :py:class:`Relationship <py2neo.neo4j.Relationship>`
        :param index: index or index name
        :type index: :py:class:`Index <py2neo.neo4j.Index>` or :py:class:`str`
        :param key: index entry key
        :type key: :py:class:`str`
        :param value: index entry value
        :param abstract: abstract node or relationship to create
        :return: batch request object
        """
        self._assert_can_create_or_fail()
        query = "uniqueness=create_or_fail"
        return self._create_in_index(cls, index, key, value, abstract, query)

    def get_or_create_in_index(self, cls, index, key, value, abstract=None):
        """ Fetch a uniquely indexed node or relationship if one exists,
        otherwise create a new entity and add that to the index.

        :param cls: the type of indexed entity
        :type cls: :py:class:`Node <py2neo.neo4j.Node>` or
                   :py:class:`Relationship <py2neo.neo4j.Relationship>`
        :param index: index or index name
        :type index: :py:class:`Index <py2neo.neo4j.Index>` or :py:class:`str`
        :param key: index entry key
        :type key: :py:class:`str`
        :param value: index entry value
        :param abstract: abstract node or relationship to create
        :return: batch request object
        """
        if self.supports_index_uniqueness_modes:
            query = "uniqueness=get_or_create"
        else:
            query = "unique"
        return self._create_in_index(cls, index, key, value, abstract, query)

    ### REMOVE FROM INDEX ###

    def remove_from_index(self, cls, index, key=None, value=None, entity=None):
        """ Remove any nodes or relationships from an index that match a
        particular set of criteria. Allowed parameter combinations are:

        `key`, `value`, `entity`
            remove a specific node or relationship indexed under a given
            key-value pair

        `key`, `entity`
            remove a specific node or relationship indexed against a given key
            and with any value

        `entity`
            remove all occurrences of a specific node or relationship
            regardless of key or value

        :param cls: the type of indexed entity
        :type cls: :py:class:`Node <py2neo.neo4j.Node>` or
                   :py:class:`Relationship <py2neo.neo4j.Relationship>`
        :param index: index or index name
        :type index: :py:class:`Index <py2neo.neo4j.Index>` or :py:class:`str`
        :param key: index entry key
        :type key: :py:class:`str`
        :param value: index entry value
        :param entity: node or relationship to remove from the index
        :type entity: concrete or reference
        :return: batch request object
        """
        index = self._index(cls, index)
        if key and value and entity:
            uri = self._uri_for(index, key, value, entity._id)
        elif key and entity:
            uri = self._uri_for(index, key, entity._id)
        elif entity:
            uri = self._uri_for(index, entity._id)
        else:
            raise TypeError("Illegal parameter combination for index removal")
        return self.append_delete(uri)

    ### START OF DEPRECATED METHODS ###

    @deprecated("WriteBatch.add_indexed_node is deprecated, "
                "use add_to_index instead")
    def add_indexed_node(self, index, key, value, node):
        return self.add_to_index(Node, index, key, value, node)

    @deprecated("WriteBatch.add_indexed_relationship is deprecated, "
                "use add_to_index instead")
    def add_indexed_relationship(self, index, key, value, relationship):
        return self.add_to_index(Relationship, index, key, value, relationship)

    @deprecated("WriteBatch.add_indexed_node_or_fail is deprecated, "
                "use add_to_index_or_fail instead")
    def add_indexed_node_or_fail(self, index, key, value, node):
        return self.add_to_index_or_fail(Node, index, key, value, node)

    @deprecated("WriteBatch.add_indexed_relationship_or_fail is deprecated, "
                "use add_to_index_or_fail instead")
    def add_indexed_relationship_or_fail(self, index, key, value, relationship):
        return self.add_to_index_or_fail(Relationship, index, key, value,
                                         relationship)

    @deprecated("WriteBatch.create_indexed_node_or_fail is deprecated, "
                "use create_in_index_or_fail instead")
    def create_indexed_node_or_fail(self, index, key, value, properties=None):
        self._assert_can_create_or_fail()
        abstract = properties or {}
        return self.create_in_index_or_fail(Node, index, key, value, abstract)

    @deprecated("WriteBatch.create_indexed_relationship_or_fail is deprecated, "
                "use create_in_index_or_fail instead")
    def create_indexed_relationship_or_fail(self, index, key, value,
                                            start_node, type_, end_node,
                                            properties=None):
        self._assert_can_create_or_fail()
        if properties:
            abstract = _rel(start_node, (type_, properties), end_node)
        else:
            abstract = _rel(start_node, type_, end_node)
        return self.create_in_index_or_fail(Relationship, index, key, value,
                                            abstract)

    @deprecated("WriteBatch.get_or_add_indexed_node is deprecated, "
                "use get_or_add_to_index instead")
    def get_or_add_indexed_node(self, index, key, value, node):
        self.get_or_add_to_index(Node, index, key, value, node)

    @deprecated("WriteBatch.get_or_add_indexed_relationship is deprecated, "
                "use get_or_add_to_index instead")
    def get_or_add_indexed_relationship(self, index, key, value, relationship):
        self.get_or_add_to_index(Relationship, index, key, value, relationship)

    @deprecated("WriteBatch.get_or_create_indexed_node is deprecated, "
                "use get_or_create_in_index instead")
    def get_or_create_indexed_node(self, index, key, value, properties=None):
        abstract = properties or {}
        return self.get_or_create_in_index(Node, index, key, value, abstract)

    @deprecated("WriteBatch.get_or_create_indexed_relationship is deprecated, "
                "use get_or_create_indexed instead")
    def get_or_create_indexed_relationship(self, index, key, value, start_node,
                                           type_, end_node, properties=None):
        if properties:
            abstract = _rel(start_node, (type_, properties), end_node)
        else:
            abstract = _rel(start_node, type_, end_node)
        return self.get_or_create_in_index(Relationship, index, key, value,
                                           abstract)

    @deprecated("WriteBatch.remove_indexed_node is deprecated, "
                "use remove_indexed instead")
    def remove_indexed_node(self, index, key=None, value=None, node=None):
        return self.remove_from_index(Node, index, key, value, node)

    @deprecated("WriteBatch.remove_indexed_relationship is deprecated, "
                "use remove_indexed instead")
    def remove_indexed_relationship(self, index, key=None, value=None,
                                    relationship=None):
        return self.remove_from_index(Relationship, index, key, value,
                                      relationship)

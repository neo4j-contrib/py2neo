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

""" The neo4j module provides the main `Neo4j <http://neo4j.org/>`_ client
functionality and will be the starting point for most applications. The main
classes provided are:

- :py:class:`Graph` - an instance of a Neo4j database server,
  providing a number of graph-global methods for handling nodes and
  relationships
- :py:class:`Node` - a representation of a database node
- :py:class:`Relationship` - a representation of a relationship between two
  database nodes
- :py:class:`Path` - a sequence of alternating nodes and relationships
- :py:class:`ReadBatch` - a batch of read requests to be carried out within a
  single transaction
- :py:class:`WriteBatch` - a batch of write requests to be carried out within
  a single transaction
"""


from __future__ import division, unicode_literals

import logging

from py2neo.error import ClientError, ServerError, ServerException
from py2neo.neo4j import CypherResults, Resource, Node, Relationship, _cast, NodePointer, Path
from py2neo.packages.jsonstream import assembled, grouped
from py2neo.packages.urimagic import percent_encode, URI
from py2neo.util import compact, deprecated, has_all, pendulate


batch_log = logging.getLogger(__name__ + ".batch")


class BatchError(Exception):

    @classmethod
    def with_name(cls, name):
        try:
            return type(name, (cls,), {})
        except TypeError:
            # for Python 2.x
            return type(str(name), (cls,), {})

    def __init__(self, response):
        self._response = response
        Exception.__init__(self, self.message)

    @property
    def message(self):
        return self._response.message

    @property
    def exception(self):
        return self._response.exception

    @property
    def full_name(self):
        return self._response.full_name

    @property
    def stack_trace(self):
        return self._response.stack_trace

    @property
    def cause(self):
        return self._response.cause

    @property
    def request(self):
        return self._response.request

    @property
    def response(self):
        return self._response


class BatchRequest(object):
    """ Individual batch request.
    """

    def __init__(self, method, uri, body=None):
        self._method = method
        self._uri = uri
        self._body = body

    def __eq__(self, other):
        return id(self) == id(other)

    def __ne__(self, other):
        return id(self) != id(other)

    def __hash__(self):
        return hash(id(self))

    @property
    def method(self):
        return self._method

    @property
    def uri(self):
        return self._uri

    @property
    def body(self):
        return self._body


class BatchResponse(object):
    """ Individual batch response.
    """

    @classmethod
    def hydrate(cls, data, body_hydrator=None):
        batch_id = data["id"]
        uri = data["from"]
        status_code = data.get("status")
        raw_body = data.get("body")
        if body_hydrator:
            body = body_hydrator(raw_body)
        else:
            body = raw_body
        location = data.get("location")
        if __debug__:
            batch_log.debug("<<< {{{0}}} {1} {2}".format(
                batch_id, status_code, raw_body, location))
        return cls(batch_id, uri, status_code, body, location)

    def __init__(self, batch_id, uri, status_code=None, body=None, location=None):
        self.batch_id = batch_id
        self.uri = URI(uri)
        self.status_code = status_code or 200
        self.body = body
        self.location = location


class BatchRequestList(object):

    def __init__(self, graph, hydrate=True):
        self.graph = graph
        # TODO: make function for subresource pattern below
        self._batch = Resource(graph.resource.metadata["batch"])
        self._cypher = Resource(graph.resource.metadata["cypher"])
        self.clear()
        self.hydrate = hydrate

    def __len__(self):
        return len(self._requests)

    def __nonzero__(self):
        return bool(self._requests)

    def append(self, request):
        self._requests.append(request)
        return request

    def append_get(self, uri):
        return self.append(BatchRequest("GET", uri))

    def append_put(self, uri, body=None):
        return self.append(BatchRequest("PUT", uri, body))

    def append_post(self, uri, body=None):
        return self.append(BatchRequest("POST", uri, body))

    def append_delete(self, uri):
        return self.append(BatchRequest("DELETE", uri))

    def append_cypher(self, query, params=None):
        """ Append a Cypher query to this batch. Resources returned from Cypher
        queries cannot be referenced by other batch requests.

        :param query: Cypher query
        :type query: :py:class:`str`
        :param params: query parameters
        :type params: :py:class:`dict`
        :return: batch request object
        :rtype: :py:class:`_Batch.Request`
        """
        if params:
            body = {"query": str(query), "params": dict(params)}
        else:
            body = {"query": str(query)}
        return self.append_post(self._uri_for(self._cypher), body)

    @property
    def _body(self):
        return [
            {
                "id": i,
                "method": request.method,
                "to": str(request.uri),
                "body": request.body,
            }
            for i, request in enumerate(self._requests)
        ]

    def clear(self):
        """ Clear all requests from this batch.
        """
        self._requests = []

    def find(self, request):
        """ Find the position of a request within this batch.
        """
        for i, req in pendulate(self._requests):
            if req == request:
                return i
        raise ValueError("Request not found")

    # TODO merge with Graph.relative_uri
    def _uri_for(self, resource, *segments, **kwargs):
        """ Return a relative URI in string format for the entity specified
        plus extra path segments.
        """
        if isinstance(resource, int):
            uri = "{{{0}}}".format(resource)
        elif isinstance(resource, NodePointer):
            uri = "{{{0}}}".format(resource.address)
        elif isinstance(resource, BatchRequest):
            uri = "{{{0}}}".format(self.find(resource))
        elif isinstance(resource, Node):
            # TODO: remove when Rel is also Bindable
            offset = len(resource.graph.resource.uri.string)
            uri = resource.resource.uri.string[offset:]
        else:
            offset = len(resource.service_root.graph.resource.uri)
            uri = str(resource.uri)[offset:]
        if segments:
            if not uri.endswith("/"):
                uri += "/"
            uri += "/".join(map(percent_encode, segments))
        query = kwargs.get("query")
        if query is not None:
            uri += "?" + query
        return uri

    def _execute(self):
        request_count = len(self)
        request_text = "request" if request_count == 1 else "requests"
        batch_log.info("Executing batch with {0} {1}".format(request_count, request_text))
        if __debug__:
            for id_, request in enumerate(self._requests):
                batch_log.debug(">>> {{{0}}} {1} {2} {3}".format(id_, request.method, request.uri, request.body))
        try:
            response = self._batch.post(self._body)
        except (ClientError, ServerError) as e:
            if e.exception:
                # A CustomBatchError is a dynamically created subclass of
                # BatchError with the same name as the underlying server
                # exception
                CustomBatchError = type(str(e.exception), (BatchError,), {})
                raise CustomBatchError(e)
            else:
                raise BatchError(e)
        else:
            return response

    def run(self):
        """ Execute the batch on the server and discard the results. If the
        batch results are not required, this will generally be the fastest
        execution method.
        """
        return self._execute().close()

    def stream(self):
        """ Execute the batch on the server and return iterable results. This
        method allows handling of results as they are received from the server.

        :return: iterable results
        :rtype: :py:class:`BatchResponseList`
        """
        response_list = BatchResponseList(self.graph, self._execute(), hydrate=self.hydrate)
        for response in response_list:
            yield response.body
        response_list.close()

    def submit(self):
        """ Execute the batch on the server and return a list of results. This
        method blocks until all results are received.

        :return: result records
        :rtype: :py:class:`list`
        """
        response_list = BatchResponseList(self.graph, self._execute(), hydrate=self.hydrate)
        return [response.body for response in response_list.responses]


class BatchResponseList(object):

    def __init__(self, graph, response, hydrate):
        self.__graph = graph
        self.__response = response
        if not hydrate:
            self.body_hydrator = None

    def __iter__(self):
        for i, response in grouped(self.__response):
            yield BatchResponse.hydrate(assembled(response), self.body_hydrator)
        self.close()

    @property
    def graph(self):
        return self.__graph

    @property
    def responses(self):
        return [
            BatchResponse.hydrate(response, self.body_hydrator)
            for response in self.__response.content
        ]

    @property
    def closed(self):
        return self.__response.closed

    def close(self):
        self.__response.close()

    def body_hydrator(self, data):
        if isinstance(data, dict) and has_all(data, ("columns", "data")):
            records = CypherResults._hydrated(self.__graph, data)
            if len(records) == 0:
                return None
            elif len(records) == 1:
                if len(records[0]) == 1:
                    return records[0][0]
                else:
                    return records[0]
            else:
                return records
        else:
            return self.__graph.hydrate(data)


# TODO: remove - redundant
class ReadBatch(BatchRequestList):
    """ Generic batch execution facility for data read requests,
    """

    def __init__(self, graph):
        BatchRequestList.__init__(self, graph)


class WriteBatch(BatchRequestList):
    """ Generic batch execution facility for data write requests. Most methods
    return a :py:class:`BatchRequest <py2neo.neo4j.BatchRequest>` object that
    can be used as a reference in other methods. See the
    :py:meth:`create <py2neo.neo4j.WriteBatch.create>` method for an example
    of this.
    """

    def __init__(self, graph):
        BatchRequestList.__init__(self, graph)

    def create(self, abstract):
        """ Create a node or relationship based on the abstract entity
        provided. For example::

            batch = WriteBatch(graph)
            a = batch.create(node(name="Alice"))
            b = batch.create(node(name="Bob"))
            batch.create(rel(a, "KNOWS", b))
            results = batch.submit()

        :param abstract: node or relationship
        :type abstract: abstract
        :return: batch request object
        """
        entity = _cast(abstract, abstract=True)
        if isinstance(entity, Node):
            uri = self._uri_for(Resource(self.graph.resource.metadata["node"]))
            body = entity.properties
        elif isinstance(entity, Relationship):
            uri = self._uri_for(entity.start_node, "relationships")
            body = {
                "type": entity.type,
                "to": self._uri_for(entity.end_node)
            }
            if entity.properties:
                body["data"] = entity.properties
        else:
            raise TypeError(entity)
        return self.append_post(uri, body)

    def create_path(self, node, *rels_and_nodes):
        """ Construct a path across a specified set of nodes and relationships.
        Nodes may be existing concrete node instances, abstract nodes or
        :py:const:`None` but references to other requests are not supported.

        :param node: start node
        :type node: concrete, abstract or :py:const:`None`
        :param rels_and_nodes: alternating relationships and nodes
        :type rels_and_nodes: concrete, abstract or :py:const:`None`
        :return: batch request object
        """
        query, params = Path(node, *rels_and_nodes)._create_query(unique=False)
        self.append_cypher(query, params)

    def get_or_create_path(self, node, *rels_and_nodes):
        """ Construct a unique path across a specified set of nodes and
        relationships, adding only parts that are missing. Nodes may be
        existing concrete node instances, abstract nodes or :py:const:`None`
        but references to other requests are not supported.

        :param node: start node
        :type node: concrete, abstract or :py:const:`None`
        :param rels_and_nodes: alternating relationships and nodes
        :type rels_and_nodes: concrete, abstract or :py:const:`None`
        :return: batch request object
        """
        query, params = Path(node, *rels_and_nodes)._create_query(unique=True)
        self.append_cypher(query, params)

    @deprecated("WriteBatch.get_or_create is deprecated, please use "
                "get_or_create_path instead")
    def get_or_create(self, rel_abstract):
        """ Use the abstract supplied to create a new relationship if one does
        not already exist.

        :param rel_abstract: relationship abstract to be fetched or created
        """
        rel = _cast(rel_abstract, cls=Relationship, abstract=True)
        if not (isinstance(rel.start_node, Node) or rel.start_node is None):
            raise TypeError("Relationship start node must be a "
                            "Node instance or None")
        if not (isinstance(rel.end_node, Node) or rel.end_node is None):
            raise TypeError("Relationship end node must be a "
                            "Node instance or None")
        if rel.start_node and rel.end_node:
            query = (
                "START a=node({A}), b=node({B}) "
                "CREATE UNIQUE (a)-[ab:`" + str(rel.type) + "` {P}]->(b) "
                "RETURN ab"
            )
        elif rel.start_node:
            query = (
                "START a=node({A}) "
                "CREATE UNIQUE (a)-[ab:`" + str(rel.type) + "` {P}]->() "
                "RETURN ab"
            )
        elif rel.end_node:
            query = (
                "START b=node({B}) "
                "CREATE UNIQUE ()-[ab:`" + str(rel.type) + "` {P}]->(b) "
                "RETURN ab"
            )
        else:
            raise ValueError("Either start node or end node must be "
                             "specified for a unique relationship")
        params = {"P": compact(rel._properties or {})}
        if rel.start_node:
            params["A"] = rel.start_node._id
        if rel.end_node:
            params["B"] = rel.end_node._id
        return self.append_cypher(query, params)

    def delete(self, entity):
        """ Delete a node or relationship from the graph.

        :param entity: node or relationship to delete
        :type entity: concrete or reference
        :return: batch request object
        """
        return self.append_delete(self._uri_for(entity))

    def set_property(self, entity, key, value):
        """ Set a single property on a node or relationship.

        :param entity: node or relationship on which to set property
        :type entity: concrete or reference
        :param key: property key
        :type key: :py:class:`str`
        :param value: property value
        :return: batch request object
        """
        if value is None:
            self.delete_property(entity, key)
        else:
            uri = self._uri_for(entity, "properties", key)
            return self.append_put(uri, value)

    def set_properties(self, entity, properties):
        """ Replace all properties on a node or relationship.

        :param entity: node or relationship on which to set properties
        :type entity: concrete or reference
        :param properties: properties
        :type properties: :py:class:`dict`
        :return: batch request object
        """
        uri = self._uri_for(entity, "properties")
        return self.append_put(uri, compact(properties))

    def delete_property(self, entity, key):
        """ Delete a single property from a node or relationship.

        :param entity: node or relationship from which to delete property
        :type entity: concrete or reference
        :param key: property key
        :type key: :py:class:`str`
        :return: batch request object
        """
        uri = self._uri_for(entity, "properties", key)
        return self.append_delete(uri)

    def delete_properties(self, entity):
        """ Delete all properties from a node or relationship.

        :param entity: node or relationship from which to delete properties
        :type entity: concrete or reference
        :return: batch request object
        """
        uri = self._uri_for(entity, "properties")
        return self.append_delete(uri)

    def add_labels(self, node, *labels):
        """ Add labels to a node.

        :param node: node to which to add labels
        :type entity: concrete or reference
        :param labels: text labels
        :type labels: :py:class:`str`
        :return: batch request object
        """
        uri = self._uri_for(node, "labels")
        return self.append_post(uri, list(labels))

    def remove_label(self, node, label):
        """ Remove a label from a node.

        :param node: node from which to remove labels (can be a reference to
            another request within the same batch)
        :param label: text label
        :type label: :py:class:`str`
        :return: batch request object
        """
        uri = self._uri_for(node, "labels", label)
        return self.append_delete(uri)

    def set_labels(self, node, *labels):
        """ Replace all labels on a node.

        :param node: node on which to replace labels (can be a reference to
            another request within the same batch)
        :param labels: text labels
        :type labels: :py:class:`str`
        :return: batch request object
        """
        uri = self._uri_for(node, "labels")
        return self.append_put(uri, list(labels))

    # TODO: PullBatch
    # TODO: PushBatch

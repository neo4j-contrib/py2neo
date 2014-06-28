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

import json
import logging

from py2neo.core import NodePointer, Bindable
from py2neo.cypher import CypherResults
from py2neo.error import GraphError
from py2neo.packages.jsonstream import assembled, grouped
from py2neo.packages.urimagic import percent_encode, URI
from py2neo.util import pendulate, ustr


log = logging.getLogger("py2neo.batch")


class BatchError(Exception):
    """ Wraps a base `GraphError` within a batch context.
    """

    def __init__(self, error):
        self.__cause__ = error


class BatchResource(Bindable):

    __instances = {}

    def __new__(cls, uri):
        try:
            inst = cls.__instances[uri]
        except KeyError:
            inst = super(BatchResource, cls).__new__(cls)
            inst.bind(uri)
            cls.__instances[uri] = inst
        return inst

    def post(self, batch):
        request_count = len(batch)
        request_text = "request" if request_count == 1 else "requests"
        log.info("Executing batch with %s %s", request_count, request_text)
        data = []
        for i, request in enumerate(batch):
            # TODO: take from repr
            log.info(">>> {{%s}} %s %s %s", i, request.method, request.uri, request.body)
            data.append(dict(request, id=i))
        return self.resource.post(data)

    def run(self, batch):
        self.post(batch).close()

    def stream(self, batch):
        response_list = self.post(batch)
        try:
            for i, rs in grouped(response_list):
                yield BatchResponse.hydrate(assembled(rs), batch)
        finally:
            response_list.close()

    def submit(self, batch):
        response_list = self.post(batch)
        try:
            return [BatchResponse.hydrate(rs, batch) for rs in response_list.content]
        finally:
            response_list.close()


class BatchRequest(object):
    """ Individual batch request.
    """

    def __init__(self, method, uri, body=None):
        self.method = method
        self.uri = ustr(uri)
        self.body = body

    def __repr__(self):
        parts = [self.method, self.uri]
        if self.body is not None:
            parts.append(json.dumps(self.body, separators=",:"))
        return " ".join(parts)

    def __eq__(self, other):
        return self is other

    def __ne__(self, other):
        return not self.__eq__(other)

    def __hash__(self):
        return hash(id(self))

    def __iter__(self):
        yield "method", self.method
        yield "to", self.uri
        if self.body is not None:
            yield "body", self.body


class BatchResponse(object):
    """ Individual batch response.
    """

    @classmethod
    def hydrate(cls, data, batch):
        request_id = data["id"]
        uri = data["from"]
        status_code = data.get("status")
        body = data.get("body")
        location = data.get("location")
        # TODO use instance repr (and move to caller)
        log.info("<<< {{{0}}} {1} {2}".format(
            request_id, status_code, body, location))
        return cls(batch, request_id, uri, body, location, status_code)

    # TODO: pass in batch-request-list instead of graph and map ids back to requests
    def __init__(self, batch, request_id, uri, content_data=None, location=None, status_code=None):
        self.batch = batch
        self.request_id = request_id
        self.uri = URI(uri)
        self.__content_data = content_data
        self.__content = NotImplemented
        self.location = URI(location)
        self.status_code = status_code or 200

    def __repr__(self):
        # TODO: fix
        return "{{{0}}} {1} {2}".format(self.request_id, self.status_code, self.content, self.location)

    @property
    def graph(self):
        return self.batch.graph

    @property
    def content_data(self):
        return self.__content_data

    @property
    def content(self):
        if self.__content is NotImplemented:
            try:
                self.__content = self.graph.hydrate(self.__content_data)
            except GraphError as error:
                # TODO: pass batch context to error constructor
                raise BatchError(error)
            else:
                # If Cypher results, reduce to single row or single value if possible
                if isinstance(self.__content, CypherResults):
                    num_rows = len(self.__content)
                    if num_rows == 0:
                        self.__content = None
                    elif num_rows == 1:
                        self.__content = self.__content[0]
                        num_columns = len(self.__content)
                        if num_columns == 1:
                            self.__content = self.__content[0]
        return self.__content


class Batch(object):

    def __init__(self, graph, hydrate=True):
        self.graph = graph
        self.clear()
        self.hydrate = hydrate

    def __len__(self):
        return len(self._requests)

    def __nonzero__(self):
        return bool(self._requests)

    def __iter__(self):
        return iter(self._requests)

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
        body = {"query": ustr(query)}
        if params:
            body["params"] = dict(params)
        return self.append_post(self.graph.cypher.relative_uri, body)

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
        else:
            uri = resource.relative_uri.string
        if segments:
            if not uri.endswith("/"):
                uri += "/"
            uri += "/".join(map(percent_encode, segments))
        query = kwargs.get("query")
        if query is not None:
            uri += "?" + query
        return uri

    def post(self):
        return self.graph.batch.post(self)

    def run(self):
        self.graph.batch.run(self)

    def stream(self):
        response_list = self.graph.batch.stream(self)
        if self.hydrate:
            for response in response_list:
                yield response.content
        else:
            for response in response_list:
                yield response.content_data

    def submit(self):
        response_list = self.graph.batch.submit(self)
        if self.hydrate:
            return [response.content for response in response_list]
        else:
            return [response.content_data for response in response_list]


#class CreateNodeBatchRequest(BatchRequest):
#    def __init__(self, properties=None):
#        BatchRequest.__init__(self, "POST", "node", properties or {})

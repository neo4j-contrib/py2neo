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
        num_jobs = len(batch)
        plural = "" if num_jobs == 1 else "s"
        log.info(">>> Sending batch request with %s job%s", num_jobs, plural)
        data = []
        for i, job in enumerate(batch):
            log.info(">>> {%s} %s", i, job)
            data.append(dict(job, id=i))
        response = self.resource.post(data)
        log.info("<<< Received batch response for %s job%s", num_jobs, plural)
        return response

    def run(self, batch):
        response = self.post(batch)
        log.info("<<< Discarding batch response")
        response.close()

    def stream(self, batch):
        response = self.post(batch)
        try:
            for i, job_data in grouped(response):
                result = JobResult.hydrate(assembled(job_data), batch)
                log.info("<<< %s", result)
                yield result
        finally:
            response.close()

    def submit(self, batch):
        response = self.post(batch)
        try:
            results = []
            for job_data in response.content:
                result = JobResult.hydrate(job_data, batch)
                log.info("<<< %s", result)
                results.append(result)
            return results
        finally:
            response.close()


class Job(object):
    """ Individual batch request.
    """

    # Indicates whether or not the result should be
    # interpreted as raw data.
    raw = False

    # TODO: tidy up
    @classmethod
    def uri_for(cls, entity, *segments, **kwargs):
        """ Return a relative URI in string format for the entity specified
        plus extra path segments.
        """
        if isinstance(entity, int):
            uri = "{{{0}}}".format(entity)
        elif isinstance(entity, NodePointer):
            uri = "{{{0}}}".format(entity.address)
        else:
            uri = entity.relative_uri.string
        if segments:
            if not uri.endswith("/"):
                uri += "/"
            uri += "/".join(map(percent_encode, segments))
        query = kwargs.get("query")
        if query is not None:
            uri += "?" + query
        return uri

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


class JobResult(object):
    """ Individual batch response.
    """

    @classmethod
    def hydrate(cls, data, batch):
        job_id = data["id"]
        uri = data["from"]
        status_code = data.get("status")
        body = data.get("body")
        location = data.get("location")
        return cls(batch, job_id, uri, body, location, status_code)

    def __init__(self, batch, job_id, uri, content_data=None, location=None, status_code=None):
        self.batch = batch
        self.job_id = job_id
        self.uri = URI(uri)
        self.__content_data = content_data
        self.__content = NotImplemented
        self.location = URI(location)
        self.status_code = status_code or 200

    def __repr__(self):
        parts = ["{" + ustr(self.job_id) + "}", ustr(self.status_code)]
        if self.content_data is not None:
            parts.append(json.dumps(self.content_data, separators=",:"))
        return " ".join(parts)

    @property
    def graph(self):
        return self.batch.graph

    @property
    def job(self):
        return self.batch[self.job_id]

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


class GetJob(Job):

    def __init__(self, uri):
        Job.__init__(self, "GET", uri)


class PutJob(Job):

    def __init__(self, uri, body=None):
        Job.__init__(self, "PUT", uri, body)


class PostJob(Job):

    def __init__(self, uri, body=None):
        Job.__init__(self, "POST", uri, body)


class DeleteJob(Job):

    def __init__(self, uri):
        Job.__init__(self, "DELETE", uri)


class CypherJob(PostJob):

    def __init__(self, query, params=None):
        body = {"query": ustr(query)}
        if params:
            body["params"] = dict(params)
        PostJob.__init__(self, "cypher", body)


class Batch(object):

    def __init__(self, graph, hydrate=True):
        self.graph = graph
        self.jobs = []
        self.hydrate = hydrate  # TODO remove (Job.raw)

    def __len__(self):
        return len(self.jobs)

    def __nonzero__(self):
        return bool(self.jobs)

    def __iter__(self):
        return iter(self.jobs)

    def resolve(self, node):
        """ Convert any references to previous jobs within the same batch
        into NodePointer objects.
        """
        if isinstance(node, Job):
            return NodePointer(self.find(node))
        else:
            return node

    def append(self, job):
        self.jobs.append(job)
        return job

    def append_get(self, uri):
        return self.append(Job("GET", uri))

    def append_put(self, uri, body=None):
        return self.append(Job("PUT", uri, body))

    def append_post(self, uri, body=None):
        return self.append(Job("POST", uri, body))

    def append_delete(self, uri):
        return self.append(Job("DELETE", uri))

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
        return self.append(CypherJob(query, params))

    def clear(self):
        """ Clear all jobs from this batch.
        """
        self.jobs = []

    def find(self, job):
        """ Find the position of a job within this batch.
        """
        for i, candidate_job in pendulate(self.jobs):
            if candidate_job == job:
                return i
        raise ValueError("Job not found in batch")

    # TODO: remove (moved to Job.uri_for)
    def _uri_for(self, resource, *segments, **kwargs):
        """ Return a relative URI in string format for the entity specified
        plus extra path segments.
        """
        if isinstance(resource, int):
            uri = "{{{0}}}".format(resource)
        elif isinstance(resource, NodePointer):
            uri = "{{{0}}}".format(resource.address)
        elif isinstance(resource, Job):
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
        # TODO: replace with `raw` detection from Job object
        if self.hydrate:
            for response in response_list:
                yield response.content
        else:
            for response in response_list:
                yield response.content_data

    def submit(self):
        response_list = self.graph.batch.submit(self)
        # TODO: replace with `raw` detection from Job object
        if self.hydrate:
            return [response.content for response in response_list]
        else:
            return [response.content_data for response in response_list]

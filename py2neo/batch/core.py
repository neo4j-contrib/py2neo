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

from py2neo.core import NodePointer, Service
from py2neo.cypher import RecordList
from py2neo.error.server import GraphError
from py2neo.packages.jsonstream import assembled, grouped
from py2neo.packages.httpstream.packages.urimagic import percent_encode, URI
from py2neo.util import pendulate, ustr, raise_from


__all__ = ["BatchError", "BatchResource", "Target", "Job", "JobResult", "CypherJob", "Batch"]


log = logging.getLogger("py2neo.batch")


class BatchError(GraphError):
    """ Wraps a base `GraphError` within a batch context.
    """

    batch = None
    job_id = None
    status_code = None
    uri = None
    location = None

    def __init__(self, message, batch, job_id, status_code, uri, location=None, **kwargs):
        GraphError.__init__(self, message, **kwargs)
        self.batch = batch
        self.job_id = job_id
        self.status_code = status_code
        self.uri = uri
        self.location = location


class BatchResource(Service):

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
        """ Post a batch to the server batch endpoint and return an
        HTTPStream response.
        """
        num_jobs = len(batch)
        plural = "" if num_jobs == 1 else "s"
        log.info("> Sending batch request with %s job%s", num_jobs, plural)
        data = []
        for i, job in enumerate(batch):
            log.info("> {%s} %s", i, job)
            data.append(dict(job, id=i))
        response = self.resource.post(data)
        log.info("< Received batch response for %s job%s", num_jobs, plural)
        return response

    def run(self, batch):
        response = self.post(batch)
        log.info("< Discarding batch response")
        response.close()

    def stream(self, batch):
        response = self.post(batch)
        try:
            for i, result_data in grouped(response):
                result = JobResult.hydrate(assembled(result_data), batch)
                log.info("< %s", result)
                yield result
        finally:
            response.close()

    def submit(self, batch):
        response = self.post(batch)
        try:
            results = []
            for result_data in response.content:
                result = JobResult.hydrate(result_data, batch)
                log.info("< %s", result)
                results.append(result)
            return results
        except ValueError:
            # Here, we're looking to gracefully handle a Neo4j server bug
            # whereby a response is received with no content and
            # 'Content-Type: application/json'. Given that correct JSON
            # technically needs to contain {} at minimum, the JSON
            # parser fails with a ValueError.
            if response.content_length == 0:
                from sys import exc_info
                from traceback import extract_tb
                type_, value, traceback = exc_info()
                for filename, line_number, function_name, text in extract_tb(traceback):
                    if "json" in filename and "decode" in function_name:
                        return []
            raise
        finally:
            response.close()


class Target(object):

    def __init__(self, entity, *offsets):
        self.entity = entity
        self.offsets = offsets

    @property
    def uri_string(self):
        if isinstance(self.entity, int):
            uri_string = "{{{0}}}".format(self.entity)
        elif isinstance(self.entity, NodePointer):
            uri_string = "{{{0}}}".format(self.entity.address)
        else:
            try:
                uri_string = self.entity.relative_uri.string
            except AttributeError:
                uri_string = ustr(self.entity)
        if self.offsets:
            if not uri_string.endswith("/"):
                uri_string += "/"
            uri_string += "/".join(map(percent_encode, self.offsets))
        return uri_string


class Job(object):
    """ Individual batch request.
    """

    # Indicates whether or not the result should be
    # interpreted as raw data.
    raw_result = False

    # TODO: comment
    def __init__(self, method, target, body=None):
        self.method = method
        self.target = target
        self.body = body

    def __repr__(self):
        parts = [self.method, self.target.uri_string]
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
        yield "to", self.target.uri_string
        if self.body is not None:
            yield "body", self.body


class JobResult(object):
    """ Individual batch response.
    """

    @classmethod
    def hydrate(cls, data, batch):
        graph = getattr(batch, "graph", None)
        job_id = data["id"]
        uri = data["from"]
        status_code = data.get("status")
        location = data.get("location")
        if graph is None or batch[job_id].raw_result:
            body = data.get("body")
        else:
            body = None
            try:
                body = graph.hydrate(data.get("body"))
            except GraphError as error:
                message = "Batch job %s failed with %s\n%s" % (
                    job_id, error.__class__.__name__, ustr(error))
                raise_from(BatchError(message, batch, job_id, status_code, uri, location), error)
            else:
                # If Cypher results, reduce to single row or single value if possible
                if isinstance(body, RecordList):
                    num_rows = len(body)
                    if num_rows == 0:
                        body = None
                    elif num_rows == 1:
                        body = body[0]
                        num_columns = len(body)
                        if num_columns == 1:
                            body = body[0]
        return cls(batch, job_id, uri, status_code, location, body)

    def __init__(self, batch, job_id, uri, status_code=None, location=None, content=None):
        self.batch = batch
        self.job_id = job_id
        self.uri = URI(uri)
        self.status_code = status_code or 200
        self.location = URI(location)
        self.content = content

    def __repr__(self):
        parts = ["{" + ustr(self.job_id) + "}", ustr(self.status_code)]
        if self.content is not None:
            parts.append(repr(self.content))
        return " ".join(parts)

    @property
    def graph(self):
        return self.batch.graph

    @property
    def job(self):
        return self.batch[self.job_id]


class CypherJob(Job):

    target = Target("cypher")

    def __init__(self, statement, parameters=None):
        body = {"query": ustr(statement)}
        if parameters:
            body["params"] = dict(parameters)
        Job.__init__(self, "POST", self.target, body)


# TODO: make single-use
class Batch(object):

    def __init__(self, graph):
        self.graph = graph
        self.jobs = []

    def __getitem__(self, index):
        return self.jobs[index]

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

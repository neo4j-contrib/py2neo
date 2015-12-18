#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright 2011-2015, Nigel Small
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


import json
import logging

from py2neo.compat import ustr, xstr, integer
from py2neo.core import Resource, Graph, Path, Node, Relationship, NodeProxy, graphy as core_cast
from py2neo.cypher import Cursor, cypher_request
from py2neo.status import GraphError, Finished
from py2neo.packages.httpstream.packages.urimagic import percent_encode, URI
from py2neo.util import raise_from


log = logging.getLogger("py2neo.ext.batch")


def cast(obj):
    if isinstance(obj, integer):
        obj = NodePointer(obj)
    elif isinstance(obj, tuple):
        obj = tuple(NodePointer(x) if isinstance(x, integer) else x for x in obj)
    return core_cast(obj)


def pendulate(collection):
    count = len(collection)
    for i in range(count):
        if i % 2 == 0:
            index = i // 2
        else:
            index = count - ((i + 1) // 2)
        yield index, collection[index]


class NodePointer(NodeProxy):
    """ Pointer to a :class:`Node` object. This can be used in a batch
    context to point to a node not yet created.
    """

    #: The address or index to which this pointer points.
    address = None

    def __init__(self, address):
        self.address = address

    def __repr__(self):
        return "<NodePointer address=%s>" % self.address

    def __str__(self):
        return xstr(self.__unicode__())

    def __unicode__(self):
        return "{%s}" % self.address

    def __eq__(self, other):
        return self.address == other.address

    def __ne__(self, other):
        return not self.__eq__(other)

    def __hash__(self):
        return hash(self.address)


def _create_query(p, unique=False):
    initial_match_clause = []
    path, values, params = [], [], {}

    def append_node(i, node):
        if node is None:
            path.append("(n{0})".format(i))
            values.append("n{0}".format(i))
        elif node.bound:
            path.append("(n{0})".format(i))
            initial_match_clause.append("MATCH (n{0}) WHERE id(n{0})={{i{0}}}".format(i))
            params["i{0}".format(i)] = node._id
            values.append("n{0}".format(i))
        else:
            path.append("(n{0} {{p{0}}})".format(i))
            params["p{0}".format(i)] = dict(node)
            values.append("n{0}".format(i))

    def append_relationship(i, relationship):
        if relationship:
            path.append("-[r{0}:`{1}` {{q{0}}}]->".format(i, relationship.type()))
            params["q{0}".format(i)] = dict(relationship)
            values.append("r{0}".format(i))
        else:
            path.append("-[r{0}:`{1}`]->".format(i, relationship.type()))
            values.append("r{0}".format(i))

    nodes = p.nodes()
    append_node(0, nodes[0])
    for i, relationship in enumerate(p.relationships()):
        append_relationship(i, relationship)
        append_node(i + 1, nodes[i + 1])
    clauses = list(initial_match_clause)
    if unique:
        clauses.append("CREATE UNIQUE p={0}".format("".join(path)))
    else:
        clauses.append("CREATE p={0}".format("".join(path)))
    clauses.append("RETURN p")
    query = "\n".join(clauses)
    return query, params


class BatchError(GraphError):
    """ Wraps a base :class:`py2neo.GraphError` within a batch context.
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


class BatchRunner(object):
    """ Resource for batch execution.
    """

    def __init__(self, uri):
        self.resource = Resource(uri)

    def post(self, batch):
        """ Post a batch of jobs to the server and receive a raw
        response.

        :arg batch: A :class:`.Batch` of jobs.
        :rtype: :class:`httpstream.Response`

        """
        num_jobs = len(batch)
        plural = "" if num_jobs == 1 else "s"
        log.info("> Sending batch request with %s job%s", num_jobs, plural)
        data = []
        for i, job in enumerate(batch):
            if job.finished:
                raise Finished(job)
            else:
                job.finished = True
            log.info("> {%s} %s", i, job)
            data.append(dict(job, id=i))
        response = self.resource.post(data)
        log.info("< Received batch response for %s job%s", num_jobs, plural)
        return response

    def run(self, batch):
        """ Execute a collection of jobs and return all results.

        :arg batch: A :class:`.Batch` of jobs.
        :rtype: :class:`list`

        """
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
    """ A callable target for a batch job. This may refer directly to a URI
    or to an object that can be resolved to a URI, such as a :class:`py2neo.Node`.
    """

    #: The entity behind this target.
    entity = None

    #: Additional path segments to append to the resolved URI.
    segments = None

    def __init__(self, entity, *segments):
        self.entity = entity
        self.segments = segments

    @property
    def uri_string(self):
        """ The fully resolved URI string for this target.

        :rtype: string

        """
        if isinstance(self.entity, int):
            uri_string = "{{{0}}}".format(self.entity)
        elif isinstance(self.entity, NodePointer):
            uri_string = "{{{0}}}".format(self.entity.address)
        else:
            try:
                uri_string = self.entity.ref
            except AttributeError:
                uri_string = ustr(self.entity)
        if self.segments:
            if not uri_string.endswith("/"):
                uri_string += "/"
            uri_string += "/".join(map(percent_encode, self.segments))
        return uri_string


class Job(NodeProxy):
    """ A single request for inclusion within a :class:`.Batch`.
    """

    #: The graph for which this job is intended (optional).
    graph = None

    #: Indicates whether or not the result should be
    #: interpreted as raw data.
    raw_result = False

    #: The HTTP method for the request.
    method = None

    #: A :class:`.Target` object used to determine the destination URI.
    target = None

    #: The request payload.
    body = None

    #: Indicates whether the job has been submitted.
    finished = False

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
    """ The result returned from the server for a single
    :class:`.Job` following a :class:`.Batch` submission.
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
                if isinstance(body, Cursor):
                    if body.move():
                        record = body.current()
                        width = len(record)
                        if width == 1:
                            body = record[0]
                        else:
                            body = record
                    else:
                        body = None
        return cls(batch, job_id, uri, status_code, location, body)

    #: The :class:`.Batch` from which this result was returned.
    batch = None

    #: The unique ID for this job within the batch.
    job_id = None

    #: The URI destination of the original job.
    uri = None

    #: The status code returned for this job.
    status_code = None

    #: The ``Location`` header returned for this job (if included).
    location = None

    #: The response content for this job.
    content = None

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
        """ The corresponding graph for this result.

        :rtype: :class:`py2neo.Graph`

        """
        return self.batch.graph

    @property
    def job(self):
        """ The original job behind this result.

        :rtype: :class:`.Job`

        """
        return self.batch[self.job_id]


class CypherJob(Job):
    """ A Cypher execution job for inclusion within a batch. Consider using
    `Cypher transactions <py2neo.cypher.Transaction>`_ instead of
    batched Cypher jobs.
    """

    target = Target("transaction/commit")

    def __init__(self, statement, parameters=None):
        Job.__init__(self, "POST", self.target,
                     {"statements": [cypher_request(statement, parameters)]})


class Batch(object):
    """ A collection of :class:`.Job` objects that can be submitted
    to a :class:`.BatchRunner`. References to previous jobs are only
    valid **within the same batch** and will not work across batches.
    """

    #: The graph with which this batch is associated
    graph = None

    def __init__(self, graph):
        self.graph = graph
        self.runner = BatchRunner(graph.resource.metadata["batch"])
        self.jobs = []

    def __getitem__(self, index):
        return self.jobs[index]

    def __len__(self):
        return len(self.jobs)

    def __bool__(self):
        return bool(self.jobs)

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
        """ Add a job to this batch.

        :param job: A :class:`.Job` object to add to this batch.
        :rtype: :class:`.Job`

        """
        self.jobs.append(job)
        return job

    def find(self, job):
        """ Find the position of a job within this batch.
        """
        for i, candidate_job in pendulate(self.jobs):
            if candidate_job == job:
                return i
        raise ValueError("Job not found in batch")


class PullPropertiesJob(Job):

    raw_result = True

    def __init__(self, entity):
        Job.__init__(self, "GET", Target(entity, "properties"))


class PullNodeLabelsJob(Job):

    raw_result = True

    def __init__(self, node):
        Job.__init__(self, "GET", Target(node, "labels"))


class PullRelationshipJob(Job):

    raw_result = True

    def __init__(self, relationship):
        Job.__init__(self, "GET", Target(relationship))


class PushPropertyJob(Job):

    def __init__(self, entity, key, value):
        Job.__init__(self, "PUT", Target(entity, "properties", key), value)


class PushPropertiesJob(Job):

    def __init__(self, entity, properties):
        Job.__init__(self, "PUT", Target(entity, "properties"), dict(properties))


class PushNodeLabelsJob(Job):

    def __init__(self, node, labels):
        Job.__init__(self, "PUT", Target(node, "labels"), set(labels))


class CreateNodeJob(Job):

    target = Target("node")

    def __init__(self, **properties):
        Job.__init__(self, "POST", self.target, properties)


class CreateRelationshipJob(Job):

    def __init__(self, start_node, type, end_node, **properties):
        body = {"type": type, "to": Target(end_node).uri_string}
        if properties:
            body["data"] = properties
        Job.__init__(self, "POST", Target(start_node, "relationships"), body)


class CreatePathJob(CypherJob):

    def __init__(self, *entities):
        # Fudge to allow graph to be passed in so Cypher syntax
        # detection can occur. Can be removed when only 2.0+ is
        # supported.
        if isinstance(entities[0], Graph):
            self.graph, entities = entities[0], entities[1:]
        path = Path(*(entity or {} for entity in entities))
        CypherJob.__init__(self, *_create_query(path))


class CreateUniquePathJob(CypherJob):

    def __init__(self, *entities):
        # Fudge to allow graph to be passed in so Cypher syntax
        # detection can occur. Can be removed when only 2.0+ is
        # supported.
        if isinstance(entities[0], Graph):
            self.graph, entities = entities[0], entities[1:]
        path = Path(*(entity or {} for entity in entities))
        CypherJob.__init__(self, *_create_query(path, unique=True))


class DeleteEntityJob(Job):

    def __init__(self, entity):
        Job.__init__(self, "DELETE", Target(entity))


class DeletePropertyJob(Job):

    def __init__(self, entity, key):
        Job.__init__(self, "DELETE", Target(entity, "properties", key))


class DeletePropertiesJob(Job):

    def __init__(self, entity):
        Job.__init__(self, "DELETE", Target(entity, "properties"))


class AddNodeLabelsJob(Job):

    def __init__(self, node, *labels):
        Job.__init__(self, "POST", Target(node, "labels"), set(labels))


class RemoveNodeLabelJob(Job):

    def __init__(self, entity, label):
        Job.__init__(self, "DELETE", Target(entity, "labels", label))


class ReadBatch(Batch):
    """ Generic batch execution facility for data read requests,
    """

    def __init__(self, graph):
        Batch.__init__(self, graph)

    def run(self):
        return [result.content for result in self.graph.batch.run(self)]


class WriteBatch(Batch):
    """ Generic batch execution facility for data write requests. Most methods
    return a :py:class:`BatchRequest <py2neo.neo4j.BatchRequest>` object that
    can be used as a reference in other methods. See the
    :py:meth:`create <py2neo.neo4j.WriteBatch.create>` method for an example
    of this.
    """

    def __init__(self, graph):
        Batch.__init__(self, graph)

    def run(self):
        return [result.content for result in self.runner.run(self)]

    def create(self, abstract):
        """ Create a node or relationship based on the abstract entity
        provided. For example::

            batch = WriteBatch(graph)
            a = batch.create(node(name="Alice"))
            b = batch.create(node(name="Bob"))
            batch.create(rel(a, "KNOWS", b))
            results = batch.run()

        :param abstract: node or relationship
        :type abstract: abstract
        :return: batch request object
        """
        entity = cast(abstract)
        if isinstance(entity, Node):
            return self.append(CreateNodeJob(**entity))
        elif isinstance(entity, Relationship):
            start_node = self.resolve(entity.start_node())
            end_node = self.resolve(entity.end_node())
            return self.append(CreateRelationshipJob(start_node, entity.type(), end_node, **entity))
        else:
            raise TypeError(entity)

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
        return self.append(CreatePathJob(self.graph, node, *rels_and_nodes))

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
        return self.append(CreateUniquePathJob(self.graph, node, *rels_and_nodes))

    def delete(self, entity):
        """ Delete a node or relationship from the graph.

        :param entity: node or relationship to delete
        :type entity: concrete or reference
        :return: batch request object
        """
        return self.append(DeleteEntityJob(self.resolve(entity)))

    def set_property(self, entity, key, value):
        """ Set a single property on a node or relationship.

        :param entity: node or relationship on which to set property
        :type entity: concrete or reference
        :param key: property key
        :type key: :py:class:`str`
        :param value: property value
        :return: batch request object
        """
        return self.append(PushPropertyJob(self.resolve(entity), key, value))

    def set_properties(self, entity, properties):
        """ Replace all properties on a node or relationship.

        :param entity: node or relationship on which to set properties
        :type entity: concrete or reference
        :param properties: properties
        :type properties: :py:class:`dict`
        :return: batch request object
        """
        return self.append(PushPropertiesJob(self.resolve(entity), properties))

    def delete_property(self, entity, key):
        """ Delete a single property from a node or relationship.

        :param entity: node or relationship from which to delete property
        :type entity: concrete or reference
        :param key: property key
        :type key: :py:class:`str`
        :return: batch request object
        """
        return self.append(DeletePropertyJob(self.resolve(entity), key))

    def delete_properties(self, entity):
        """ Delete all properties from a node or relationship.

        :param entity: node or relationship from which to delete properties
        :type entity: concrete or reference
        :return: batch request object
        """
        return self.append(DeletePropertiesJob(self.resolve(entity)))

    def add_labels(self, node, *labels):
        """ Add labels to a node.

        :param node: node to which to add labels
        :type entity: concrete or reference
        :param labels: text labels
        :type labels: :py:class:`str`
        :return: batch request object
        """
        return self.append(AddNodeLabelsJob(self.resolve(node), *labels))

    def remove_label(self, node, label):
        """ Remove a label from a node.

        :param node: node from which to remove labels (can be a reference to
            another request within the same batch)
        :param label: text label
        :type label: :py:class:`str`
        :return: batch request object
        """
        return self.append(RemoveNodeLabelJob(self.resolve(node), label))

    def set_labels(self, node, *labels):
        """ Replace all labels on a node.

        :param node: node on which to replace labels (can be a reference to
            another request within the same batch)
        :param labels: text labels
        :type labels: :py:class:`str`
        :return: batch request object
        """
        return self.append(PushNodeLabelsJob(self.resolve(node), labels))

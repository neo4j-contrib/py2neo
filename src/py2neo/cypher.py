#!/usr/bin/env python

# Copyright 2011 Nigel Small
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

"""
Cypher Query Language
"""

__author__    = "Nigel Small <py2neo@nigelsmall.org>"
__copyright__ = "Copyright 2011 Nigel Small"
__license__   = "Apache License, Version 2.0"


try:
    import json
except ImportError:
    import simplejson as json
try:
    from . import neo4j
except ImportError:
    import neo4j

class Query(object):
    """
    Represents a Cypher query which can be executed multiple times.
    """

    def __init__(self, graph_db, query):
        if not graph_db._cypher_uri:
            raise NotImplementedError("Cypher functionality not available")
        self.graph_db = graph_db
        self.query = query

    def execute(self, params=None, row_handler=None, metadata_handler=None, error_handler=None, **kwargs):
        if row_handler or metadata_handler:
            e = Query._Execution(self.graph_db, self.query, params, row_handler, metadata_handler, error_handler)
            return [], e.metadata
        else:
            if params:
                payload = {"query": str(self.query), "params": dict(params)}
            else:
                payload = {"query": str(self.query)}
            try:
                response = self.graph_db._post(self.graph_db._cypher_uri, payload, **kwargs)
                rows, columns = response['data'], response['columns']
                return [map(_resolve, row) for row in rows], Query.Metadata(columns)
            except ValueError as err:
                if error_handler:
                    try:
                        error_handler(json.loads(err.args[0].body)["message"])
                        return [], None
                    except Exception as ex:
                        raise ex


    class Metadata(object):
        """
        Metadata for query results.
        """

        #: List of column names
        columns = []

        def __init__(self, columns=None):
            self.columns = columns or []

    class _Execution(object):

        def __init__(self, graph_db, query, params=None, row_handler=None, metadata_handler=None, error_handler=None):

            self._body = []
            self._decoder = json.JSONDecoder()

            self._section = None
            self._depth = 0
            self._last_value = None
            self._metadata = Query.Metadata()
            self._row = None
            self._handler_error = None
            self._cypher_error = {}

            try:
                if params:
                    payload = {"query": str(query), "params": dict(params)}
                else:
                    payload = {"query": str(query)}
                self.row_handler = row_handler
                self.metadata_handler = metadata_handler
                self.error_handler = error_handler
                graph_db._post(
                    graph_db._cypher_uri,
                    payload,
                    headers={
                        "Accept": "application/json",
                        "Content-Type": "application/json"
                    },
                    streaming_callback=self.handle_chunk
                )
                self.handle_chunk(None, True)
            except Exception as ex:
                if self._handler_error:
                    raise self._handler_error
                elif not self.error_handler:
                    raise ex

        @property
        def metadata(self):
            return self._metadata

        def handle_chunk(self, data, finish=False):
            if data:
                self._body.extend(data.splitlines(True))
            while self._body:
                line = self._body.pop(0)
                if line.endswith("\n"):
                    self.handle_line(line)
                elif self._body:
                    self._body[0] = line + self._body[0]
                elif finish:
                    self.handle_line(line)
                else:
                    self._body.insert(0, line)
                    return

        def handle_line(self, line):
            while len(line) > 0:
                line = line.strip()
                if line:
                    if line[0] in ",:[]{}":
                        self.handle_token(line[0], None)
                        line = line[1:]
                    else:
                        value, pos = self._decoder.raw_decode(line)
                        self.handle_token(line[:pos], value)
                        line = line[pos:]

        def handle_token(self, src, value):
            if src in "]}":
                self._depth -= 1
            token = (self._depth, src)
            if token == (1, ":"):
                self._section = self._last_value
            elif token == (1, ","):
                self._section = None
            elif token == (0, "}"):
                self._section = None
            if self._section == "columns":
                if token == (1, "]"):
                    if self.metadata_handler:
                        try:
                            self.metadata_handler(self.metadata)
                        except Exception as ex:
                            self._handler_error = ex
                            raise ex
                if self._depth == 2 and value is not None:
                    self._metadata.columns.append(value)
            if self._section == "data":
                if token == (2, "["):
                    self._row = ""
                if token == (1, "]") or token == (2, ","):
                    if self.row_handler:
                        try:
                            self.row_handler(map(_resolve, json.loads(self._row)))
                        except Exception as ex:
                            self._handler_error = ex
                            raise ex
                    self._row = ""
                elif self._depth >= 2:
                    self._row += src
            if self._section == "message":
                if self._depth == 1 and value:
                    self._cypher_error["message"] = value
                    if self.error_handler:
                        try:
                            self.error_handler(self._cypher_error["message"])
                        except Exception as ex:
                            self._handler_error = ex
                            raise ex
            if src in "[{":
                self._depth += 1
            self._last_value = value


def _resolve(value):
    if isinstance(value, dict) and "self" in value:
        # is a neo4j resolvable entity
        uri = value["self"]
        if "type" in value:
            rel = neo4j.Relationship(uri)
            rel._index = value
            return rel
        else:
            node = neo4j.Node(uri)
            node._index = value
            return node
    else:
        # is a plain value
        return value

def execute(graph_db, query, params=None, row_handler=None, metadata_handler=None, **kwargs):
    """
    Execute a Cypher query against a database and return a tuple of rows and
    metadata. If handlers are supplied, an empty list of rows is returned
    instead, with each row being passed to the row_handler as it becomes
    available. Each row is passed as a list of values which may be either Nodes,
    Relationships or properties.

    :param graph_db: the graph database against which to execute this query
    :param query: the Cypher query to execute
    :param params: parameters to apply to the query provided
    :param row_handler: a handler function for each row returned
    :param metadata_handler: a handler function for returned metadata
    :param kwargs: extra parameters to forward to the underlying HTTP request;
        long-running queries may benefit from the request_timeout parameter in
        order to avoid timeout errors

    """
    #: allow first two arguments to be in either order, for backward
    #: compatibility
    if isinstance(query, neo4j.GraphDatabaseService):
        query, graph_db = graph_db, query
    return Query(graph_db, query).execute(
        params, row_handler=row_handler,
        metadata_handler=metadata_handler, **kwargs
    )

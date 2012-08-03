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

"""Cypher Query Language
"""


__author__    = "Nigel Small <py2neo@nigelsmall.org>"
__copyright__ = "Copyright 2011 Nigel Small"
__license__   = "Apache License, Version 2.0"


try:
    import simplejson as json
except ImportError:
    import json
import logging
import threading
try:
    from . import neo4j, rest
except ImportError:
    import neo4j, rest

logger = logging.getLogger(__name__)

_thread_local = threading.local()


DEFAULT_BLOCK_SIZE = 8192

def local_client():
    if not hasattr(_thread_local, "client"):
        _thread_local.client = CypherClient()
    return _thread_local.client

def _payload(query, params=None):
    if params:
        return {"query": str(query), "params": dict(params)}
    else:
        return {"query": str(query)}


class CypherError(ValueError):

    def __init__(self, message, exception, stacktrace):
        ValueError.__init__(self, message, exception, stacktrace)
        self.exception = exception
        self.stacktrace = stacktrace
        logger.error(self)


class CypherClient(rest.Client):

    def __init__(self):
        rest.Client.__init__(self)
        self.block_size = DEFAULT_BLOCK_SIZE

    def execute_query(self, method, uri, query="", params=None, handlers=None):
        data = _payload(query, params)
        if handlers:
            rs = self._send_request(method, uri, data)
            if rs.status in handlers:
                # asynchronous - use handler for each block received
                handler = handlers[rs.status]
                if self.block_size > 0:
                    while True:
                        block = rs.read(self.block_size)
                        if block:
                            handler(block)
                        else:
                            break
                else:
                    handler(rs.read())
                return rs.status, uri, rs.getheaders(), None
        else:
            return rest.Client.request(self, method, uri, data)


class Query(object):
    """A Cypher query which can be executed multiple times.
    """

    def __init__(self, graph_db, query):
        if not graph_db._cypher_uri:
            raise NotImplementedError("Cypher functionality not available")
        self.graph_db = graph_db
        self.query = query

    def execute(self, params=None, row_handler=None, metadata_handler=None, error_handler=None):
        logger.info((self.graph_db, self.query, params))
        if row_handler or metadata_handler:
            e = Query._Execution(self.graph_db, self.query, params,
                row_handler, metadata_handler, error_handler
            )
            return [], e.metadata
        else:
            try:
                response = self.graph_db._post(
                    self.graph_db._cypher_uri, _payload(self.query, params)
                )
            except rest.BadRequest as err:
                if error_handler:
                    try:
                        error_handler(
                            message=err.data["message"],
                            exception=err.data["exception"],
                            stacktrace=err.data["stacktrace"]
                        )
                        return [], None
                    except Exception as ex:
                        raise ex
                else:
                    raise CypherError(
                        err.data["message"],
                        exception=err.data["exception"],
                        stacktrace=err.data["stacktrace"]
                    )
            rows, columns = response['data'], response['columns']
            return [map(self.graph_db._resolve, row) for row in rows], Query.Metadata(columns)


    class Metadata(object):
        """Metadata for query results.
        """

        #: List of column names
        columns = []

        def __init__(self, columns=None):
            self.columns = columns or []

    class _Execution(object):

        def __init__(self, graph_db, query, params=None, row_handler=None, metadata_handler=None, error_handler=None):

            self._graph_db = graph_db
            self._data = ""
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
                    params = dict(params)
                else:
                    params = {}
                self.row_handler = row_handler
                self.metadata_handler = metadata_handler
                self.error_handler = error_handler
                local_client().execute_query("POST", graph_db._cypher_uri, str(query), params, handlers={
                    200: self.handle_block,
                    400: self.error_handler
                })
                if self._data:
                    raise ValueError("Unexpected data: " + self._data)
            except Exception as ex:
                if self._handler_error:
                    raise self._handler_error
                elif not self.error_handler:
                    raise ex

        @property
        def metadata(self):
            return self._metadata

        def handle_block(self, data):
            self._data += data
            while self._data:
                self._data = self._data.strip()
                if self._data:
                    if self._data[0] in ",:[]{}":
                        self.handle_token(self._data[0], None)
                        self._data = self._data[1:]
                    else:
                        try:
                            value, pos = self._decoder.raw_decode(self._data)
                            self.handle_token(self._data[:pos], value)
                            self._data = self._data[pos:]
                        except ValueError:
                            # need more data - wait for next block
                            break

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
                    if self._row and self.row_handler:
                        try:
                            self.row_handler(map(self._graph_db._resolve, json.loads(self._row)))
                        except Exception as ex:
                            self._handler_error = ex
                            raise ex
                    self._row = ""
                elif self._depth >= 2:
                    self._row += src
            if self._section == "message":
                if self._depth == 1 and value:
                    if self.error_handler:
                        self._cypher_error["message"] = value
                        try:
                            self.error_handler(self._cypher_error["message"])
                        except Exception as ex:
                            self._handler_error = ex
                            raise ex
                    else:
                        raise CypherError(value)
            if src in "[{":
                self._depth += 1
            self._last_value = value

def execute(graph_db, query, params=None, row_handler=None, metadata_handler=None, error_handler=None):
    """Execute a Cypher query against a database and return a tuple of rows and
    metadata. If handlers are supplied, an empty list of rows is returned
    instead, with each row being passed to the row_handler as it becomes
    available. Each row is passed as a list of values which may be either Nodes,
    Relationships or properties.

    :param graph_db: the graph database against which to execute this query
    :param query: the Cypher query to execute
    :param params: parameters to apply to the query provided
    :param row_handler: a handler function for each row returned
    :param metadata_handler: a handler function for returned metadata
    :param error_handler: a handler function for error conditions
    """
    return Query(graph_db, query).execute(
        params, row_handler=row_handler, metadata_handler=metadata_handler, error_handler=error_handler
    )

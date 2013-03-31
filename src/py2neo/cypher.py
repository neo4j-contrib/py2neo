#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright 2011-2013, Nigel Small
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

""" Cypher Query Language module.
"""

import json
import logging
import threading
import sys

from . import rest

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
        logger.debug("{0}: {1}\n{2}".format(exception, message, stacktrace))
        ValueError.__init__(self, message, exception, stacktrace)
        self.exception = exception
        self.stacktrace = stacktrace
        logger.error(self)


class CypherClient(rest.Client):

    def __init__(self):
        rest.Client.__init__(self)
        self.block_size = DEFAULT_BLOCK_SIZE

    def send(self, request, **kwargs):
        rs = self._send_request(request.method, request.uri, request.body)
        handlers = kwargs.get("handlers")
        if rs.status in handlers:
            # asynchronous - use handler for each block received
            handler = handlers[rs.status]
            if self.block_size > 0:
                data = []
                while True:
                    block = rs.read(self.block_size).decode("utf-8")
                    if block:
                        data.append(block)
                    else:
                        break
                    try:
                        utf8_data = "".join(data)
                        data = []
                        handler(utf8_data)
                    except UnicodeDecodeError:
                        continue
            else:
                handler(rs.read().decode("utf-8"))
        return rest.Response(
            request.graph_db, rs.status, request.uri,
            rs.getheader("Location", None)
        )


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
                rs = self.graph_db._send(
                    rest.Request(self.graph_db, "POST", self.graph_db._cypher_uri, _payload(self.query, params))
                )
            except rest.BadRequest as err:
                if error_handler:
                    try:
                        error_handler(
                            message=err.message,
                            exception=err.exception,
                            stacktrace=err.stacktrace,
                        )
                        return [], None
                    except Exception as ex:
                        raise ex
                else:
                    raise CypherError(
                        err.message,
                        exception=err.exception,
                        stacktrace=err.stacktrace,
                    )
            rows, columns = rs.body['data'], rs.body['columns']
            return [list(map(self.graph_db._resolve, row)) for row in rows], \
                   Query.Metadata(columns)


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
                local_client().send(
                    rest.Request(graph_db, "POST", graph_db._cypher_uri, _payload(query, params)),
                    handlers={
                        200: self.handle_block,
                        400: self.error_handler,
                    }
                )
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
                            self.row_handler(list(map(
                                self._graph_db._resolve,
                                json.loads(self._row)
                            )))
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


class ResultWriter(object):

    def __init__(self, out=None):
        self.out = out or sys.stdout

    @classmethod
    def _stringify(cls, value, quoted=False):
        if value is None:
            if quoted:
                return "null"
            else:
                return ""
        elif isinstance(value, list):
            out = " ".join(
                cls._stringify(item, quoted=False)
                for item in value
            )
            if quoted:
                out = json.dumps(out, separators=(',',':'))
        else:
            if quoted:
                try:
                    out = json.dumps(value)
                except TypeError:
                    out = json.dumps(str(value))
            else:
                out = str(value)
        return out

    @classmethod
    def _jsonify(cls, value):
        if isinstance(value, list):
            out = "[" + ", ".join(cls._jsonify(i) for i in value) + "]"
        elif hasattr(value, "__uri__") and hasattr(value, "_properties"):
            metadata = {
                "uri": str(value.__uri__),
                #"labels": list(value._labels),
                "properties": value._properties,
            }
            try:
                metadata.update({
                    "start": str(value.start_node.__uri__),
                    "type": str(value.type),
                    "end": str(value.end_node.__uri__),
                })
            except AttributeError:
                pass
            out = json.dumps(metadata)
        else:
            out = json.dumps(value)
        return out

    def write_delimited(self, data, metadata, **kwargs):
        field_delimiter = kwargs.get("field_delimiter", "\t")
        self.out.write(field_delimiter.join([
            json.dumps(column)
            for column in metadata.columns
        ]))
        self.out.write("\n")
        for row in data:
            self.out.write(field_delimiter.join([
                ResultWriter._stringify(value, quoted=True)
                for value in row
            ]))
            self.out.write("\n")

    def write_geoff(self, data, metadata, **kwargs):
        nodes = set()
        rels = set()
        def update_descriptors(value):
            if isinstance(value, list):
                for item in value:
                    update_descriptors(item)
            elif hasattr(value, "__uri__") and hasattr(value, "_properties"):
                if hasattr(value, "type"):
                    rels.add(value)
                else:
                    nodes.add(value)
        for row in data:
            for i in range(len(row)):
                update_descriptors(row[i])
        for node in sorted(nodes):
            self.out.write(str(node))
            self.out.write("\n")
        for rel in sorted(rels):
            self.out.write(str(rel))
            self.out.write("\n")

    def write_json(self, data, metadata, **kwargs):
        columns = [json.dumps(column) for column in metadata.columns]
        row_count = 0
        self.out.write("[")
        for row in data:
            row_count += 1
            if row_count > 1:
                self.out.write(", ")
            self.out.write("{" + ", ".join([
                columns[i] + ": " + ResultWriter._jsonify(row[i])
                for i in range(len(row))
            ]) + "}")
        self.out.write("]")

    def write_text(self, data, metadata, **kwargs):
        columns = metadata.columns
        column_widths = [len(column) for column in columns]
        data = [
            [
                ResultWriter._stringify(value)
                for value in row
            ]
            for row in data
        ]
        for row in data:
            column_widths = [
                max(column_widths[i], len(value))
                for i, value in enumerate(row)
            ]
        self.out.write("+-" + "---".join(
            "".ljust(column_widths[i], "-")
            for i, column in enumerate(columns)
        ) + "-+\n")
        self.out.write("| " + " | ".join(
            columns[i].ljust(column_widths[i])
            for i, column in enumerate(columns)
        ) + " |\n")
        self.out.write("+-" + "---".join(
            "".ljust(column_widths[i], "-")
            for i, column in enumerate(columns)
        ) + "-+\n")
        for row in data:
            self.out.write("| " + " | ".join([
                value.ljust(column_widths[i])
                for i, value in enumerate(row)
            ]) + " |\n")
        self.out.write("+-" + "---".join(
            "".ljust(column_widths[i], "-")
            for i, column in enumerate(columns)
        ) + "-+\n")

    formats = {
        "csv": (write_delimited, {"field_delimiter": ","}),
        "geoff": (write_geoff, {}),
        "json": (write_json, {}),
        "text": (write_text, {}),
        "tsv": (write_delimited, {"field_delimiter": "\t"}),
    }

    def write(self, format, data, metadata):
        try:
            method, kwargs = self.formats[format]
        except KeyError:
            raise ValueError("Unknown format {0}".format(repr(format)))
        if data or metadata:
            method(self, data, metadata, **kwargs)


def execute(graph_db, query, params=None, row_handler=None, metadata_handler=None, error_handler=None):
    """ Execute a Cypher query against a database and return a tuple of rows
    and metadata. If handlers are supplied, an empty list of rows is returned
    instead, with each row being passed to the row_handler as it becomes
    available. Each row is passed as a list of values which may be either Nodes,
    Relationships or properties.

    :param graph_db: the graph database against which to execute this query
    :param query: the Cypher query to execute
    :param params: parameters to apply to the query provided
    :param format:
    :param row_handler: a handler function for each row returned
    :param metadata_handler: a handler function for returned metadata
    :param error_handler: a handler function for error conditions
    """
    return Query(graph_db, query).execute(
        params, row_handler=row_handler, metadata_handler=metadata_handler, error_handler=error_handler
    )


def write(out, format, graph_db, query, params=None, error_handler=None):
    try:
        data, metadata = execute(graph_db, query, error_handler=error_handler)
        ResultWriter(out).write(format, data, metadata)
    except ValueError as err:
        if error_handler:
            error_handler(err.message)

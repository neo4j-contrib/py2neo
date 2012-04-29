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
Cypher utility module
"""

__author__    = "Nigel Small <py2neo@nigelsmall.org>"
__copyright__ = "Copyright 2011 Nigel Small"
__license__   = "Apache License, Version 2.0"


import argparse
try:
    import json
except ImportError:
    import simplejson as json
try:
    from . import neo4j
except ImportError:
    import neo4j
import sys

class Query(object):
    """
    Represents a Cypher query which can be executed multiple times.
    """

    def __init__(self, graph_db, query):
        if not graph_db._cypher_uri:
            raise NotImplementedError("Cypher functionality not available")
        self.graph_db = graph_db
        self.query = query

    def execute(self, params=None, row_handler=None, metadata_handler=None, **kwargs):
        if row_handler or metadata_handler:
            e = Query._Execution(self.graph_db, self.query, params, row_handler, metadata_handler)
            return [], e.metadata
        else:
            if params:
                payload = {"query": str(self.query), "params": dict(params)}
            else:
                payload = {"query": str(self.query)}
            response = self.graph_db._post(self.graph_db._cypher_uri, payload, **kwargs)
            rows, columns = response['data'], response['columns']
            return [map(_resolve, row) for row in rows], Query.Metadata(columns)

    class Metadata(object):
        """
        Metadata for query results.
        """

        #: List of column names
        columns = []

        def __init__(self, columns=None):
            self.columns = columns or []

    class _Execution(object):

        def __init__(self, graph_db, query, params=None, row_handler=None, metadata_handler=None):

            self._body = []
            self._decoder = json.JSONDecoder()

            self._section = None
            self._depth = 0
            self._last_value = None
            self._metadata = Query.Metadata()
            self._row = None
            self._error = None

            try:
                if params:
                    payload = {"query": str(query), "params": dict(params)}
                else:
                    payload = {"query": str(query)}
                self.metadata_handler = metadata_handler
                self.row_handler = row_handler
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
                if self._error:
                    raise self._error
                else:
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
                            self._error = ex
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
                            self._error = ex
                            raise ex
                    self._row = ""
                elif self._depth >= 2:
                    self._row += src
            if src in "[{":
                self._depth += 1
            self._last_value = value


def _stringify(value, quoted=False, with_properties=False):
    if isinstance(value, neo4j.Node):
        out = str(value)
        if quoted:
            out = '"' + out + '"'
        if with_properties:
            out += " " + json.dumps(value._lookup('data'), separators=(',',':'))
    elif isinstance(value, neo4j.Relationship):
        out = str(value.get_start_node()) + str(value) + str(value.get_end_node())
        if quoted:
            out = '"' + out + '"'
        if with_properties:
            out += " " + json.dumps(value._lookup('data'), separators=(',',':'))
    else:
        if quoted:
            out = json.dumps(value)
        else:
            out = str(value)
    return out

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

def execute_and_output_as_delimited(graph_db, query, field_delimiter="\t", out=None):
    out = out or sys.stdout
    data, metadata = execute(graph_db, query)
    out.write(field_delimiter.join([
        json.dumps(column)
        for column in metadata.columns
    ]))
    out.write("\n")
    for row in data:
        out.write(field_delimiter.join([
            _stringify(value, quoted=True)
            for value in row
        ]))
        out.write("\n")

def execute_and_output_as_json(graph_db, query, out=None):
    out = out or sys.stdout
    data, metadata = execute(graph_db, query)
    columns = [json.dumps(column) for column in metadata.columns]
    row_count = 0
    out.write("[\n")
    for row in data:
        row_count += 1
        if row_count > 1:
            out.write(",\n")
        out.write("\t{" + ", ".join([
            columns[i] + ": " + _stringify(row[i], quoted=True)
            for i in range(len(row))
        ]) + "}")
    out.write("\n]\n")

def execute_and_output_as_geoff(graph_db, query, out=None):
    out = out or sys.stdout
    nodes = {}
    relationships = {}
    def update_descriptors(value):
        if isinstance(value, neo4j.Node):
            nodes[str(value)] = value._lookup('data')
        elif isinstance(value, neo4j.Relationship):
            relationships[str(value.get_start_node()) + str(value) + str(value.get_end_node())] = value._lookup('data')
        else:
            # property - not supported in GEOFF format, so ignore
            pass
    data, columns = execute(graph_db, query)
    for row in data:
        for i in range(len(row)):
            update_descriptors(row[i])
    for key, value in nodes.items():
        out.write("{0}\t{1}\n".format(
            key,
            json.dumps(value)
        ))
    for key, value in relationships.items():
        out.write("{0}\t{1}\n".format(
            key,
            json.dumps(value)
        ))

def execute_and_output_as_text(graph_db, query, out=None):
    out = out or sys.stdout
    data, metadata = execute(graph_db, query)
    columns = metadata.columns
    column_widths = [len(column) for column in columns]
    for row in data:
        column_widths = [
            max(column_widths[i], None if row[i] is None else len(_stringify(row[i], with_properties=True)))
            for i in range(len(row))
        ]
    out.write("+-" + "---".join([
        "".ljust(column_widths[i], "-")
        for i in range(len(columns))
    ]) + "-+\n")
    out.write("| " + " | ".join([
        columns[i].ljust(column_widths[i])
        for i in range(len(columns))
    ]) + " |\n")
    out.write("+-" + "---".join([
        "".ljust(column_widths[i], "-")
        for i in range(len(columns))
    ]) + "-+\n")
    for row in data:
        out.write("| " + " | ".join([
            _stringify(row[i], with_properties=True).ljust(column_widths[i])
            for i in range(len(row))
        ]) + " |\n")
    out.write("+-" + "---".join([
        "".ljust(column_widths[i], "-")
        for i in range(len(columns))
    ]) + "-+\n")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Execute Cypher queries against a Neo4j database server and output the results.")
    parser.add_argument("-u", metavar="DATABASE_URI", default=None, help="the URI of the source Neo4j database server")
    parser.add_argument("-d", action="store_true", default=False, help="output all values in delimited format")
    parser.add_argument("-g", action="store_true", default=False, help="output nodes and relationships in GEOFF format")
    parser.add_argument("-j", action="store_true", default=False, help="output all values as a single JSON array")
    parser.add_argument("-t", action="store_true", default=True, help="output all results in a plain text table (default)")
    parser.add_argument("query", help="the Cypher query to execute")
    args = parser.parse_args()
    try:
        graph_db = neo4j.GraphDatabaseService(args.u or "http://localhost:7474/db/data/")
        if args.g:
            execute_and_output_as_geoff(graph_db, args.query)
        elif args.j:
            execute_and_output_as_json(graph_db, args.query)
        elif args.t:
            execute_and_output_as_text(graph_db, args.query)
        else:
            execute_and_output_as_delimited(graph_db, args.query)
    except SystemError as err:
        content = err.args[0]['content']
        if 'exception' in content and 'stacktrace' in content:
            sys.stderr.write("{0}\n".format(content['exception']))
            stacktrace = content['stacktrace']
            for frame in stacktrace:
                sys.stderr.write("\tat {0}\n".format(frame))
        else:
            sys.stderr.write("{0}\n".format(content))


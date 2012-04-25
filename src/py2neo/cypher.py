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

    def __init__(self, query, graph_db):
        if not graph_db._cypher_uri:
            raise NotImplementedError("Cypher functionality not available")
        self.query = str(query)
        self.graph_db = graph_db

    def execute(self, row_handler=None, metadata_handler=None):
        if row_handler or metadata_handler:
            e = Query._Execution(self.query, self.graph_db, row_handler, metadata_handler)
            return [], e.metadata
        else:
            response = self.graph_db._post(self.graph_db._cypher_uri, {'query': self.query})
            rows, columns = response['data'], response['columns']
            return [map(_resolve, row) for row in rows], Query.Metadata(columns)

    class Metadata(object):

        def __init__(self, columns=None):
            self.columns = columns or []

    class _Execution(object):

        def __init__(self, query, graph_db, row_handler=None, metadata_handler=None):

            self._body = []
            self._decoder = json.JSONDecoder()

            self._section = None
            self._depth = 0
            self._last_value = None
            self._metadata = Query.Metadata()
            self._row = None

            self.metadata_handler = metadata_handler
            self.row_handler = row_handler
            graph_db._post(
                graph_db._cypher_uri,
                {'query': str(query)},
                headers={
                    "Accept": "application/json",
                    "Content-Type": "application/json"
                },
                streaming_callback=self.handle_chunk
            )
            self.handle_chunk(None, True)

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
                        self.metadata_handler(self.metadata)
                if self._depth == 2 and value is not None:
                    self._metadata.columns.append(value)
            if self._section == "data":
                if token == (2, "["):
                    self._row = ""
                if token == (1, "]") or token == (2, ","):
                    if self.row_handler:
                        self.row_handler(map(_resolve, json.loads(self._row)))
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
    if isinstance(value, dict):
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
        return value

def execute(query, graph_db, row_handler=None, metadata_handler=None):
    """
    Execute a Cypher query against a database and return a tuple of rows and
    metadata. If handlers are supplied, an empty list of rows are returned,
    each row instead being passed to the row_handler as it becomes available.
    Each row is returned as a list of values, each value may be either a Node,
    a Relationship or a property.
    """
    return Query(query, graph_db).execute(
        row_handler=row_handler, metadata_handler=metadata_handler
    )

def execute_and_output_as_delimited(query, graph_db, field_delimiter="\t", out=None):
    out = out or sys.stdout
    data, metadata = execute(query, graph_db)
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

def execute_and_output_as_json(query, graph_db, out=None):
    out = out or sys.stdout
    data, metadata = execute(query, graph_db)
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

def execute_and_output_as_geoff(query, graph_db, out=None):
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
    data, columns = execute(query, graph_db)
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

def execute_and_output_as_text(query, graph_db, out=None):
    out = out or sys.stdout
    data, metadata = execute(query, graph_db)
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
            execute_and_output_as_geoff(args.query, graph_db)
        elif args.j:
            execute_and_output_as_json(args.query, graph_db)
        elif args.t:
            execute_and_output_as_text(args.query, graph_db)
        else:
            execute_and_output_as_delimited(args.query, graph_db)
    except SystemError as err:
        content = err.args[0]['content']
        if 'exception' in content and 'stacktrace' in content:
            sys.stderr.write("{0}\n".format(content['exception']))
            stacktrace = content['stacktrace']
            for frame in stacktrace:
                sys.stderr.write("\tat {0}\n".format(frame))
        else:
            sys.stderr.write("{0}\n".format(content))


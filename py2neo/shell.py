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


import codecs
import json
import locale
import os
import sys

from py2neo import __version__
from py2neo import Graph, GraphError, Node
from py2neo.util import ustr


# TODO: use ! prefix for meta-commands and everything else is cypher


# TODO: !ITERATE «format» «parameter-file» «cypher»
# TODO: !ITERATE CSV people.csv "CREATE (p:Person {name:{N}}) RETURN p"
SHELL_HELP = """\
The Neotool Shell allows you to run Cypher queries from an interactive prompt.
Queries may be entered directly or run from files using the EXECUTE command. To
quit the shell, press Ctrl+D or use the EXIT command.

Commands available:

    ADD PARAM[ETER]S <json-file>
    CLEAR
    EXECUTE <cypher-file>
    EXIT
    HELP
    LOOKUP
    REMOVE PARAM[ETER]S
    SHOW PARAM[ETER]S
    VERSION

If a numeric value is entered, details of the node with that ID are displayed.
Any other statements are executed as Cypher.
"""

USAGE = """\
Usage:
    {0}
"""


if sys.version_info >= (3,):
    def get_input(prompt):
        return input(prompt)
else:
    _stdin = sys.stdin
    preferred_encoding = locale.getpreferredencoding()
    sys.stdin = codecs.getreader(preferred_encoding)(sys.stdin)
    sys.stdout = codecs.getwriter(preferred_encoding)(sys.stdout)
    sys.stderr = codecs.getwriter(preferred_encoding)(sys.stderr)
    def get_input(prompt):
        sys.stdin = _stdin
        return raw_input(prompt).decode(sys.stdin.encoding)


class CommandLine(object):

    def __init__(self, text):
        self.text = text.strip()

    def __bool__(self):
        return bool(self.text)

    def __nonzero__(self):
        return bool(self.text)

    def peek(self):
        bits = self.text.split(None, 1)
        if not bits:
            return None
        return bits[0]

    def pop(self):
        bits = self.text.split(None, 1)
        if not bits:
            return None
        try:
            self.text = bits[1]
        except IndexError:
            self.text = ""
        return bits[0]


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
                out = json.dumps(out, separators=(',', ':'), ensure_ascii=False)
        else:
            if quoted:
                try:
                    out = json.dumps(value, ensure_ascii=False)
                except TypeError:
                    out = json.dumps(ustr(value), ensure_ascii=False)
            else:
                out = ustr(value)
        return out

    @classmethod
    def _jsonify(cls, value):
        if isinstance(value, list):
            out = "[" + ", ".join(cls._jsonify(i) for i in value) + "]"
        elif hasattr(value, "uri") and hasattr(value, "_properties"):
            metadata = {
                "uri": ustr(value.uri),
                "properties": value._properties,
            }
            try:
                metadata.update({
                    "start": ustr(value.start_node.uri),
                    "type": ustr(value.type),
                    "end": ustr(value.end_node.uri),
                })
            except AttributeError:
                pass
            out = json.dumps(metadata, ensure_ascii=False)
        else:
            out = json.dumps(value, ensure_ascii=False)
        return out

    def write_delimited(self, record_set, **kwargs):
        field_delimiter = kwargs.get("field_delimiter", "\t")
        self.out.write(field_delimiter.join([
            json.dumps(column, ensure_ascii=False)
            for column in record_set.columns
        ]))
        self.out.write("\n")
        for row in record_set:
            self.out.write(field_delimiter.join([
                ResultWriter._stringify(value, quoted=True)
                for value in row
            ]))
            self.out.write("\n")

    def write_geoff(self, record_set):
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

        for row in record_set:
            for i in range(len(row)):
                update_descriptors(row[i])
        for node in sorted(nodes, key=lambda x: x._id):
            self.out.write(ustr(node))
            self.out.write("\n")
        for rel in sorted(rels, key=lambda x: x._id):
            self.out.write(ustr(rel))
            self.out.write("\n")

    def write_json(self, record_set):
        columns = [json.dumps(column, ensure_ascii=False) for column in record_set.columns]
        row_count = 0
        self.out.write("[")
        for row in record_set:
            row_count += 1
            if row_count > 1:
                self.out.write(", ")
            self.out.write("{" + ", ".join([
                columns[i] + ": " + ResultWriter._jsonify(row[i])
                for i in range(len(row))
            ]) + "}")
        self.out.write("]")

    def write_text(self, record_set):
        columns = record_set.columns
        column_widths = [len(column) for column in columns]
        data = [
            [
                ResultWriter._stringify(value)
                for value in row
            ]
            for row in record_set
        ]
        for row in data:
            column_widths = [
                max(column_widths[i], len(value))
                for i, value in enumerate(row)
            ]
        self.out.write(" " + " | ".join(
            columns[i].ljust(column_widths[i])
            for i, column in enumerate(columns)
        ) + " \n")
        self.out.write("-" + "-+-".join(
            "".ljust(column_widths[i], "-")
            for i, column in enumerate(columns)
        ) + "-\n")
        for row in data:
            self.out.write(" " + " | ".join([
                value.ljust(column_widths[i])
                for i, value in enumerate(row)
            ]) + " \n")
        if len(data) == 1:
            self.out.write("(1 row)\n\n")
        else:
            self.out.write("({0} rows)\n\n".format(len(data)))

    formats = {
        "csv": (write_delimited, {"field_delimiter": ","}),
        "geoff": (write_geoff, {}),
        "json": (write_json, {}),
        "text": (write_text, {}),
        "tsv": (write_delimited, {"field_delimiter": "\t"}),
    }

    def write(self, format_, record_set):
        try:
            method, kwargs = self.formats[format_]
        except KeyError:
            raise ValueError("Unknown format {0}".format(repr(format_)))
        if kwargs:
            method(self, record_set, **kwargs)
        else:
            method(self, record_set)


class Shell(object):

    def __init__(self, graph):
        self.graph = graph
        self.lang = "cypher"
        self.format = "text"
        self.param_sets = []

    @property
    def prompt(self):
        if self.param_sets:
            return "\x1b[32;1m{0}/{1}\x1b[36;1m[{2}]\x1b[32;1m>\x1b[0m ".format(self.graph.uri.host_port, self.lang, len(self.param_sets))
        else:
            return "\x1b[32;1m{0}/{1}>\x1b[0m ".format(self.graph.uri.host_port, self.lang)

    def repl(self):
        print("neosh (py2neo/{0} Python/{1}.{2}.{3}-{4}-{5})".format(__version__, *sys.version_info))
        print("Copyright 2013-2014, Nigel Small")
        print("")
        try:
            while True:
                line = get_input(self.prompt)
                self.execute(line)
        except EOFError:
            print("⌁")
        except StopIteration:
            pass

    def _pop_command_and_argument(self, line):
        return line.pop(), line.pop()

    def execute(self, line):
        line = CommandLine(line)
        if not line:
            return
        command = line.peek().upper()
        if all(ch.isdigit() for ch in command):
            done = False
            while not done:
                word = line.pop()
                if word:
                    try:
                        node_id = int(word)
                    except ValueError:
                        pass
                    else:
                        self.display_node_by_id(node_id)
                else:
                    done = True
        elif command == "HELP":
            self.help(line)
        elif command == "EXECUTE":
            self.execute_cypher_from_file(line)
        elif command == "EXIT":
            raise StopIteration()
        elif command == "ADD":
            self.add_something(line)
        elif command == "CLEAR":
            if get_input("Are you sure you want to clear everything "
                         "from the database [y/N]? ").upper().startswith("Y"):
                print("Clearing all nodes and relationships")
                self.graph.delete_all()
            else:
                print("Clear aborted")
        elif command == "REMOVE":
            self.remove_something(line)
        elif command == "SHOW":
            self.show_something(line)
        elif command == "VERSION":
            self.show_neo4j_version(line)
        elif command == "LOOKUP":
            self.lookup(line)
        elif self.param_sets:
            self.execute_cypher(line.text, self.param_sets)
        else:
            self.execute_cypher(line.text, {})

    def help(self, line):
        sys.stdout.write(SHELL_HELP)

    def execute_cypher(self, query, params):
        if isinstance(params, list):
            for p in params:
                self.execute_cypher(query, p)
        else:
            if not isinstance(params, dict):
                params = {}
            try:
                record_set = self.graph.cypher.execute(query, params)
            except GraphError as error:
                print("\x1b[31;1m{0}: {1}\x1b[0m".format(error.__class__.__name__, error))
                print("")
            else:
                writer = ResultWriter(sys.stdout)
                writer.write(self.format, record_set)

    def display_node_by_id(self, node_id):
        query = "START n=node({i}) RETURN n"
        params = {"i": node_id}
        try:
            record_set = self.graph.cypher.execute(query, params)
        except GraphError as error:
            print("\x1b[31;1m{0}: {1}\x1b[0m".format(error.__class__.__name__, error))
            print("")
        else:
            self.display_node(record_set[0][0])

    def display_node(self, n):
        title = "Node {0}".format(n._id)
        print(title)
        print("=" * len(title))
        if self.graph.supports_node_labels:
            print("Labels: " + ", ".join(n.labels))
        print("Properties:")
        properties = n.properties
        if properties:
            max_key_len = max(len(key) for key in properties.keys())
            for key, value in sorted(properties.items()):
                print("  {0} : {1}".format(key.ljust(max_key_len), json.dumps(value)))
        print("Relationships:")
        for r in n.match():
            print("  {0}".format(r))
        print("")

    def lookup(self, line):
        command = line.pop()
        index_name = line.pop()
        if index_name:
            index = self.graph.get_index(Node, index_name)
            if not index:
                print("\x1b[31;1mNode index {0} not found\x1b[0m".format(repr(index_name)))
                print("")
                return
        else:
            print("Usage: LOOKUP <index-name> <key> <value>")
            return
        key = line.pop()
        value = line.pop()
        nodes = index.get(key, value)
        if nodes:
            for n in nodes:
                self.display_node(n)
        else:
            print("No nodes found\n")

    def execute_cypher_from_file(self, line):
        command, file_name = self._pop_command_and_argument(line)
        if file_name:
            file_name = os.path.expanduser(file_name)
        else:
            print("Usage: EXECUTE <cypher-file>")
            return
        try:
            with codecs.open(file_name, encoding="utf-8") as f:
                query = f.read()
        except IOError as err:
            sys.stderr.write("{0}: {1}".format(err.__class__.__name__, err))
            sys.stderr.write("\n")
        else:
            if self.param_sets:
                self.execute_cypher(query, self.param_sets)
            else:
                self.execute_cypher(query, {})

    def show_neo4j_version(self, line):
        print("Neo4j " + self.graph.__metadata__["neo4j_version"])

    def add_something(self, line):
        command, subject = self._pop_command_and_argument(line)
        if not subject:
            print("Usage: ADD PARAMS <json-file>\n"
                  "       ADD PARAMETERS <json-file>")
            return
        if subject.upper() in ("PARAMS", "PARAMETERS"):
            self.add_parameters_from_file(line)
        else:
            sys.stderr.write("Bad command\n")

    def remove_something(self, line):
        command, subject = self._pop_command_and_argument(line)
        if not subject:
            print("Usage: REMOVE PARAMS\n"
                  "       REMOVE PARAMETERS")
            return
        if subject.upper() in ("PARAMS", "PARAMETERS"):
            self.remove_parameters(line)
        else:
            sys.stderr.write("Bad command\n")

    def show_something(self, line):
        command, subject = self._pop_command_and_argument(line)
        if not subject:
            print("Usage: SHOW PARAMS\n"
                  "       SHOW PARAMETERS")
            return
        if subject.upper() in ("PARAMS", "PARAMETERS"):
            self.show_parameters(line)
        else:
            sys.stderr.write("Bad command\n")

    def add_parameters_from_file(self, line):

        file_name = os.path.expanduser(line.pop())
        try:
            params = json.load(codecs.open(file_name, encoding="utf-8"))
        except IOError as err:
            sys.stderr.write("{0}: {1}".format(err.__class__.__name__, err))
            sys.stderr.write("\n")
        else:
            if isinstance(params, list):
                count = len(params)
                self.param_sets.extend(params)
            elif isinstance(params, dict):
                count = 1
                self.param_sets.append(params)
            else:
                count = 0
            if count == 1:
                print("1 parameter set added")
            else:
                print("{0} parameter sets added".format(count))

    def remove_parameters(self, line):
        self.param_sets = []

    def show_parameters(self, line):
        print(json.dumps(self.param_sets, sort_keys=True, indent=4))


def _help(script):
    print(USAGE.format(os.path.basename(script)))


def main():
    script, args = sys.argv[0], sys.argv[1:]
    try:
        # TODO command line args
        # TODO neosh -x "CREATE (n) RETURN n"
        uri = "http://localhost:7474/db/data/"
        shell = Shell(Graph(uri))
        shell.repl()
    except Exception as err:
        sys.stderr.write(ustr(err))
        sys.stderr.write("\n")
        _help(script)
        sys.exit(1)


if __name__ == "__main__":
    main()

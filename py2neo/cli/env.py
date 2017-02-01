#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright 2011-2017, Nigel Small
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


from collections import deque, OrderedDict
from getpass import getpass
from json import dumps as json_dumps
from readline import set_completer

from py2neo import GraphService, Unauthorized, Forbidden
from py2neo.cli.console import Table
from py2neo.cypher import cypher_keywords, cypher_str, cypher_repr
from py2neo.types import Subgraph, Node, Walkable, remote, walk


class SimpleCompleter(object):

    node_labels = None
    relationship_types = None
    matches = None

    def __init__(self, graph_service, options):
        self.graph = graph_service.graph
        self.keywords = sorted(options)

    def complete(self, text, state):
        if state == 0:
            # This is the first time for this text, so build a match list.
            if text:
                self.node_labels = self.graph.node_labels
                self.relationship_types = self.graph.relationship_types
                dictionary = self.node_labels | self.relationship_types | set(self.keywords)
                upper_text = text.upper()
                self.matches = [word for word in dictionary if word and word.upper().startswith(upper_text)]
            else:
                self.matches = self.keywords[:]

        # Return the state'th item from the match list,
        # if we have that many.
        try:
            response = self.matches[state] + " "
        except IndexError:
            response = None
        return response


class Environment(object):

    services = None
    graph_service = None
    user = "neo4j"
    transaction = None
    run = None
    parameter_sets = None
    output_format = None

    def __init__(self, console, interactive=False):
        self.console = console
        self.interactive = interactive
        self.services = {}
        self.parameter_sets = deque()

    @property
    def command_prefix(self):
        return "/" if self.interactive else "--"

    def print_usage_overview(self, commands):
        write = self.console.write_help
        write("The py2neo console accepts both raw Cypher and slash commands and supports\n"
              "basic auto-completion.\n")
        write("Commands:")
        for command in sorted(commands, key=lambda x: x.name):
            write("  %s%s %s" % (self.command_prefix, command.name, command.summary()))

    def show_server_details(self):
        graph_service = self.graph_service
        table = Table(self.console, header_columns=1)
        table.append(("HTTP URI", graph_service.address.http_uri))
        table.append(("Bolt URI", graph_service.address.bolt_uri))
        table.append(("User", self.user))
        table.append(("Kernel Version", ".".join(map(str, graph_service.kernel_version))))
        table.append(("Store Directory", graph_service.store_directory))
        table.append(("Store ID", graph_service.store_id))
        for store, size in graph_service.store_file_sizes.items():
            table.append((store, size))
        table.write()

    def show_config(self, search_terms):
        graph_service = self.graph_service
        table = Table(self.console, header_columns=1)
        for name, value in sorted(graph_service.config.items()):
            if not search_terms or all(term in name for term in search_terms):
                table.append((name, value))
        table.write()

    def connect(self, uri, user=None, password=None):
        if user:
            self.user = user
        else:
            user = self.user
        while True:
            graph_service = GraphService(uri, auth=(user, password))
            uri = graph_service.address.uri["/"]
            try:
                _ = graph_service.kernel_version
            except Unauthorized:
                password = getpass("Enter password for user %s: " % user)
            except Forbidden:
                if graph_service.password_change_required():
                    new_password = getpass("Password expired. Enter new password for user %s: " % user)
                    password = graph_service.change_password(new_password)
                else:
                    raise
            else:
                self.services[uri] = graph_service
                break
        self.graph_service = self.services[uri]
        self.run = self.graph_service.graph.run
        set_completer(SimpleCompleter(self.graph_service, cypher_keywords).complete)

    def assert_connected(self):
        if not self.graph_service:
            raise AssertionError("Not connected")

    def assert_in_transaction(self):
        if not self.transaction:
            raise AssertionError("Not in a transaction")

    def assert_not_in_transaction(self):
        if self.transaction:
            raise AssertionError("Already in a transaction")

    def run_cypher(self, statement):
        self.assert_connected()
        try:
            parameters = self.parameter_sets.popleft()
        except IndexError:
            parameters = {}
        return self.run(statement, parameters)

    def dump(self, cursor):
        if self.output_format == "json":
            self.dump_json(cursor)
        elif self.output_format == "csv":
            self.dump_separated_values(cursor, separator=u",", header=0)
        elif self.output_format == "csv-with-header":
            self.dump_separated_values(cursor, separator=u",", header=1)
        elif self.output_format == "tsv":
            self.dump_separated_values(cursor, separator=u"\t", header=0)
        elif self.output_format == "tsv-with-header":
            self.dump_separated_values(cursor, separator=u"\t", header=1)
        else:
            self.dump_human(cursor)

    def dump_human(self, cursor):
        table = Table(self.console, str_function=cypher_str, header_rows=1, auto_align=True)
        table.append(cursor.keys())
        num_records = 0
        for record in cursor:
            table.append(record)
            num_records += 1
        table.write()
        footer = u"(%d record%s)" % (num_records, u"" if num_records == 1 else u"s")
        self.console.write_metadata(footer)

    def dump_json(self, cursor):

        def json_value(obj):
            if isinstance(obj, Node):
                return dict(obj)
            if isinstance(obj, Walkable):
                w = []
                for i, entity in enumerate(walk(obj)):
                    if i % 2 == 0:
                        w.append(json_value(entity))
                    else:
                        w.append({entity.type(): dict(entity)})
                return w
            return obj

        for record in cursor:
            data = OrderedDict()
            for key, value in record.items():
                if isinstance(value, Subgraph):
                    value = json_value(value)
                data[key] = value
            dumped = json_dumps(data, ensure_ascii=False)
            if isinstance(dumped, bytes):
                dumped = dumped.decode("utf-8")
            self.console.write(dumped)

    def dump_separated_values(self, cursor, separator, header):
        if header:
            self.console.write_metadata(separator.join(cypher_repr(key, quote=u'"') for key in cursor.keys()))
        for record in cursor:
            self.console.write(separator.join(cypher_repr(value, quote=u'"') for value in record.values()))

    def has_transaction(self):
        return bool(self.transaction)

    def begin_transaction(self):
        self.assert_connected()
        self.assert_not_in_transaction()
        self.transaction = self.graph_service.graph.begin()
        self.run = self.transaction.run

    def commit_transaction(self):
        self.assert_connected()
        self.assert_in_transaction()
        try:
            self.transaction.commit()
        finally:
            self.run = self.graph_service.graph.run
            self.transaction = None

    def rollback_transaction(self):
        self.assert_connected()
        self.assert_in_transaction()
        try:
            self.transaction.rollback()
        finally:
            self.run = self.graph_service.graph.run
            self.transaction = None

    def list_parameter_sets(self):
        for data in self.parameter_sets:
            self.console.write(cypher_str(data))
        num_sets = len(self.parameter_sets)
        self.console.write_metadata("({} parameter set{})".format(num_sets, "" if num_sets == 1 else "s"))

    def append_parameter_set(self, **parameters):
        self.parameter_sets.append(parameters)

    def clear_parameter_sets(self):
        self.parameter_sets.clear()

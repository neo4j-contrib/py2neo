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


from collections import deque
from getpass import getpass
from readline import set_completer

from py2neo import GraphService, Unauthorized, Forbidden, cypher_keywords


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
        for command_str, command_class in sorted(commands.items()):
            write("  %s%s %s" % (self.command_prefix, " ".join(command_str), command_class.summary()))

    def list_connections(self):
        for name, service in sorted(self.services.items()):
            if service is self.graph_service:
                if self.console.colour:
                    from .console import green
                    name = green(name)
                self.console.write("* {}".format(name))
            else:
                self.console.write("  {}".format(name))

    def connect(self, uri, user=None, password=None):
        if not user:
            user = self.user
        while True:
            graph_service = GraphService(uri, auth=(user, password))
            uri = graph_service.address.uri["/"]
            try:
                _ = graph_service.kernel_version
            except Unauthorized:
                self.console.write()
                password = getpass("Enter password for user %s: " % user)
            except Forbidden:
                if graph_service.password_change_required():
                    self.console.write()
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
        from py2neo.cypher import cypher_str

        def aligned_cypher_str(x, width):
            if isinstance(x, (int, float)):
                template = u"{:>%d}" % width
            else:
                template = u"{:<%d}" % width
            return template.format(cypher_str(x))

        records = list(cursor)
        keys = cursor.keys()
        if keys:
            widths = [len(key) for key in keys]
            for record in records:
                for i, value in enumerate(record):
                    widths[i] = max(widths[i], len(cypher_str(value)))
            templates = [u"{:%d}" % width for width in widths]
            header = u"  ".join(templates[i].format(key) for i, key in enumerate(keys)) + u"\n"
            header += u"  ".join("-" * width for width in widths)
            self.console.write_metadata(header)
            for i, record in enumerate(records):
                self.console.write(u"  ".join(aligned_cypher_str(value, widths[i]) for i, value in enumerate(record)))
        num_records = len(records)
        footer = u"(%d record%s)" % (num_records, u"" if num_records == 1 else u"s")
        self.console.write_metadata(footer)

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
            self.console.write(repr(data))
        num_sets = len(self.parameter_sets)
        self.console.write_metadata("({} parameter set{})".format(num_sets, "" if num_sets == 1 else "s"))

    def append_parameter_set(self, parameters):
        self.parameter_sets.append(parameters)

    def clear_parameter_sets(self):
        self.parameter_sets.clear()
        num_sets = len(self.parameter_sets)
        self.console.write_metadata("({} parameter set{})".format(num_sets, "" if num_sets == 1 else "s"))

    def print_dbms_details(self):
        graph_service = self.graph_service
        self.console.write_metadata("Kernel version: %s" % ".".join(map(str, graph_service.kernel_version)))
        self.console.write_metadata("Store directory: %s" % graph_service.store_directory)
        self.console.write_metadata("Store ID: %s" % graph_service.store_id)
        for store, size in graph_service.store_file_sizes.items():
            self.console.write_metadata("%s: %s" % (store, size))

    def print_config(self, search_terms):
        graph_service = self.graph_service
        for name, value in sorted(graph_service.config.items()):
            if not search_terms or all(term in name for term in search_terms):
                self.console.write_metadata("%s %s" % (name.ljust(50), value))

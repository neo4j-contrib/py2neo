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
from py2neo.cli.colour import cyan


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

    input_stream = None
    output_stream = None
    coloured_output = True
    metadata_colour = cyan

    services = None
    graph_service = None
    user = "neo4j"
    transaction = None
    run = None
    parameter_sets = None

    def __init__(self, input_stream, output_stream):
        self.input_stream = input_stream
        self.output_stream = output_stream
        self.services = {}
        self.parameter_sets = deque()

    def write(self, s="", end="\n"):
        self.output_stream.write(s)
        self.output_stream.write(end)

    def write_metadata(self, s="", end="\n"):
        if self.coloured_output:
            s = self.metadata_colour(s)
        self.write(s, end=end)

    def connect(self, uri, user=None, password=None):
        if not user:
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
        result = self.run(statement, parameters)
        result.dump(self.output_stream, colour=self.coloured_output)
        plan = result.plan()
        if plan:
            self.write_metadata(repr(plan))

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
            self.write(repr(data))
        num_sets = len(self.parameter_sets)
        self.write_metadata("({} parameter set{})".format(num_sets, "" if num_sets == 1 else "s"))

    def append_parameter_set(self, parameters):
        self.parameter_sets.append(parameters)

    def clear_parameter_sets(self):
        self.parameter_sets.clear()
        num_sets = len(self.parameter_sets)
        self.write_metadata("({} parameter set{})".format(num_sets, "" if num_sets == 1 else "s"))
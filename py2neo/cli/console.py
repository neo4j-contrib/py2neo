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


import atexit
from code import InteractiveConsole
from getpass import getpass
import readline
from os import getenv
import os.path
from platform import python_implementation, python_version, system
from sys import stderr

from neo4j.v1 import ServiceUnavailable

from py2neo import GraphService, Unauthorized, __version__ as py2neo_version
from py2neo.cypher.lang import keywords

from .colour import bright_green, blue, bright_blue

WELCOME = """\
Py2neo {py2neo_version} ({python_implementation} {python_version} on {system})
""".format(
    py2neo_version=py2neo_version,
    python_implementation=python_implementation(),
    python_version=python_version(),
    system=system(),
)
DEFAULT_EXIT_MESSAGE = "Tschüß!"
DEFAULT_HISTORY_FILE = os.path.expanduser("~/.py2neo_history")


class SimpleCompleter(object):

    matches = None

    def __init__(self, options):
        self.keywords = sorted(options)
        return

    def complete(self, text, state):
        if state == 0:
            # This is the first time for this text, so build a match list.
            if text:
                self.matches = [keyword
                                for keyword in self.keywords
                                if keyword and keyword.startswith(text.upper())]
            else:
                self.matches = self.keywords[:]

        # Return the state'th item from the match list,
        # if we have that many.
        try:
            response = self.matches[state] + " "
        except IndexError:
            response = None
        return response


class Console(InteractiveConsole):

    graph_service = None

    def __init__(self, history_file=DEFAULT_HISTORY_FILE):
        InteractiveConsole.__init__(self)

        stderr.write(WELCOME)

        user = getenv("NEO4J_USER", "neo4j")
        password = getenv("NEO4J_PASSWORD")
        while self.graph_service is None:
            graph_service = GraphService(user=user, password=password)
            try:
                _ = graph_service.kernel_version
            except Unauthorized:
                password = getpass()
            except ServiceUnavailable:
                stderr.write("Cannot connect to database service\n")
                exit(1)
            else:
                self.graph_service = graph_service
                self.graph = graph_service.graph

        readline.set_completer(SimpleCompleter(keywords).complete)
        readline.parse_and_bind("tab: complete")
        if hasattr(readline, "read_history_file"):
            try:
                readline.read_history_file(history_file)
            except IOError:
                pass

            def save_history():
                readline.set_history_length(1000)
                readline.write_history_file(history_file)

            atexit.register(save_history)

    def interact(self, banner=""):
        InteractiveConsole.interact(self, banner)

    def push(self, line):
        self.graph.run(line).dump(stderr)
        stderr.write("\n")
        return 0

    def prompt(self):
        user_at_host = "%s@%s" % (self.graph_service.user, self.graph_service.address.host)
        return bright_blue(user_at_host) + "> "

    def raw_input(self, prompt=""):
        return InteractiveConsole.raw_input(self, self.prompt())


def main():
    console = Console()
    console.interact()

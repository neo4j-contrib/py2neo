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

from py2neo import GraphService, Unauthorized, __version__ as py2neo_version, CypherSyntaxError
from py2neo.cypher.lang import keywords, first_words

from .colour import bright_blue, cyan, bright_yellow, blue, bright_green, green

WELCOME = """\
Py2neo {py2neo_version} ({python_implementation} {python_version} on {system})
""".format(
    py2neo_version=py2neo_version,
    python_implementation=python_implementation(),
    python_version=python_version(),
    system=system(),
)
HTTP = "H"
BOLT = "⚡"
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

    services = None
    graph_service = None

    def __init__(self, history_file=DEFAULT_HISTORY_FILE):
        InteractiveConsole.__init__(self)
        stderr.write(WELCOME)
        self.services = {}

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

        self.connect("localhost", getenv("NEO4J_USER", "neo4j"), getenv("NEO4J_PASSWORD"))
        self.join("localhost")

    def interact(self, banner="", *args, **kwargs):
        InteractiveConsole.interact(self, banner, *args, **kwargs)

    def prompt(self):
        if self.buffer:
            return "\x01\x1b[32m\x02...\x01\x1b[0m\x02 "
        else:
            return "\x01\x1b[32;1m\x02>>>\x01\x1b[0m\x02 "

    def raw_input(self, prompt=""):
        return InteractiveConsole.raw_input(self, self.prompt())

    def runsource(self, source, filename="<input>", symbol="single"):
        if not source:
            return 0

        words = source.strip().split()
        first_word = words[0]
        if first_word.startswith("/"):
            return self.run_slash_command(source)
        elif first_word.upper() in first_words:
            try:
                return self.run_cypher_source(source)
            except CypherSyntaxError as error:
                message = error.args[0]
                if message.startswith("Unexpected end of input") or message.startswith("Query cannot conclude with"):
                    return 1
                else:
                    stderr.write("Syntax Error: %s\n" % error.args[0])
                    return 0
        else:
            stderr.write("Syntax Error: Invalid input %s\n" % repr(first_word).lstrip("u"))
            return 0

    def run_cypher_source(self, line):
        graph = self.graph_service.graph
        result = graph.run(line)
        if result.keys():
            result.dump(stderr, colour=True)

        plan = result.plan()
        if plan:
            stderr.write(cyan(repr(plan)))
            stderr.write("\n")
        return 0

    def run_slash_command(self, line):
        words = line.strip().split()
        command = words[0].lower()
        if command == "/help":
            self.help()
        elif command == "/connect":
            self.connect(words[1])
        elif command == "/join":
            self.join(words[1])
        else:
            stderr.write("Syntax Error: Invalid slash command '%s'\n" % command)
        return 0

    def help(self):
        stderr.write("HELP!\n")

    def connect(self, host, user="neo4j", password=None):
        while True:
            graph_service = GraphService(host=host, user=user, password=password, bolt=True)
            try:
                _ = graph_service.kernel_version
            except Unauthorized:
                stderr.write("\n")
                password = getpass("Enter password for %s at %s: " % (user, host))
            except ServiceUnavailable:
                stderr.write("Cannot connect to %s\n" % graph_service.address.host)
                exit(1)
            else:
                stderr.write("Connected to %s as %s\n" % (host, user))
                self.services[host] = graph_service
                return

    def join(self, host):
        self.graph_service = self.services[host]
        stderr.write("Joined %s\n" % host)


def main():
    console = Console()
    console.interact()

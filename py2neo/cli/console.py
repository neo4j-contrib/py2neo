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
import codecs
from collections import deque
from getpass import getpass
try:
    from json import loads as json_loads, JSONDecodeError
except ImportError:
    from json import loads as json_loads
    JSONDecodeError = ValueError
import readline
from os import getenv
import os.path
from platform import python_implementation, python_version, system
from shlex import split as shlex_split
from sys import stderr, exit

from neo4j.v1 import ServiceUnavailable

from py2neo import GraphService, Unauthorized, Forbidden, __version__ as py2neo_version, CypherSyntaxError, \
    ClientError
from py2neo.cypher.lang import cypher_keywords, cypher_first_words

from .colour import cyan, green

WELCOME = """\
Py2neo {py2neo_version} ({python_implementation} {python_version} on {system})
Press [TAB] to auto-complete, type "/help" for more information or type
"/exit" to exit the console.
""".format(
    py2neo_version=py2neo_version,
    python_implementation=python_implementation(),
    python_version=python_version(),
    system=system(),
)
DEFAULT_HISTORY_FILE = os.path.expanduser("~/.py2neo_history")
DEFAULT_WRITE = stderr.write
DEFAULT_USER = "neo4j"

HELP = """\
The py2neo console accepts both raw Cypher and slash commands and supports
basic auto-completion. Available slash commands are listed below:
"""
EPILOGUE = """\
Report bugs to py2neo@nige.tech
"""


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


class Console(InteractiveConsole):

    services = None
    graph_service = None

    def __init__(self, write=DEFAULT_WRITE, history_file=DEFAULT_HISTORY_FILE):
        InteractiveConsole.__init__(self)
        self.write = write
        self.write(WELCOME)
        self.services = {}
        self.parameters = deque()

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

    def writeln(self, s=""):
        self.write(s)
        self.write("\n")

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
        try:
            source = source.lstrip()
            if not source:
                return 0
            words = source.strip().split()
            first_word = words[0]
            if first_word.startswith("/"):
                return self.run_slash_command(source)
            elif first_word.upper() in cypher_first_words:
                try:
                    return self.run_cypher_source(source)
                except CypherSyntaxError as error:
                    message = error.args[0]
                    if message.startswith("Unexpected end of input") or message.startswith("Query cannot conclude with"):
                        return 1
                    else:
                        self.write("Syntax Error: %s\n" % error.args[0])
                        return 0
            else:
                self.write("Syntax Error: Invalid input %s\n" % repr(first_word).lstrip("u"))
                return 0
        finally:
            self.writeln()

    def run_cypher_source(self, source):
        try:
            params = self.parameters.popleft()
        except IndexError:
            params = {}
        try:
            result = self.graph_service.graph.run(source, params)
        except ClientError as error:
            self.write("{:s}\n".format(error))
            return 0
        else:
            result.dump(stderr, colour=True)
            plan = result.plan()
            if plan:
                self.write(cyan(repr(plan)))
                self.write("\n")
            return 0

    def run_slash_command(self, source):
        tokens = shlex_split(source)
        command = tokens.pop(0).lower()
        if command == "/help":
            return self.help(*tokens)
        elif command == "/exit":
            return self.exit(0)
        elif command == "/connect":
            return self.connect(*tokens)
        elif command == "/play":
            return self.play(*tokens)
        elif command == "/params":
            return self.params(*tokens)
        elif command == "/push":
            return self.push_(*tokens)
        elif command == "/clear":
            return self.clear(*tokens)
        else:
            self.writeln("Syntax Error: Invalid slash command '%s'" % command)
        return 0

    def help(self, topic=None, *args):
        """

        :usage: /help [TOPIC]
        """
        self.writeln(HELP)
        for name in dir(self):
            attr = getattr(self, name)
            if callable(attr):
                try:
                    doc = attr.__doc__
                except AttributeError:
                    pass
                else:
                    if doc:
                        lines = doc.splitlines()
                        for line in lines:
                            line = line.lstrip()
                            if line.startswith(":usage:"):
                                self.writeln(line[7:].lstrip())
        self.writeln()
        self.writeln(EPILOGUE.rstrip())

    def exit(self, status=0):
        """

        :usage: /exit
        :param status:
        :return:
        """
        exit(status)

    def connect(self, host=None, user="neo4j", password=None, *args):
        """

        :usage: /connect [HOST] [USER] [PASSWORD]
        :param host:
        :param user:
        :param password:
        :param args:
        :return:
        """
        if not host:
            for name, service in sorted(self.services.items()):
                if service is self.graph_service:
                    self.writeln("* {}".format(green(name)))
                else:
                    self.writeln("  {}".format(name))
            return
        if not user:
            user = DEFAULT_USER
        while True:
            graph_service = GraphService(host=host, user=user, password=password, bolt=True)
            try:
                _ = graph_service.kernel_version
            except Unauthorized:
                password = getpass("Enter password for user %s: " % user)
            except Forbidden:
                if graph_service.password_change_required():
                    self.write("Password expired\n")
                    new_password = getpass("Enter new password for user %s: " % user)
                    password = graph_service.change_password(new_password)
                else:
                    raise
            except ServiceUnavailable:
                self.write("Cannot connect to %s\n" % graph_service.address.host)
                raise
            else:
                self.write("Connected to %s\n" % host)
                self.services[host] = graph_service
                break
        self.graph_service = self.services[host]
        readline.set_completer(SimpleCompleter(self.graph_service, cypher_keywords).complete)
        return 0

    def play(self, script=None, *args):
        """

        :usage: /play CYPHER_SCRIPT
        :param script:
        :param args:
        :return:
        """
        with codecs.open(script, encoding="utf-8") as fin:
            source = fin.read()
        return self.run_cypher_source(source)

    def params(self, *args):
        """

        :usage: /params
        :param args:
        :return:
        """
        for data in self.parameters:
            self.writeln(repr(data))
        num_sets = len(self.parameters)
        self.writeln(cyan("({} parameter set{})".format(num_sets, "" if num_sets == 1 else "s")))

    def push_(self, *args):
        """

        :usage: /push NAME=VALUE [NAME=VALUE ...]
        :param args:
        :return:
        """
        data = {}
        for arg in args:
            name, _, value = arg.partition("=")
            try:
                data[name] = json_loads(value)
            except JSONDecodeError:
                data[name] = value
        self.parameters.append(data)

    def clear(self, *args):
        """

        :usage: /clear
        :param args:
        :return:
        """
        self.parameters.clear()
        num_sets = len(self.parameters)
        self.writeln(cyan("({} parameter set{})".format(num_sets, "" if num_sets == 1 else "s")))


def main():
    Console().interact()

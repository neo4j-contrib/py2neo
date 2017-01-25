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
from sys import stdin, stdout, stderr, exit

from neo4j.v1 import ServiceUnavailable

from py2neo import GraphService, Unauthorized, Forbidden, __version__ as py2neo_version, __email__ as py2neo_email, \
    CypherSyntaxError, ClientError
from py2neo.cypher.lang import cypher_keywords, cypher_first_words

from .colour import cyan, green
from .env import Environment

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

NEO4J_URI = getenv("NEO4J_URI", "localhost")
NEO4J_USER = getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = getenv("NEO4J_PASSWORD")

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


class BaseCommander(object):

    def __init__(self, args=(), out=None):
        from sys import stdout
        self.args = args
        self.out = out or stdout
        self.script = os.path.basename(self.args[0])

    def __write_usage(self, doc, indent=0):
        if not doc:
            return
        lines = doc.splitlines()
        for line in lines:
            line = line.lstrip()
            if line.startswith(":usage:"):
                text = line[7:].lstrip().format(script=self.script)
                self.out.write(" " * indent)
                self.out.write(text)
                self.out.write("\n")

    def usage(self):
        """

        """
        self.out.write("Usage: ")
        self.__write_usage(self.__doc__)
        self.out.write("\nCommands:\n")
        for name in sorted(dir(self)):
            attr = getattr(self, name)
            if attr.__doc__ is self.__doc__:
                continue
            if callable(attr):
                try:
                    doc = attr.__doc__
                except AttributeError:
                    pass
                else:
                    self.__write_usage(doc, indent=2)


class Console(InteractiveConsole, BaseCommander):

    def __init__(self, args=(), out=None, history_file=DEFAULT_HISTORY_FILE):
        InteractiveConsole.__init__(self)
        BaseCommander.__init__(self, args, out)
        self.env = Environment(stdin, stdout)
        self.write = self.out.write
        self.write(WELCOME)

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

        self.writeln()
        try:
            self.env.connect(NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD)
        except ServiceUnavailable:
            exit(1)
        else:
            from sys import version_info
            if version_info >= (3,):
                self.writeln()

    def writeln(self, s=""):
        self.write(s)
        self.write("\n")

    def interact(self, banner="", *args, **kwargs):
        InteractiveConsole.interact(self, banner, *args, **kwargs)

    def prompt(self):
        colour = 31 if self.env.transaction else 32
        if self.buffer:
            return "\x01\x1b[%dm\x02...\x01\x1b[0m\x02 " % colour
        else:
            return "\x01\x1b[%d;1m\x02>>>\x01\x1b[0m\x02 " % colour

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
                    self.env.run_cypher(source)
                    return 0
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

    def run_slash_command(self, source):
        tokens = shlex_split(source)
        command = tokens.pop(0).lower()
        if command == "/help":
            return self.help(*tokens)
        elif command == "/exit":
            return self.exit(0)
        elif command == "/connect":
            self.env.connect(*tokens)
        elif command == "/begin":
            self.env.begin_transaction()
        elif command == "/commit":
            self.env.commit_transaction()
        elif command == "/rollback":
            self.env.rollback_transaction()
        elif command == "/play":
            return self.play(*tokens)
        elif command == "/params":
            self.env.list_parameter_sets()
        elif command == "/push":
            self.env.append_parameter_set(*tokens)
        elif command == "/clear":
            self.env.clear_parameter_sets()
        else:
            self.writeln("Syntax Error: Invalid slash command '%s'" % command)
        return 0

    def help(self, topic=None, *args):
        """

        :usage: /help [<topic>]
        """
        self.usage()
        self.writeln()
        self.writeln(EPILOGUE.rstrip())

    def exit(self, status=0):
        """

        :usage: /exit
        :param status:
        :return:
        """
        exit(status)

    def play(self, script=None, *args):
        """

        :usage: /play <script>
        :param script:
        :param args:
        :return:
        """
        with codecs.open(script, encoding="utf-8") as fin:
            source = fin.read()
        return self.env.run_cypher(source)


class Commander(BaseCommander):
    """

    :usage: {script} <command> [args]
    """

    epilog = "Report bugs to %s" % py2neo_email

    def __init__(self, args=(), out=None):
        BaseCommander.__init__(self, args, out)

    def write(self, s):
        self.out.write(s)

    def write_line(self, s=""):
        from os import linesep
        self.out.write(s)
        self.out.write(linesep)

    def usage(self):
        super(Commander, self).usage()
        if self.epilog:
            self.write_line()
            self.write_line(self.epilog)

    def execute(self):
        try:
            command, command_args = self.args[1], self.args[2:]
        except IndexError:
            Console(self.args, stderr).interact()
        else:
            command = command.replace("-", "_")
            try:
                method = getattr(self, command)
            except AttributeError:
                self.write_line("Unknown command %r" % command)
            else:
                method()

    def parser(self, script):
        from argparse import ArgumentParser
        return ArgumentParser(prog=script, epilog=self.epilog)

    def help(self):
        self.usage()

    def config(self):
        """ Display store file sizes.

        :usage: config [-f <term>]
        """
        parser = self.parser(self.args[1])
        parser.add_argument("term", nargs="*", help="filter by term")
        parser.description = "Display configuration"
        parsed = parser.parse_args(self.args[2:])
        graph_service = GraphService(NEO4J_URI, user=NEO4J_USER, password=NEO4J_PASSWORD)
        for name, value in sorted(graph_service.config.items()):
            if not parsed.term or all(term in name for term in parsed.term):
                self.write_line("%s %s" % (name.ljust(50), value))

    def evaluate(self):
        """ Evaluate a Cypher statement.

        :usage: evaluate <statement>
        """
        from py2neo.compat import unicode
        parser = self.parser(self.args[1])
        parser.description = "Evaluate a Cypher statement"
        parser.add_argument("statement", help="Cypher statement")
        parsed = parser.parse_args(self.args[2:])
        graph_service = GraphService(NEO4J_URI, user=NEO4J_USER, password=NEO4J_PASSWORD)
        self.write_line(unicode(graph_service.graph.evaluate(parsed.statement)))

    def kernel_info(self):
        """ Display kernel information.

        :usage: kernel-info
        """
        parser = self.parser(self.args[1])
        parser.description = "Display kernel information"
        graph_service = GraphService(NEO4J_URI, user=NEO4J_USER, password=NEO4J_PASSWORD)
        self.write_line("Kernel version: %s" % ".".join(map(str, graph_service.kernel_version)))
        self.write_line("Store directory: %s" % graph_service.store_directory)
        self.write_line("Store ID: %s" % graph_service.store_id)

    def store_file_sizes(self):
        """ Display store file sizes.
        :usage: store-file-sizes
        """
        parser = self.parser(self.args[1])
        parser.description = "Display store file sizes"
        graph_service = GraphService(NEO4J_URI, user=NEO4J_USER, password=NEO4J_PASSWORD)
        for store, size in graph_service.store_file_sizes.items():
            self.write_line("%s: %s" % (store, size))

    def run(self):
        """ Run a Cypher statement.
        :usage: run <statement>
        """
        parser = self.parser(self.args[1])
        parser.description = "Run a Cypher statement"
        parser.add_argument("statement", help="Cypher statement")
        parsed = parser.parse_args(self.args[2:])
        graph_service = GraphService(NEO4J_URI, user=NEO4J_USER, password=NEO4J_PASSWORD)
        graph_service.graph.run(parsed.statement).dump(self.out, colour=True)
        self.out.write("\n")


def main(args=None, out=None):
    from sys import argv
    Commander(args or argv, out).execute()

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
try:
    from json import loads as json_loads, JSONDecodeError
except ImportError:
    from json import loads as json_loads
    JSONDecodeError = ValueError
import readline
from os import getenv
import os.path
from platform import python_implementation, python_version, system
from sys import version_info

from neo4j.v1 import ServiceUnavailable

from py2neo import __version__ as py2neo_version, __email__ as py2neo_email, \
    CypherSyntaxError, ClientError
from py2neo.cypher.lang import starts_like_cypher

from .command import *
from .console import Console
from .env import Environment

WELCOME = u"""\
Py2neo {py2neo_version} ({python_implementation} {python_version} on {system})
Press [TAB] to auto-complete, type "/help" for more information or type
"/exit" to exit the console.
""".format(
    py2neo_version=py2neo_version,
    python_implementation=python_implementation(),
    python_version=python_version(),
    system=system(),
)

NEO4J_URI = getenv("NEO4J_URI", "localhost")
NEO4J_USER = getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = getenv("NEO4J_PASSWORD")

HELP = u"""\
The py2neo console accepts both raw Cypher and slash commands and supports
basic auto-completion. Available slash commands are listed below:
"""
EPILOGUE = """\
Report bugs to py2neo@nige.tech
"""


class Py2neoTool(InteractiveConsole):

    commands = {
        BeginTransactionCommand,
        CommitTransactionCommand,
        RollbackTransactionCommand,
        ShowServerDetailsCommand,
        ExitCommand,
        SetOutputFormatToHumanReadableCommand,
        SetOutputFormatToJSONCommand,
        SetOutputFormatToCSVCommand,
        SetOutputFormatToCSVWithHeaderCommand,
        SetOutputFormatToTSVCommand,
        SetOutputFormatToTSVWithHeaderCommand,
        PlayCypherCommand,
        ShowServerConfigCommand,
        AppendParameterSetCommand,
        ListParameterSetsCommand,
    }
    epilogue = "Report bugs to %s" % py2neo_email
    history_file = os.path.expanduser("~/.py2neo_history")
    history_length = 1000

    def __init__(self, *args, **kwargs):
        InteractiveConsole.__init__(self)
        self.args = args
        self.console = Console(**kwargs)
        self.env = Environment(self.console, interactive=(len(self.args) == 1))

        commands = self.commands
        epilogue = self.epilogue

        class HelpCommand(Command):
            """ Get help.
            """

            name = "help"

            def execute(self):
                self.env.print_usage_overview(commands)
                self.env.console.write()
                self.env.console.write(epilogue)

        commands.add(HelpCommand)

    def use(self):
        """ Run a set of batch commands or start the tool in interactive mode.
        """
        if self.env.interactive:
            if version_info >= (3,):
                self.console.write_help(WELCOME)
            else:
                self.console.write_help(WELCOME, end=u"")

            try:
                self.env.connect(NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD)
            except ServiceUnavailable:
                self.console.write_error("Service Unavailable: Unable to connect to database on %s" % NEO4J_URI)
                exit(1)

            self.interact()
        else:
            try:
                self.env.connect(NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD)
            except ServiceUnavailable:
                self.console.write_error("Service Unavailable: Unable to connect to database on %s" % NEO4J_URI)
                exit(1)

            for arg in self.args[1:]:
                self.runsource(arg)

    def interact(self, banner="", *args, **kwargs):
        readline.parse_and_bind("tab: complete")
        if hasattr(readline, "read_history_file"):
            try:
                readline.read_history_file(self.history_file)
            except IOError:
                pass

            def save_history():
                readline.set_history_length(self.history_length)
                readline.write_history_file(self.history_file)

            atexit.register(save_history)
        InteractiveConsole.interact(self, banner, *args, **kwargs)

    def prompt(self):
        colour = 31 if self.env.has_transaction() else 32
        if self.buffer:
            return "\x01\x1b[%dm\x02...\x01\x1b[0m\x02 " % colour
        else:
            return "\x01\x1b[%d;1m\x02>>>\x01\x1b[0m\x02 " % colour

    def raw_input(self, prompt=""):
        return InteractiveConsole.raw_input(self, self.prompt())

    def runsource(self, source, filename="<input>", symbol="single"):
        source = source.lstrip()
        more = 0
        command_prefix = self.env.command_prefix
        if not source:
            pass
        elif source.startswith(command_prefix):
            self.run_command(source[len(command_prefix):])
        elif starts_like_cypher(source):
            try:
                try:
                    RunCypherCommand(self.env, source).execute()
                except CypherSyntaxError as error:
                    message = error.args[0]
                    if message.startswith("Unexpected end of input") or message.startswith("Query cannot conclude with"):
                        more = 1
                    else:
                        raise
            except ClientError as error:
                self.console.write_error(error.args[0])
        else:
            self.console.write_error("Syntax Error: Invalid input")
        return more

    def run_command(self, source):
        for command in self.commands:
            if command.match(source):
                command.instance(self.env, source).execute()
                return
        self.console.write_error("Unknown Command")


def main(args=None, out=None):
    from sys import argv, stdout
    Py2neoTool(*args or argv, out_stream=out or stdout).use()

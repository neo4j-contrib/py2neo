#!/usr/bin/env python
# coding: utf-8

# Copyright 2011-2018, Nigel Small
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


from __future__ import division, print_function

import shlex
from datetime import datetime
from os import environ, getenv, makedirs
from os.path import expanduser, join as path_join
from subprocess import call
from tempfile import NamedTemporaryFile
from textwrap import dedent
from timeit import default_timer as timer

import click
from neo4j.exceptions import ServiceUnavailable, CypherError
from neo4j.v1 import TransactionError
from prompt_toolkit import prompt
from prompt_toolkit.history import FileHistory
from prompt_toolkit.layout.lexers import PygmentsLexer
from prompt_toolkit.styles import style_from_pygments
from pygments.styles.vim import VimStyle
from pygments.token import Token

from py2neo.cypher.reading import CypherLexer
from py2neo.data import DataList
from py2neo.database import Graph
from py2neo.meta import __version__


DEFAULT_NEO4J_URI = "bolt://localhost:7687"
DEFAULT_NEO4J_USER = "neo4j"
DEFAULT_NEO4J_PASSWORD = "password"
EDITOR = environ.get("EDITOR", "vim")
HISTORY_FILE_DIR = expanduser("~/.py2neo")
HISTORY_FILE = "console_history"


title = "Py2neo console v{}".format(__version__)
description = "Py2neo console is a Cypher runner and interactive tool for Neo4j."
history_file = expanduser("~/.py2neo_history")
quick_help = """\
  ::  to enter multi-line mode (press [Alt]+[Enter] to run)
  :e  to launch external editor
  :?  for help
  :x  to exit\
"""
full_help = """\
If command line arguments are provided, these are executed in order as
statements. If no arguments are provided, an interactive console is
presented.

Statements entered at the interactive prompt or as arguments can be
regular Cypher, transaction control keywords or slash commands. Multiple
Cypher statements can be entered on the same line separated by semicolons.
These will be executed within a single transaction.

For a handy Cypher reference, see:

  https://neo4j.com/docs/cypher-refcard/current/

Transactions can be managed interactively. To do this, use the transaction
control keywords BEGIN, COMMIT and ROLLBACK.

Slash commands provide access to supplementary functionality.

\b
{}

\b
Formatting commands:
  :csv      format output as comma-separated values
  :table    format output in a table
  :tsv      format output as tab-separated values

\b
Information commands:
  :config   show Neo4j server configuration
  :kernel   show Neo4j kernel information

Report bugs to py2neo@nige.tech\
""".format(quick_help)


class Console(object):

    multi_line = False
    watcher = None

    tx_colour = "yellow"
    err_colour = "reset"
    meta_colour = "cyan"
    prompt_colour = "cyan"

    def __init__(self, uri, auth, secure=True, verbose=False):
        try:
            self.graph = Graph(uri, auth=auth, secure=secure)
        except ServiceUnavailable as error:
            raise ConsoleError("Could not connect to {} -- {}".format(uri, error))
        self.uri = uri
        try:
            makedirs(HISTORY_FILE_DIR)
        except OSError:
            pass
        self.history = FileHistory(path_join(HISTORY_FILE_DIR, HISTORY_FILE))
        self.prompt_args = {
            "history": self.history,
            "lexer": PygmentsLexer(CypherLexer),
            "style": style_from_pygments(VimStyle, {
                Token.Prompt: "#ansi{}".format(self.prompt_colour.replace("cyan", "teal")),
                Token.TxCounter: "#ansi{} bold".format(self.tx_colour.replace("cyan", "teal")),
            })
        }
        self.lexer = CypherLexer()
        self.result_writer = DataList.write
        if verbose:
            from py2neo.watcher import watch
            self.watcher = watch("neo4j.bolt")

        self.commands = {

            "::": self.set_multi_line,
            ":e": self.edit,

            ":?": self.help,
            ":h": self.help,
            ":help": self.help,

            ":x": self.exit,
            ":exit": self.exit,

            ":csv": self.set_csv_result_writer,
            ":table": self.set_tabular_result_writer,
            ":tsv": self.set_tsv_result_writer,

            ":config": self.config,
            ":kernel": self.kernel,

        }
        self.tx = None
        self.tx_counter = 0

    def loop(self):
        click.echo(title, err=True)
        click.echo("Connected to {}".format(self.uri).rstrip(), err=True)
        click.echo(err=True)
        click.echo(dedent(quick_help), err=True)
        while True:
            try:
                source = self.read()
            except KeyboardInterrupt:
                continue
            except EOFError:
                return 0
            try:
                self.run(source)
            except ServiceUnavailable:
                return 1

    def run(self, source):
        source = source.strip()
        if not source:
            return
        try:
            if source.startswith(":"):
                self.run_command(source)
            else:
                self.run_source(source)
        except CypherError as error:
            if error.classification == "ClientError":
                pass
            elif error.classification == "DatabaseError":
                pass
            elif error.classification == "TransientError":
                pass
            else:
                pass
            click.secho("{}: {}".format(error.title, error.message), err=True)
        except TransactionError:
            click.secho("Transaction error", err=True, fg=self.err_colour)
        except ServiceUnavailable:
            raise
        except Exception as error:
            click.secho("{}: {}".format(error.__class__.__name__, str(error)), err=True, fg=self.err_colour)

    def begin_transaction(self):
        if self.tx is None:
            self.tx = self.graph.begin()
            self.tx_counter = 1
            click.secho(u"--- BEGIN at {} ---".format(datetime.now()),
                        err=True, fg=self.tx_colour, bold=True)
        else:
            click.secho(u"Transaction already open", err=True, fg=self.err_colour)

    def commit_transaction(self):
        if self.tx:
            try:
                self.tx.commit()
                click.secho(u"--- COMMIT at {} ---".format(datetime.now()),
                            err=True, fg=self.tx_colour, bold=True)
            finally:
                self.tx = None
                self.tx_counter = 0
        else:
            click.secho(u"No current transaction", err=True, fg=self.err_colour)

    def rollback_transaction(self):
        if self.tx:
            try:
                self.tx.rollback()
                click.secho(u"--- ROLLBACK at {} ---".format(datetime.now()),
                            err=True, fg=self.tx_colour, bold=True)
            finally:
                self.tx = None
                self.tx_counter = 0
        else:
            click.secho(u"No current transaction", err=True, fg=self.err_colour)

    def read(self):
        if self.multi_line:
            self.multi_line = False
            return prompt(u"", multiline=True, **self.prompt_args)

        def get_prompt_tokens(_):
            tokens = []
            if self.tx is None:
                tokens.append((Token.Prompt, "\n-> "))
            else:
                tokens.append((Token.Prompt, "\n-("))
                tokens.append((Token.TxCounter, "{}".format(self.tx_counter)))
                tokens.append((Token.Prompt, ")-> "))
            return tokens

        return prompt(get_prompt_tokens=get_prompt_tokens, **self.prompt_args)

    def run_source(self, source):
        for i, statement in enumerate(self.lexer.get_statements(source)):
            if i > 0:
                click.echo(u"")
            if statement.upper() == "BEGIN":
                self.begin_transaction()
            elif statement.upper() == "COMMIT":
                self.commit_transaction()
            elif statement.upper() == "ROLLBACK":
                self.rollback_transaction()
            elif self.tx is None:
                self.run_cypher(self.graph.run, statement, {})
            else:
                self.run_cypher(self.tx.run, statement, {}, line_no=self.tx_counter)
                self.tx_counter += 1

    def run_cypher(self, runner, statement, parameters, line_no=0):
        t0 = timer()
        result = runner(statement, parameters)
        record_count = self.write_result(result)
        status = u"{} record{} from {} in {:.3f}s".format(
            record_count,
            "" if record_count == 1 else "s",
            "server",  # TODO: summary -- address_str(result.summary().server.address),
            timer() - t0,
        )
        if line_no:
            click.secho(u"(", err=True, fg=self.meta_colour, bold=True, nl=False)
            click.secho(u"{}".format(line_no), err=True, fg=self.tx_colour, bold=True, nl=False)
            click.secho(u")->({})".format(status), err=True, fg=self.meta_colour, bold=True)
        else:
            click.secho(u"({})".format(status), err=True, fg=self.meta_colour, bold=True)

    def write_result(self, result, page_size=50):
        data = DataList(result)
        data_size = len(data)
        for skip in range(0, data_size, page_size):
            self.result_writer(data, header={"fg": "cyan", "bold": True}, skip=skip, limit=page_size)
            click.echo("\r\n", nl=False)
        return data_size

    def run_command(self, source):
        source = source.lstrip()
        assert source
        terms = shlex.split(source)
        command_name = terms[0]
        try:
            command = self.commands[command_name]
        except KeyError:
            click.secho("Unknown command: " + command_name, err=True, fg=self.err_colour)
        else:
            args = []
            kwargs = {}
            for term in terms[1:]:
                if "=" in term:
                    key, _, value = term.partition("=")
                    kwargs[key] = value
                else:
                    args.append(term)
            command(*args, **kwargs)

    def set_multi_line(self, **kwargs):
        self.multi_line = True

    def edit(self, **kwargs):
        initial_message = b""
        with NamedTemporaryFile(suffix=".cypher") as f:
            f.write(initial_message)
            f.flush()
            call([EDITOR, f.name])
            f.seek(0)
            source = f.read().decode("utf-8")
            click.echo(source)
            self.run(source)

    @classmethod
    def help(cls, **kwargs):
        click.echo(description, err=True)
        click.echo(err=True)
        click.echo(full_help.replace("\b\n", ""), err=True)

    @classmethod
    def exit(cls, **kwargs):
        exit(0)

    def load_unit_of_work(self, file_name):
        """ Load a transaction function from a cypher source file.
        """
        with open(expanduser(file_name)) as f:
            source = f.read()

        def unit_of_work(tx):
            for line_no, statement in enumerate(self.lexer.get_statements(source), start=1):
                if line_no > 0:
                    click.echo(u"")
                self.run_cypher(tx.run, statement, {}, line_no=line_no)

        return unit_of_work

    def set_csv_result_writer(self, **kwargs):
        self.result_writer = DataList.write_csv

    def set_tabular_result_writer(self, **kwargs):
        self.result_writer = DataList.write

    def set_tsv_result_writer(self, **kwargs):
        self.result_writer = DataList.write_tsv

    def config(self, **kwargs):
        result = self.graph.run("CALL dbms.listConfig")
        records = None
        last_category = None
        for record in result:
            name = record["name"]
            category, _, _ = name.partition(".")
            if category != last_category:
                if records is not None:
                    DataList(records, ["name", "value"]).write(auto_align=False, padding=0, separator=u" = ")
                    click.echo()
                records = []
            records.append((name, record["value"]))
            last_category = category
        if records is not None:
            DataList(records, ["name", "value"]).write(auto_align=False, padding=0, separator=u" = ")

    def kernel(self, **kwargs):
        result = self.graph.run("CALL dbms.queryJmx", {"query": "org.neo4j:instance=kernel#0,name=Kernel"})
        records = []
        for record in result:
            attributes = record["attributes"]
            for key, value_dict in sorted(attributes.items()):
                value = value_dict["value"]
                if key.endswith("Date") or key.endswith("Time"):
                    try:
                        value = datetime.fromtimestamp(value / 1000).isoformat(" ")
                    except:
                        pass
                records.append((key, value))
        DataList(records, ["key", "value"]).write(auto_align=False, padding=0, separator=u" = ")


class ConsoleError(Exception):

    pass


def address_str(address):
    if len(address) == 4:  # IPv6
        return "[{}]:{}".format(*address)
    else:  # IPv4
        return "{}:{}".format(*address)


@click.command(help=description, epilog=full_help)
@click.option("-U", "--uri",
              default=getenv("NEO4J_URI", DEFAULT_NEO4J_URI),
              help="Set the connection URI.")
@click.option("-u", "--user",
              default=getenv("NEO4J_USER", DEFAULT_NEO4J_USER),
              help="Set the user.")
@click.option("-p", "--password",
              default=getenv("NEO4J_PASSWORD", DEFAULT_NEO4J_PASSWORD),
              help="Set the password.")
@click.option("-i", "--insecure",
              is_flag=True,
              default=False,
              help="Use unencrypted communication (no TLS).")
@click.option("-v", "--verbose",
              is_flag=True,
              default=False,
              help="Show low level communication detail.")
@click.argument("statement", nargs=-1)
def repl(statement, uri, user, password, insecure, verbose):
    try:
        console = Console(uri, auth=(user, password), secure=not insecure, verbose=verbose)
        if statement:
            gap = False
            for s in statement:
                if gap:
                    click.echo(u"")
                console.run(s)
                if not s.startswith(":"):
                    gap = True
            exit_status = 0
        else:
            exit_status = console.loop()
    except ConsoleError as e:
        click.secho(e.args[0], err=True)
        exit_status = 1
    exit(exit_status)


if __name__ == "__main__":
    repl()

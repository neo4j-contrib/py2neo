#!/usr/bin/env python
# coding: utf-8

# Copyright 2011-2020, Nigel Small
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
from os import environ, makedirs
from os.path import expanduser, join as path_join
from subprocess import call
from tempfile import NamedTemporaryFile
from textwrap import dedent
from timeit import default_timer as timer

import click
from prompt_toolkit import prompt
from prompt_toolkit.history import FileHistory
from prompt_toolkit.lexers import PygmentsLexer
from prompt_toolkit.styles import merge_styles, style_from_pygments_cls, style_from_pygments_dict
from pygments.styles.native import NativeStyle
from pygments.token import Token

from py2neo.client import ConnectionProfile, Failure
from py2neo.cypher.lexer import CypherLexer
from py2neo.data import Table
from py2neo.database import Graph
from py2neo.meta import __version__


EDITOR = environ.get("EDITOR", "vim")

HISTORY_FILE_DIR = expanduser(path_join("~", ".py2neo"))

HISTORY_FILE = "console_history"

TITLE = "Py2neo console v{}".format(__version__)

DESCRIPTION = "Py2neo console is a Cypher runner and interactive tool for Neo4j."

QUICK_HELP = """\
  //  to enter multi-line mode (press [Alt]+[Enter] to run)
  /e  to launch external editor
  /?  for help
  /x  to exit\
"""

FULL_HELP = """\
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
Execution commands:
  /play     run a query from a file

\b
Formatting commands:
  /csv      format output as comma-separated values
  /table    format output in a table
  /tsv      format output as tab-separated values

\b
Information commands:
  /config   show Neo4j server configuration
  /kernel   show Neo4j kernel information

Report bugs to py2neo@nige.tech\
""".format(QUICK_HELP)


def is_command(source):
    if source == "//":
        return True
    if source.startswith("//"):
        return False
    if source.startswith("/*"):
        return False
    return source.startswith("/")


class ClientConsole(object):

    def echo(self, text, file=None, nl=True, err=False, color=None, **styles):
        return click.secho(text, file=file, nl=nl, err=err, color=color, **styles)

    def prompt(self, *args, **kwargs):
        return prompt(*args, **kwargs)

    multi_line = False
    watcher = None

    tx_colour = "yellow"
    err_colour = "reset"
    meta_colour = "cyan"
    prompt_colour = "cyan"

    def __init__(self, uri=None, **settings):
        self.output_file = settings.pop("file", None)
        verbose = settings.pop("verbose", False)
        profile = ConnectionProfile(uri, **settings)
        try:
            self.graph = Graph(uri, **settings)
        except OSError as error:
            raise ClientConsoleError("Could not connect to {} -- {}".format(profile.uri, error))
        try:
            makedirs(HISTORY_FILE_DIR)
        except OSError:
            pass
        self.history = FileHistory(path_join(HISTORY_FILE_DIR, HISTORY_FILE))
        self.prompt_args = {
            "history": self.history,
            "lexer": PygmentsLexer(CypherLexer),
            "style": merge_styles([
                style_from_pygments_cls(NativeStyle),
                style_from_pygments_dict({
                    Token.Prompt: "#ansi{}".format(self.prompt_colour.replace("cyan", "teal")),
                    Token.TxCounter: "#ansi{} bold".format(self.tx_colour.replace("cyan", "teal")),
                })
            ])
        }
        self.lexer = CypherLexer()
        self.result_writer = Table.write
        if verbose:
            from py2neo.diagnostics import watch
            self.watcher = watch("py2neo")

        self.commands = {

            "//": self.set_multi_line,
            "/e": self.edit,

            "/?": self.help,
            "/h": self.help,
            "/help": self.help,

            "/x": self.exit,
            "/exit": self.exit,

            "/play": self.play,

            "/csv": self.set_csv_result_writer,
            "/table": self.set_tabular_result_writer,
            "/tsv": self.set_tsv_result_writer,

            "/config": self.config,
            "/kernel": self.kernel,

        }
        self.tx = None
        self.tx_counter = 0

    def loop(self):
        self.echo(TITLE, err=True)
        self.echo("Connected to {}".format(self.graph.service.uri).rstrip(), err=True)
        self.echo(u"", err=True)
        self.echo(dedent(QUICK_HELP), err=True)
        while True:
            try:
                source = self.read()
            except KeyboardInterrupt:
                continue
            except EOFError:
                return 0
            try:
                self.run(source)
            except OSError as error:
                self.echo("Service Unavailable: %s" % (error.args[0]), err=True)

    def run_all(self, sources):
        gap = False
        for s in sources:
            if gap:
                self.echo("")
            self.run(s)
            if not is_command(s):
                gap = True
        return 0

    def run(self, source):
        source = source.strip()
        if not source:
            return
        try:
            if is_command(source):
                self.run_command(source)
            else:
                self.run_source(source)
        except Failure as error:
            if error.classification == "ClientError":
                pass
            elif error.classification == "DatabaseError":
                pass
            elif error.classification == "TransientError":
                pass
            else:
                pass
            self.echo("{}: {}".format(error.title, error.message), err=True)
        # except TransactionError:
        #     self.echo("Transaction error", err=True, fg=self.err_colour)
        except OSError:
            raise
        except Exception as error:
            self.echo("{}: {}".format(error.__class__.__name__, str(error)), err=True, fg=self.err_colour)

    def begin_transaction(self):
        if self.tx is None:
            self.tx = self.graph.begin()
            self.tx_counter = 1
            self.echo(u"--- BEGIN at {} ---".format(datetime.now()),
                      err=True, fg=self.tx_colour, bold=True)
        else:
            self.echo(u"Transaction already open", err=True, fg=self.err_colour)

    def commit_transaction(self):
        if self.tx:
            try:
                self.tx.commit()
                self.echo(u"--- COMMIT at {} ---".format(datetime.now()),
                          err=True, fg=self.tx_colour, bold=True)
            finally:
                self.tx = None
                self.tx_counter = 0
        else:
            self.echo(u"No current transaction", err=True, fg=self.err_colour)

    def rollback_transaction(self):
        if self.tx:
            try:
                self.tx.rollback()
                self.echo(u"--- ROLLBACK at {} ---".format(datetime.now()),
                          err=True, fg=self.tx_colour, bold=True)
            finally:
                self.tx = None
                self.tx_counter = 0
        else:
            self.echo(u"No current transaction", err=True, fg=self.err_colour)

    def read(self):
        if self.multi_line:
            self.multi_line = False
            return self.prompt(u"", multiline=True, **self.prompt_args)

        def get_prompt_tokens():
            tokens = []
            if self.tx is None:
                tokens.append(("class:pygments.prompt", "\n-> "))
            else:
                tokens.append(("class:pygments.prompt", "\n-("))
                tokens.append(("class:pygments.txcounter", "{}".format(self.tx_counter)))
                tokens.append(("class:pygments.prompt", ")-> "))
            return tokens

        return self.prompt(get_prompt_tokens, **self.prompt_args)

    def run_source(self, source):
        for i, statement in enumerate(self.lexer.get_statements(source)):
            if i > 0:
                self.echo(u"")
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
        summary = result.summary()
        if summary.connection:
            uri = summary.connection["uri"]
        else:
            uri = self.graph.service.uri
        status = u"{} record{} from {} in {:.3f}s".format(
            record_count,
            "" if record_count == 1 else "s",
            uri,
            timer() - t0,
        )
        if line_no:
            self.echo(u"(", err=True, fg=self.meta_colour, bold=True, nl=False)
            self.echo(u"{}".format(line_no), err=True, fg=self.tx_colour, bold=True, nl=False)
            self.echo(u")->({})".format(status), err=True, fg=self.meta_colour, bold=True)
        else:
            self.echo(u"({})".format(status), err=True, fg=self.meta_colour, bold=True)

    def write_result(self, result, page_size=50):
        table = Table(result)
        table_size = len(table)
        for skip in range(0, table_size, page_size):
            self.result_writer(table, file=self.output_file, header={"fg": "cyan", "bold": True}, skip=skip, limit=page_size)
            self.echo("\r\n", nl=False)
        return table_size

    def run_command(self, source):
        source = source.lstrip()
        assert source
        terms = shlex.split(source)
        command_name = terms[0]
        try:
            command = self.commands[command_name]
        except KeyError:
            self.echo("Unknown command: " + command_name, err=True, fg=self.err_colour)
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
            self.echo(source)
            self.run(source)

    def help(self, **kwargs):
        self.echo(DESCRIPTION, err=True)
        self.echo(u"", err=True)
        self.echo(FULL_HELP.replace("\b\n", ""), err=True)

    def exit(self, **kwargs):
        exit(0)

    def play(self, file_name):
        work = self.load_unit_of_work(file_name=file_name)
        with self.graph.begin() as tx:
            work(tx)

    def load_unit_of_work(self, file_name):
        """ Load a transaction function from a cypher source file.
        """
        with open(expanduser(file_name)) as f:
            source = f.read()

        def unit_of_work(tx):
            for line_no, statement in enumerate(self.lexer.get_statements(source), start=1):
                if line_no > 0:
                    self.echo(u"")
                self.run_cypher(tx.run, statement, {}, line_no=line_no)

        return unit_of_work

    def set_csv_result_writer(self, **kwargs):
        self.result_writer = Table.write_csv

    def set_tabular_result_writer(self, **kwargs):
        self.result_writer = Table.write

    def set_tsv_result_writer(self, **kwargs):
        self.result_writer = Table.write_tsv

    def config(self, **kwargs):
        result = self.graph.run("CALL dbms.listConfig")
        records = None
        last_category = None
        for record in result:
            name = record["name"]
            category, _, _ = name.partition(".")
            if category != last_category:
                if records is not None:
                    Table(records, ["name", "value"]).write(auto_align=False, padding=0, separator=u" = ")
                    self.echo(u"")
                records = []
            records.append((name, record["value"]))
            last_category = category
        if records is not None:
            Table(records, ["name", "value"]).write(auto_align=False, padding=0, separator=u" = ")

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
        Table(records, ["key", "value"]).write(auto_align=False, padding=0, separator=u" = ")


class ClientConsoleError(Exception):

    pass

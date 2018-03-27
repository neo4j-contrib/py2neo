#!/usr/bin/env python
# coding: utf-8

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


from __future__ import division, print_function

from datetime import datetime
import shlex
import os
from os.path import expanduser
from subprocess import call
from tempfile import NamedTemporaryFile
from timeit import default_timer as timer
from textwrap import dedent

import click
from py2neo.cypher.lex import CypherLexer
from neo4j.v1 import GraphDatabase, ServiceUnavailable, CypherError, TransactionError
from prompt_toolkit import prompt
from prompt_toolkit.history import FileHistory
from prompt_toolkit.layout.lexers import PygmentsLexer
from prompt_toolkit.styles import style_from_pygments
from pygments.styles.vim import VimStyle
from pygments.token import Token

from .data import TabularResultWriter, CSVResultWriter, TSVResultWriter
from .meta import title, description, quick_help, full_help
from .table import Table


EDITOR = os.environ.get("EDITOR", "vim")
HISTORY_FILE = expanduser("~/.n4_history")


class Console(object):

    multi_line = False
    watcher = None

    tx_colour = "yellow"
    err_colour = "reset"
    meta_colour = "cyan"
    prompt_colour = "cyan"

    def __init__(self, uri, auth, secure=True, verbose=False):
        try:
            self.driver = GraphDatabase.driver(uri, auth=auth, encrypted=secure)
        except ServiceUnavailable as error:
            raise ConsoleError("Could not connect to {} ({})".format(uri, error))
        self.uri = uri
        self.history = FileHistory(HISTORY_FILE)
        self.prompt_args = {
            "history": self.history,
            "lexer": PygmentsLexer(CypherLexer),
            "style": style_from_pygments(VimStyle, {
                Token.Prompt: "#ansi{}".format(self.prompt_colour.replace("cyan", "teal")),
                Token.TxCounter: "#ansi{} bold".format(self.tx_colour.replace("cyan", "teal")),
            })
        }
        self.lexer = CypherLexer()
        self.result_writer = TabularResultWriter()
        if verbose:
            from .watcher import watch
            self.watcher = watch("neo4j.bolt")

        self.commands = {

            "//": self.set_multi_line,
            "/e": self.edit,

            "/?": self.help,
            "/h": self.help,
            "/help": self.help,

            "/x": self.exit,
            "/exit": self.exit,

            "/r": self.run_read_tx,
            "/read": self.run_read_tx,
            "/w": self.run_write_tx,
            "/write": self.run_write_tx,

            "/csv": self.set_csv_result_writer,
            "/table": self.set_tabular_result_writer,
            "/tsv": self.set_tsv_result_writer,

            "/config": self.config,
            "/kernel": self.kernel,

        }
        self.session = None
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
            if source.startswith("/"):
                try:
                    self.run_command(source)
                except TypeError:
                    self.run_source(source)
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
            self.session = self.driver.session()
            self.tx = self.session.begin_transaction()
            self.tx_counter = 1
            click.secho(u"--- BEGIN at {} ---".format(datetime.now()),
                        err=True, fg=self.tx_colour, bold=True)
        else:
            click.secho(u"Transaction already open", err=True, fg=self.err_colour)

    def commit_transaction(self):
        if self.session:
            try:
                self.session.commit_transaction()
                click.secho(u"--- COMMIT at {} ---".format(datetime.now()),
                            err=True, fg=self.tx_colour, bold=True)
            finally:
                self.tx = None
                self.tx_counter = 0
                self.session.close()
                self.session = None
        else:
            click.secho(u"No current transaction", err=True, fg=self.err_colour)

    def rollback_transaction(self):
        if self.session:
            try:
                self.session.rollback_transaction()
                click.secho(u"--- ROLLBACK at {} ---".format(datetime.now()),
                            err=True, fg=self.tx_colour, bold=True)
            finally:
                self.tx = None
                self.tx_counter = 0
                self.session.close()
                self.session = None
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
                with self.driver.session() as session:
                    self.run_cypher(session.run, statement, {})
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
            address_str(result.summary().server.address),
            timer() - t0,
        )
        if line_no:
            click.secho(u"(", err=True, fg=self.meta_colour, bold=True, nl=False)
            click.secho(u"{}".format(line_no), err=True, fg=self.tx_colour, bold=True, nl=False)
            click.secho(u")->({})".format(status), err=True, fg=self.meta_colour, bold=True)
        else:
            click.secho(u"({})".format(status), err=True, fg=self.meta_colour, bold=True)

    def write_result(self, result, page_size=50):
        record_count = 0
        if result.keys():
            self.result_writer.write_header(result)
            more = True
            while more:
                record_count += self.result_writer.write(result, page_size)
                more = result.peek() is not None
        return record_count

    def run_command(self, source):
        source = source.lstrip()
        assert source
        terms = shlex.split(source)
        command_name = terms[0]
        try:
            command = self.commands[command_name]
        except KeyError:
            if terms[0].startswith("//"):
                raise TypeError("Comment not command")
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

    def run_read_tx(self, *args, **kwargs):
        if args:
            with self.driver.session() as session:
                session.read_transaction(self.load_unit_of_work(args[0]))
        else:
            click.secho("Usage: /r FILE", err=True, fg=self.err_colour)

    def run_write_tx(self, *args, **kwargs):
        if args:
            with self.driver.session() as session:
                session.write_transaction(self.load_unit_of_work(args[0]))
        else:
            click.secho("Usage: /w FILE", err=True, fg=self.err_colour)

    def set_csv_result_writer(self, **kwargs):
        self.result_writer = CSVResultWriter()

    def set_tabular_result_writer(self, **kwargs):
        self.result_writer = TabularResultWriter()

    def set_tsv_result_writer(self, **kwargs):
        self.result_writer = TSVResultWriter()

    def config(self, **kwargs):
        with self.driver.session() as session:
            result = session.run("CALL dbms.listConfig")
            table = None
            last_category = None
            for record in result:
                name = record["name"]
                category, _, _ = name.partition(".")
                if category != last_category:
                    if table is not None:
                        table.echo(header_style={"fg": "cyan"})
                        click.echo()
                    table = Table(["name", "value"], field_separator=u" = ", padding=0, auto_align=False, header=0)
                table.append((name, record["value"]))
                last_category = category
            table.echo(header_style={"fg": self.meta_colour})

    def kernel(self, **kwargs):
        with self.driver.session() as session:
            result = session.run("CALL dbms.queryJmx", {"query": "org.neo4j:instance=kernel#0,name=Kernel"})
            table = Table(["key", "value"], field_separator=u" = ", padding=0, auto_align=False, header=0)
            for record in result:
                attributes = record["attributes"]
                for key, value_dict in sorted(attributes.items()):
                    value = value_dict["value"]
                    if key.endswith("Date") or key.endswith("Time"):
                        try:
                            value = datetime.fromtimestamp(value / 1000).isoformat(" ")
                        except:
                            pass
                    table.append((key, value))
            table.echo(header_style={"fg": self.meta_colour})


class ConsoleError(Exception):

    pass


def address_str(address):
    if len(address) == 4:  # IPv6
        return "[{}]:{}".format(*address)
    else:  # IPv4
        return "{}:{}".format(*address)

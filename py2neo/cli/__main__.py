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


from os import getenv

import click

from py2neo.cli.console import Console, ConsoleError
from py2neo.cli.meta import description, full_help


DEFAULT_NEO4J_URI = "bolt://localhost:7687"
DEFAULT_NEO4J_USER = "neo4j"
DEFAULT_NEO4J_PASSWORD = "password"


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
                if not s.startswith("/"):
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

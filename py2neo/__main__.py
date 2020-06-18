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


import click

from py2neo.meta import NEO4J_URI, NEO4J_AUTH


@click.group(help="""\
Multipurpose Neo4j toolkit.
""")
def py2neo():
    pass


@py2neo.command("version", help="""\
Display the current library version
""")
def py2neo_version():
    from py2neo.meta import __version__
    click.echo(__version__)


@py2neo.command("console", help="""\
Interactive Cypher console
""")
@click.option("-u", "--uri", default=NEO4J_URI,
              help="Set the connection URI.")
@click.option("-a", "--auth", default=NEO4J_AUTH, metavar="USER:PASSWORD",
              help="Set the user and password.")
@click.option("-s", "--secure", is_flag=True, default=False,
              help="Use encrypted communication (TLS).")
@click.option("-v", "--verbose", is_flag=True, default=False,
              help="Show low level communication detail.")
def py2neo_console(uri, auth=None, secure=None, verbose=None):
    from py2neo.console import Console, ConsoleError
    try:
        con = Console(uri, auth=auth, secure=secure, verbose=verbose)
    except ConsoleError as error:
        click.echo(error)
        raise SystemExit(1)
    else:
        raise SystemExit(con.loop())


@py2neo.command("run", help="""\
Run a Cypher query
""")
@click.option("-u", "--uri", default=NEO4J_URI,
              help="Set the connection URI.")
@click.option("-a", "--auth", default=NEO4J_AUTH, metavar="USER:PASSWORD",
              help="Set the user and password.")
@click.option("-s", "--secure", is_flag=True, default=False,
              help="Use encrypted communication (TLS).")
@click.option("-v", "--verbose", is_flag=True, default=False,
              help="Show low level communication detail.")
@click.argument("cypher", nargs=-1)
def py2neo_run(cypher, uri, auth=None, secure=None, verbose=None):
    from py2neo.console import Console, ConsoleError
    try:
        con = Console(uri, auth=auth, secure=secure, verbose=verbose)
    except ConsoleError as error:
        click.echo(error)
        raise SystemExit(1)
    else:
        raise SystemExit(con.run_all(cypher))


def main():
    try:
        py2neo(obj={})
    except Exception as error:
        click.secho(error.args[0], err=True)
        exit(1)
    else:
        exit(0)


if __name__ == "__main__":
    main()

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


import click

from py2neo.console import Console, ConsoleError
from py2neo.console.meta import DESCRIPTION, FULL_HELP
from py2neo.internal.addressing import NEO4J_URI, NEO4J_AUTH


@click.command(help=DESCRIPTION, epilog=FULL_HELP)
@click.option("-u", "--uri", default=NEO4J_URI, help="Set the connection URI.")
@click.option("-a", "--auth", default=NEO4J_AUTH, help="Set the user and password.")
@click.option("-s", "--secure", is_flag=True, default=False, help="Use encrypted communication (TLS).")
@click.option("-v", "--verbose", is_flag=True, default=False, help="Show low level communication detail.")
@click.argument("cypher", nargs=-1)
def main(cypher, uri, auth=None, secure=None, verbose=None):
    try:
        console = Console(uri, auth=auth, secure=secure, verbose=verbose)
    except ConsoleError as error:
        click.echo(error)
        raise SystemExit(1)
    else:
        raise SystemExit(console.run_all_or_loop(cypher))


if __name__ == "__main__":
    main()

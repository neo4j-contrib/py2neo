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


from sys import exit

import click

from py2neo.meta import NEO4J_URI, NEO4J_AUTH
from py2neo.wire import Address


class AddressParamType(click.ParamType):

    name = "addr"

    def __init__(self, default_host=None, default_port=None):
        self.default_host = default_host
        self.default_port = default_port

    def convert(self, value, param, ctx):
        return Address.parse(value, self.default_host, self.default_port)

    def __repr__(self):
        return 'HOST:PORT'


class AuthParamType(click.ParamType):

    name = "auth"

    def __init__(self, default_user=None, default_password=None):
        self.default_user = default_user
        self.default_password = default_password

    def convert(self, value, param, ctx):
        try:
            from py2neo.security import make_auth
            return make_auth(value, self.default_user, self.default_password)
        except ValueError as e:
            self.fail(e.args[0], param, ctx)

    def __repr__(self):
        return 'USER:PASSWORD'


def watch_log(ctx, param, value):
    from logging import INFO, DEBUG
    from py2neo.diagnostics import watch
    watch("py2neo", DEBUG if value >= 1 else INFO)


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
    from py2neo.client.console import ClientConsole, ClientConsoleError
    try:
        con = ClientConsole(uri, auth=auth, secure=secure, verbose=verbose)
    except ClientConsoleError as error:
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
    from py2neo.client.console import ClientConsole, ClientConsoleError
    try:
        con = ClientConsole(uri, auth=auth, secure=secure, verbose=verbose)
    except ClientConsoleError as error:
        click.echo(error)
        raise SystemExit(1)
    else:
        raise SystemExit(con.run_all(cypher))


@py2neo.command("server", help="""\
Run a Neo4j cluster or standalone server in one or more local Docker 
containers.

If an additional COMMAND is supplied, this will be executed after startup, 
with a shutdown occurring immediately afterwards. If no COMMAND is supplied,
an interactive command line console will be launched which allows direct
control of the service. This console can be shut down with Ctrl+C, Ctrl+D or
by entering the command 'exit'.

A couple of environment variables will also be made available to any COMMAND
passed. These are:

\b
- BOLT_SERVER_ADDR
- NEO4J_AUTH

""")
@click.option("-a", "--auth", type=AuthParamType(), envvar="NEO4J_AUTH",
              help="Credentials with which to bootstrap the service. These "
                   "must be specified as a 'user:password' pair and may "
                   "alternatively be supplied via the NEO4J_AUTH environment "
                   "variable. These credentials will also be exported to any "
                   "COMMAND executed during the service run.")
@click.option("-n", "--name",
              help="A Docker network name to which all servers will be "
                   "attached. If omitted, an auto-generated name will be "
                   "used.")
@click.option("-v", "--verbose", count=True, callback=watch_log,
              expose_value=False, is_eager=True,
              help="Show more detail about the startup and shutdown process.")
@click.option("-z", "--self-signed-certificate", is_flag=True)
@click.argument("image", default="latest")
def py2neo_server(name, image, auth, self_signed_certificate):
    from py2neo.security import make_self_signed_certificate
    from py2neo.server import Neo4jService
    try:
        if self_signed_certificate:
            cert_key_pair = make_self_signed_certificate()
        else:
            cert_key_pair = None
        with Neo4jService.single_instance(name, image, auth, cert_key_pair) as neo4j:
            neo4j.run_console()
    except KeyboardInterrupt:
        exit(130)
    except Exception as e:
        message = " ".join(map(str, e.args))
        if hasattr(e, 'explanation'):
            message += "\n" + e.explanation
        click.echo(message, err=True)
        exit(1)


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

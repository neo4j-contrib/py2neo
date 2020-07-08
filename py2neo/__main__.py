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


from __future__ import print_function

from argparse import ArgumentParser
from inspect import getdoc
from sys import stderr, exit

from py2neo import __version__
from py2neo.security import make_auth, make_self_signed_certificate
from py2neo.server import Neo4jService
from py2neo.server.console import Neo4jConsole


def argument(*args, **kwargs):
    """ Decorator for specifying argparse arguments attached to a
    function.

    ::

        @argument("-v", "--verbose", action="count", default=0,
                  help="Increase verbosity.")
        def foo(verbose):
            pass

    """

    def f__(f):
        def f_(*a, **kw):
            return f(*a, **kw)

        f_.__name__ = f.__name__
        f_.__doc__ = f.__doc__
        f_.__dict__.update(f.__dict__)
        if hasattr(f, "arguments"):
            f_.arguments = f.arguments
        else:
            f_.arguments = []
        f_.arguments.insert(0, (args, kwargs))
        return f_

    return f__


def version():
    """ Display the current library version.
    """
    print(__version__)


@argument("-u", "--uri",
          help="Set the connection URI.")
@argument("-a", "--auth", metavar="USER:PASSWORD",
          help="Set the user and password.")
@argument("-s", "--secure", action="store_true",
          help="Use encrypted communication (TLS).")
@argument("-v", "--verbose", action="count", default=0,
          help="Adjust level of communication detail.")
def console(uri=None, auth=None, secure=None, verbose=0):
    """ Interactive Cypher console.
    """
    from py2neo.client.console import ClientConsole
    con = ClientConsole(uri, auth=auth, secure=secure, verbosity=verbose)
    con.loop()


@argument("-u", "--uri",
          help="Set the connection URI.")
@argument("-a", "--auth", metavar="USER:PASSWORD",
          help="Set the user and password.")
@argument("-q", "--quiet", action="count", default=0,
          help="Reduce verbosity.")
@argument("-s", "--secure", action="store_true", default=False,
          help="Use encrypted communication (TLS).")
@argument("-v", "--verbose", action="count", default=0,
          help="Increase verbosity.")
@argument("-x", "--times", type=int, default=1,
          help="Number of times to repeat.")
@argument("cypher", nargs="+")
def run(cypher, uri, auth=None, secure=False, verbose=False, quiet=False, times=1):
    """ Run one or more Cypher query.
    """
    from py2neo.client.console import ClientConsole
    con = ClientConsole(uri, auth=auth, secure=secure, verbosity=(verbose - quiet), welcome=False)
    con.process_all(cypher, times)


@argument("-a", "--auth", type=make_auth,
          help="Credentials with which to bootstrap the service. "
               "These must be specified as a 'user:password' pair.")
@argument("-n", "--name",
          help="A Docker network name to which all servers will be "
               "attached. If omitted, an auto-generated name will be "
               "used.")
@argument("-v", "--verbose", action="count", default=0,
          help="Increase verbosity.")
@argument("-z", "--self-signed-certificate", action="store_true",
          help="Generate and use a self-signed certificate")
@argument("image", nargs="?", default="latest",
          help="Docker image to use (defaults to 'latest')")
def server(name, image, auth, self_signed_certificate, verbose):
    """ Start a Neo4j service in a Docker container.
    """
    con = Neo4jConsole()
    con.verbosity = verbose
    try:
        if self_signed_certificate:
            cert_key_pair = make_self_signed_certificate()
        else:
            cert_key_pair = None
        with Neo4jService.single_instance(name, image, auth, cert_key_pair) as neo4j:
            con.service = neo4j
            con.env()
            con.loop()
    except KeyboardInterrupt:
        exit(130)
    except Exception as e:
        message = " ".join(map(str, e.args))
        if hasattr(e, 'explanation'):
            message += "\n" + e.explanation
        print(message, file=stderr)
        exit(1)


def movies():
    """ Start the demo 'movies' web server.
    """
    from py2neo.packages.bottle import load_app
    load_app("py2neo.movies").run()


def main():
    parser = ArgumentParser("py2neo")
    subparsers = parser.add_subparsers(title="commands")

    def add_command(func, name):
        subparser = subparsers.add_parser(name, help=getdoc(func))
        subparser.set_defaults(f=func)
        if hasattr(func, "arguments"):
            for a, kw in func.arguments:
                subparser.add_argument(*a, **kw)

    add_command(console, "console")
    add_command(movies, "movies")
    add_command(run, "run")
    add_command(server, "server")
    add_command(version, "version")

    args = parser.parse_args()
    kwargs = vars(args)
    f = kwargs.pop("f")
    f(**kwargs)


if __name__ == "__main__":
    main()

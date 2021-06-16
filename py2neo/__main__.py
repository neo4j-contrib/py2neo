#!/usr/bin/env python
# coding: utf-8

# Copyright 2011-2021, Nigel Small
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

from py2neo import __version__
from py2neo.client.__main__ import console


def version():
    """ Display the current library version.
    """
    print(__version__)


def movies():
    """ Start the demo 'movies' web server.
    """
    from py2neo.vendor.bottle import load_app
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
    add_command(console, "run")
    add_command(version, "version")

    args = parser.parse_args()
    kwargs = vars(args)
    try:
        f = kwargs.pop("f")
    except KeyError:
        parser.print_help()
    else:
        f(**kwargs)


if __name__ == "__main__":
    main()

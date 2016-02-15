#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright 2011-2015, Nigel Small
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


__author__ = "Nigel Small <nigel@py2neo.org>"
__copyright__ = "2011-2015, Nigel Small"
__email__ = "nigel@py2neo.org"
__license__ = "Apache License, Version 2.0"
__package__ = "py2neo"
__version__ = "3b1"

PRODUCT = ("py2neo", __version__)


from py2neo.types import *
from py2neo.env import *
from py2neo.ext import *
from py2neo.database import *
from py2neo.http import *
from py2neo.packages.httpstream.watch import watch
from py2neo.status import Finished, GraphError

from py2neo.util import BaseCommander as _BaseCommander


class Commander(_BaseCommander):
    """
    usage: py2neo run [-h] [-H host] [-P port] statement
           py2neo evaluate [-h] [-H host] [-P port] statement
    """

    epilog = "Report bugs to %s" % __email__

    def __init__(self, out=None):
        _BaseCommander.__init__(self, out)

    def parser_with_connection(self, script):
        parser = self.parser(script)
        parser.add_argument("-H", "--host",
                            metavar="host", help="database host", default="localhost")
        parser.add_argument("-P", "--port",
                            metavar="port", help="database port (HTTP)", type=int, default=7474)
        return parser

    def config(self, *args):
        """ Display store file sizes.
        usage: config [-H <host>] [-P <port>] [-f <term>]
        """
        parser = self.parser_with_connection(args[0])
        parser.add_argument("term", nargs="*", help="filter by term")
        parser.description = "Display configuration"
        parsed = parser.parse_args(args[1:])
        dbms = DBMS("http://%s:%d" % (parsed.host, parsed.port))
        for name, value in sorted(dbms.config().items()):
            if not parsed.term or all(term in name for term in parsed.term):
                self.write_line("%s %s" % (name.ljust(50), value))

    def evaluate(self, *args):
        """ Evaluate a Cypher statement.
        usage: evaluate [-H <host>] [-P <port>] <statement>
        """
        parser = self.parser_with_connection(args[0])
        parser.description = "Evaluate a Cypher statement"
        parser.add_argument("statement", help="Cypher statement")
        parsed = parser.parse_args(args[1:])
        dbms = DBMS("http://%s:%d" % (parsed.host, parsed.port))
        self.write_line(ustr(dbms.graph.evaluate(parsed.statement)))

    def kernel_info(self, *args):
        """ Display kernel information.
        usage: kernel-info [-H <host>] [-P <port>]
        """
        parser = self.parser_with_connection(args[0])
        parser.description = "Display kernel information"
        parsed = parser.parse_args(args[1:])
        dbms = DBMS("http://%s:%d" % (parsed.host, parsed.port))
        self.write_line("Kernel version: %s" % dbms.kernel_version())
        self.write_line("Store directory: %s" % dbms.store_directory())
        self.write_line("Store ID: %s" % dbms.store_id())

    def store_file_sizes(self, *args):
        """ Display store file sizes.
        usage: store-file-sizes [-H <host>] [-P <port>]
        """
        parser = self.parser_with_connection(args[0])
        parser.description = "Display store file sizes"
        parsed = parser.parse_args(args[1:])
        dbms = DBMS("http://%s:%d" % (parsed.host, parsed.port))
        for store, size in dbms.store_file_sizes().items():
            self.write_line("%s: %s" % (store, size))

    def run(self, *args):
        """ Run a Cypher statement.
        usage: run [-H <host>] [-P <port>] <statement>
        """
        parser = self.parser_with_connection(args[0])
        parser.description = "Run a Cypher statement"
        parser.add_argument("statement", help="Cypher statement")
        parsed = parser.parse_args(args[1:])
        dbms = DBMS("http://%s:%d" % (parsed.host, parsed.port))
        dbms.graph.run(parsed.statement).dump(self.out)


def main(args=None, out=None):
    from sys import argv
    Commander(out).execute(*args or argv)

if __name__ == "__main__":
    main()

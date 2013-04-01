#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright 2011-2013, Nigel Small
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

""" High level tools for Neo4j.
"""

import os
import sys

from . import neo4j, cypher, geoff, rest
from . import __package__ as py2neo_package
from . import __version__ as py2neo_version
from . import __copyright__ as py2neo_copyright


SCRIPT_NAME = "neotool"
HELP = """\
Usage:
  {script} <options> <command> <args>
Options:
  -h/--help [<command>]  Show tool usage
  -v/--version           Show tool version
  -c/--copyright         Show tool copyright
  -S/--scheme <scheme>   Set database scheme
  -H/--host <host>       Set database host
  -P/--port <port>       Set database port
Commands:
  clear                 Clear all nodes and relationships
  cypher <query>        Execute Cypher query and output as text
  cypher-csv <query>    Execute Cypher query and output as CSV
  cypher-geoff <query>  Execute Cypher query and output as Geoff
  cypher-json <query>   Execute Cypher query and output as JSON
  cypher-tsv <query>    Execute Cypher query and output as tab separated values
  geoff-insert <file>   Insert Geoff data
  geoff-merge <file>    Merge Geoff data
  xml-insert <file>     Insert XML data
  xml-merge <file>      Merge XML data
"""


class Tool(object):

    def __init__(self, in_=None, out=None, err=None):
        self._scheme = "http"
        self._host = "localhost"
        self._port = 7474
        self._in = in_ or sys.stdin
        self._out = out or sys.stdout
        self._err = err or sys.stderr
        self._script = None

    @property
    def _graph_db(self):
        metadata = rest.ServiceRoot.get(self._scheme, self._host, self._port)
        return neo4j.GraphDatabaseService.get_instance(metadata["data"])

    def _version(self):
        """ Show tool version
        """
        self._out.write("{0} ({1}/{2})\n".format(SCRIPT_NAME, py2neo_package, py2neo_version))

    def _copyright(self):
        """ Show tool copyright
        """
        self._out.write("(C) Copyright {0}\n".format(py2neo_copyright))

    def _help(self):
        """ Show tool usage
        """
        script = self._script.split(os.sep)[-1].rstrip(".py")
        self._version()
        self._out.write(HELP.format(script=script))
        self._copyright()

    def do(self, args):
        self._script = args.pop(0)
        command = None
        while not command:
            arg = args.pop(0)
            if arg.startswith("-"):
                if arg in ("-h", "--help"):
                    self._help()
                    sys.exit(0)
                elif arg in ("-v", "--version"):
                    self._version()
                    sys.exit(0)
                elif arg in ("-c", "--copyright"):
                    self._copyright()
                    sys.exit(0)
                elif arg in ("-S", "--scheme"):
                    self._scheme = args.pop(0)
                elif arg in ("-H", "--host"):
                    self._host = args.pop(0)
                elif arg in ("-P", "--port"):
                    self._port = int(args.pop(0))
                else:
                    raise ValueError("Unknown option {0}".format(repr(arg)))
            else:
                command = arg
        try:
            method = getattr(self, command.replace("-", "_"))
        except AttributeError:
            raise ValueError("Unknown command {0}".format(repr(command)))
        method(*args)

    def _error(self, message, exception=None, stacktrace=None):
        if exception:
            self._err.write("{0}: {1}\n".format(exception, message))
        else:
            self._err.write("{0}\n".format(message))

    def clear(self):
        """ Clear all nodes and relationships.
        """
        self._graph_db.clear()

    def _cypher(self, format, query, params=None):
        if query == "-":
            query = self._in.read()
        cypher.write(format, self._out, self._graph_db, query, params,
                     error_handler=self._error)

    def cypher(self, query, params=None):
        """ Execute Cypher query and output as text.
        """
        self._cypher("text", query, params)

    def cypher_csv(self, query, params=None):
        """ Execute Cypher query and output as comma separated values
        """
        self._cypher("csv", query, params)

    def cypher_geoff(self, query, params=None):
        """ Execute Cypher query and output as Geoff
        """
        self._cypher("geoff", query, params)

    def cypher_json(self, query, params=None):
        """ Execute Cypher query and output as JSON
        """
        self._cypher("json", query, params)

    def cypher_tsv(self, query, params=None):
        """ Execute Cypher query and output as tab separated values
        """
        self._cypher("tsv", query, params)

    def _geoff_write(self, params):
        for key, value in params.items():
            sys.stdout.write(key)
            sys.stdout.write("\t")
            sys.stdout.write(str(value))
            sys.stdout.write("\n")

    def geoff_insert(self, file):
        """ Insert Geoff data
        """
        params = geoff.Subgraph.load(open(file)).insert_into(self._graph_db)
        self._geoff_write(params)

    def geoff_merge(self, file):
        """ Merge Geoff data
        """
        params = geoff.Subgraph.load(open(file)).merge_into(self._graph_db)
        self._geoff_write(params)

    def xml_insert(self, file):
        """ Insert XML data
        """
        params = geoff.Subgraph.load_xml(open(file)).insert_into(self._graph_db)
        self._geoff_write(params)

    def xml_merge(self, file):
        """ Merge XML data
        """
        params = geoff.Subgraph.load_xml(open(file)).merge_into(self._graph_db)
        self._geoff_write(params)


if __name__ == "__main__":
    try:
        Tool().do(sys.argv)
        sys.exit(0)
    except Exception as err:
        sys.stderr.write(err.message)
        sys.stderr.write("\n")
        sys.exit(1)

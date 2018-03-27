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


from os.path import expanduser

from py2neo.meta import __version__


title = "Py2neo console v{}".format(__version__)
description = "Py2neo console is a Cypher runner and interactive tool for Neo4j."
history_file = expanduser("~/.py2neo_history")
quick_help = """\
  //  to enter multi-line mode (press [Alt]+[Enter] to run)
  /e  to launch external editor
  /?  for help
  /x  to exit\
"""
full_help = """\
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
Playback commands:
  /r FILE   load and run a Cypher file in a read transaction
  /w FILE   load and run a Cypher file in a write transaction

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
""".format(quick_help)

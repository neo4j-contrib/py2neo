#!/usr/bin/env python
# -*- encoding: utf-8 -*-

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


from codecs import open as codecs_open
from sys import exit


class Command(object):

    def __init__(self, env):
        self.env = env

    @classmethod
    def summary(cls):
        doc_string = cls.__doc__
        if doc_string is None:
            return ""
        doc_string = doc_string.strip()
        if not doc_string:
            return ""
        lines = doc_string.splitlines()
        return lines[0]

    def execute(self):
        """ Execute command.
        """


class RunCypherCommand(Command):
    """ Run Cypher.
    """

    def __init__(self, env, statement):
        super(RunCypherCommand, self).__init__(env)
        self.statement = statement

    def execute(self):
        result = self.env.run_cypher(self.statement)
        self.env.dump(result)
        plan = result.plan()
        if plan:
            self.env.write_metadata(repr(plan))


class PlayCypherCommand(RunCypherCommand):
    """ Load and run Cypher from a file.
    """

    def __init__(self, env, script):
        with codecs_open(script, encoding="utf-8") as fin:
            statement = fin.read()
        super(PlayCypherCommand, self).__init__(env, statement)


class ListConnectionsCommand(Command):

    def __init__(self, env):
        super(ListConnectionsCommand, self).__init__(env)

    def execute(self):
        self.env.list_connections()


class ConnectCommand(Command):

    def __init__(self, env, uri, user=None, password=None):
        super(ConnectCommand, self).__init__(env)
        self.uri = uri
        self.user = user
        self.password = password

    def execute(self):
        self.env.connect(self.uri, self.user, self.password)


class BeginTransactionCommand(Command):

    def __init__(self, env):
        super(BeginTransactionCommand, self).__init__(env)

    def execute(self):
        self.env.begin_transaction()


class CommitTransactionCommand(Command):

    def __init__(self, env):
        super(CommitTransactionCommand, self).__init__(env)

    def execute(self):
        self.env.commit_transaction()


class RollbackTransactionCommand(Command):

    def __init__(self, env):
        super(RollbackTransactionCommand, self).__init__(env)

    def execute(self):
        self.env.rollback_transaction()


class ExitCommand(Command):

    def __init__(self, env):
        super(ExitCommand, self).__init__(env)

    def execute(self):
        exit(0)


class PrintDBMSDetailsCommand(Command):

    def __init__(self, env):
        super(PrintDBMSDetailsCommand, self).__init__(env)

    def execute(self):
        self.env.print_dbms_details()


class PrintConfigCommand(Command):

    def __init__(self, env, *search_terms):
        super(PrintConfigCommand, self).__init__(env)
        self.search_terms = search_terms

    def execute(self):
        self.env.print_config(self.search_terms)


class ListParameterSetsCommand(Command):

    def __init__(self, env):
        super(ListParameterSetsCommand, self).__init__(env)

    def execute(self):
        self.env.list_parameter_sets()


class AppendParameterSetCommand(Command):

    def __init__(self, env, parameters):
        super(AppendParameterSetCommand, self).__init__(env)
        self.parameters = parameters

    def execute(self):
        self.env.append_parameter_set(self.parameters)


class ClearParameterSetsCommand(Command):

    def __init__(self, env):
        super(ClearParameterSetsCommand, self).__init__(env)

    def execute(self):
        self.env.clear_parameter_sets()

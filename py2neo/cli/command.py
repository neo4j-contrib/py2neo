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
from shlex import split as shlex_split
from sys import exit

try:
    from json import loads as json_loads, JSONDecodeError
except ImportError:
    from json import loads as json_loads
    JSONDecodeError = ValueError


class Command(object):

    name = None

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

    @classmethod
    def match(cls, line):
        tokens = shlex_split(line)
        first_token = tokens[0].lower()
        return first_token == cls.name

    @classmethod
    def instance(cls, env, line):
        tokens = shlex_split(line)
        return cls(env, *tokens[1:])

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

    name = "play"

    def __init__(self, env, script):
        with codecs_open(script, encoding="utf-8") as fin:
            statement = fin.read()
        super(PlayCypherCommand, self).__init__(env, statement)


class ShowServerDetailsCommand(Command):
    """ Show details of the remote server.
    """

    name = "server"

    def __init__(self, env):
        super(ShowServerDetailsCommand, self).__init__(env)

    def execute(self):
        self.env.show_server_details()


class ShowServerConfigCommand(Command):

    name = "config"

    def __init__(self, env, *search_terms):
        super(ShowServerConfigCommand, self).__init__(env)
        self.search_terms = search_terms

    def execute(self):
        self.env.show_config(self.search_terms)


class BeginTransactionCommand(Command):

    name = "begin"

    def __init__(self, env):
        super(BeginTransactionCommand, self).__init__(env)

    def execute(self):
        self.env.begin_transaction()


class CommitTransactionCommand(Command):

    name = "commit"

    def __init__(self, env):
        super(CommitTransactionCommand, self).__init__(env)

    def execute(self):
        self.env.commit_transaction()


class RollbackTransactionCommand(Command):

    name = "rollback"

    def __init__(self, env):
        super(RollbackTransactionCommand, self).__init__(env)

    def execute(self):
        self.env.rollback_transaction()


class ExitCommand(Command):

    name = "exit"

    def __init__(self, env):
        super(ExitCommand, self).__init__(env)

    def execute(self):
        exit(0)


class ListParameterSetsCommand(Command):

    name = "params"

    def __init__(self, env):
        super(ListParameterSetsCommand, self).__init__(env)

    def execute(self):
        self.env.list_parameter_sets()


class AppendParameterSetCommand(Command):

    name = "push"

    @classmethod
    def parse_parameter(cls, source):
        key, _, value = source.partition("=")
        try:
            value = json_loads(value)
        except JSONDecodeError:
            pass
        return key, value

    def __init__(self, env, *parameters):
        super(AppendParameterSetCommand, self).__init__(env)
        self.parameters = dict(self.parse_parameter(parameter) for parameter in parameters)

    def execute(self):
        self.env.append_parameter_set(**self.parameters)


class ClearParameterSetsCommand(Command):

    def __init__(self, env):
        super(ClearParameterSetsCommand, self).__init__(env)

    def execute(self):
        self.env.clear_parameter_sets()


class SetOutputFormatToHumanReadableCommand(Command):

    name = "human"

    def __init__(self, env):
        super(SetOutputFormatToHumanReadableCommand, self).__init__(env)

    def execute(self):
        self.env.output_format = None


class SetOutputFormatToJSONCommand(Command):

    name = "json"

    def __init__(self, env):
        super(SetOutputFormatToJSONCommand, self).__init__(env)

    def execute(self):
        self.env.output_format = "json"

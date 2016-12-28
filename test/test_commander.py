#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright 2011-2016, Nigel Small
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


from unittest import TestCase

try:
    from unittest.mock import MagicMock, call, patch
except ImportError:
    from mock import MagicMock, call, patch
from py2neo import Commander


class CommanderTestCase(TestCase):

    def test_simple_init_commander(self):
        from sys import stdout
        commander = Commander()
        self.assertEqual(commander.out, stdout)

    def test_write_to_out(self):
        from sys import stdout
        stdout.write = MagicMock()
        commander = Commander()
        commander.write('dummy')
        stdout.write.assert_called_once_with('dummy')

    def test_write_line(self):
        from sys import stdout
        from os import linesep
        stdout.write = MagicMock()
        commander = Commander()
        commander.write_line('dummy')
        calls = [call('dummy'), call(linesep)]
        stdout.write.assert_has_calls(calls)

    def test_usage_w_commader_class_docs(self):
        commander = Commander()
        commander.write_line = MagicMock()
        commander.usage('py2neo')
        calls = [
            call('usage: py2neo run [-h] [-H host] [-P port] statement\n      '
                 ' py2neo evaluate [-h] [-H host] [-P port] statement'),
            call(''),
            call('Report bugs to nigel@py2neo.org'),
        ]
        commander.write_line.assert_has_calls(calls)

    def test_usage_wo_commader_class_docs(self):
        commander = Commander()
        commander.__doc__ = None
        commander.write_line = MagicMock()
        commander.usage('script.py')
        commander.write_line.assert_called_once_with("usage: ?")

    def test_execute_with_script_name_arg(self):
        commander = Commander()
        commander.usage = MagicMock()
        commander.execute('script.py')
        commander.usage.assert_called_once_with('script.py')

    def test_execute_unknown_command(self):
        commander = Commander()
        commander.write_line = MagicMock()
        commander.execute('script.py', 'unknown_command', 'other')
        commander.write_line.assert_called_once_with(
            "Unknown command 'unknown_command'"
        )

    def test_execute_w_valid_command(self):
        commander = Commander()
        commander.valid_command = MagicMock()
        commander.execute('script.py', 'valid_command', 'other')
        commander.valid_command.assert_called_once_with(
            'valid_command', 'other'
        )

    def test_parser(self):
        from argparse import ArgumentParser
        commander = Commander()
        commander.epilog = 'epilog'
        script = 'script.py'
        parser = commander.parser(script)
        expected_parser = ArgumentParser(prog=script, epilog=commander.epilog)
        assert parser.prog == expected_parser.prog
        assert parser.epilog == expected_parser.epilog

    def test_parser_with_connection(self):
        commander = Commander()
        parser = commander.parser_with_connection('script.py')
        parsed = parser.parse_args(['--host', '127.0.0.1', '--port', '1234'])
        assert parsed.host == '127.0.0.1'
        assert parsed.port == 1234

    @patch('py2neo.DBMS.config')
    def test_config(self, dbms_config):
        dbms_config.return_value = {'dummy': 'value'}
        commander = Commander()
        commander.write_line = MagicMock()
        commander.config(['script.py', 'term', 'dummy'])
        commander.write_line.assert_called_once_with(
            "%s %s" % ("dummy".ljust(50), "value")
        )

    @patch('py2neo.DBMS.graph')
    def test_evaluate(self, dbms_graph):
        dbms_graph.evaluate = MagicMock(return_value=None)
        commander = Commander()
        commander.write_line = MagicMock()
        commander.evaluate('script.py', 'statement')
        dbms_graph.evaluate.assert_called_once_with('statement')
        commander.write_line.assert_called_once_with('None')

    @patch('py2neo.DBMS')
    def test_kernel_info(self, DBMS):
        dbms = DBMS()
        dbms.kernel_version = MagicMock(return_value='1.2.3')
        dbms.store_directory = MagicMock(return_value='/tmp/neo4j')
        dbms.store_id = MagicMock(return_value=121212121212)
        commander = Commander()
        commander.write_line = MagicMock()
        commander.kernel_info('script.py')
        calls = [
            call('Kernel version: 1.2.3'),
            call('Store directory: /tmp/neo4j'),
            call('Store ID: 121212121212')
        ]
        commander.write_line.assert_has_calls(calls)

    @patch('py2neo.DBMS')
    def test_store_file_sizes(self, DBMS):
        pass

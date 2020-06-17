#!/usr/bin/env python
# -*- encoding: utf-8 -*-

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


from io import StringIO

from pytest import fixture, raises

from py2neo.console import Console
from py2neo.meta import __version__


class CapturedConsole(Console):

    def __init__(self, *args, **kwargs):
        kwargs["file"] = StringIO()
        super(CapturedConsole, self).__init__(*args, **kwargs)
        self.captured_output = []
        self.scripted_input = []

    def echo(self, text, *args, **kwargs):
        self.captured_output.append(text)

    def prompt(self, *args, **kwargs):
        return self.scripted_input.pop(0)


@fixture()
def console(uri):
    return CapturedConsole(uri)


def assert_prologue(console):
    output = console.captured_output
    assert output.pop(0) == "Py2neo console v" + __version__
    assert output.pop(0) == "Connected to %s" % console.graph.service.uri
    assert output.pop(0) == ""
    assert output.pop(0) == ("//  to enter multi-line mode (press [Alt]+[Enter] to run)\n"
                             "/e  to launch external editor\n"
                             "/?  for help\n"
                             "/x  to exit")


def test_can_start_console(console):
    console.scripted_input = ["/x\n"]
    with raises(SystemExit):
        console.loop()
    assert_prologue(console)
    assert not console.captured_output


def test_can_run_query(console):
    console.scripted_input = ["RETURN 1 AS x\n", "/x\n"]
    with raises(SystemExit):
        console.loop()
    assert_prologue(console)
    assert console.output_file.getvalue() == " x \r\n---\r\n 1 \r\n"
    assert console.captured_output.pop(0) == "\r\n"
    assert console.captured_output.pop(0).startswith("(1 record from ")
    assert not console.captured_output

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


import atexit
from code import InteractiveConsole
import readline
import os.path

from py2neo import Graph


DEFAULT_BANNER = "Py2neo Console"
DEFAULT_EXIT_MESSAGE = "Tschüß!"
DEFAULT_HISTORY_FILE = os.path.expanduser("~/.py2neo_history")


class Console(InteractiveConsole):

    def __init__(self, hist_file=DEFAULT_HISTORY_FILE):
        super(Console, self).__init__()
        self.init_history(hist_file)
        self.graph = Graph(password="password")

    def init_history(self, history_file):
        readline.parse_and_bind("tab: complete")
        if hasattr(readline, "read_history_file"):
            try:
                readline.read_history_file(history_file)
            except IOError:
                pass
            atexit.register(self.save_history, history_file)

    def save_history(self, history_file):
        readline.set_history_length(1000)
        readline.write_history_file(history_file)

    def interact(self, banner=None, exitmsg=None):
        super(Console, self).interact(banner or DEFAULT_BANNER, exitmsg or DEFAULT_EXIT_MESSAGE)

    def push(self, line):
        self.graph.run(line).dump()
        return 0

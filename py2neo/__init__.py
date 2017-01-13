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


from py2neo.addressing import *
from py2neo.cypher import *
from py2neo.ext import *
from py2neo.graph import *
from py2neo.http import *
from py2neo.meta import *
from py2neo.selection import *
from py2neo.types import *

from neo4j.util import Watcher


def watch(logger, level=None, out=None):
    """ Dump log messages to standard output.

    To watch Bolt traffic::

        >>> from py2neo import watch
        >>> watch("neo4j.bolt")

    To watch HTTP traffic::

        >>> from py2neo import watch
        >>> watch("neo4j.http")

    :param logger: logger name
    :param level: logging level (default ``INFO``)
    :param out: output channel (default ``stdout``)
    """
    if logger == "neo4j.http":
        logger = "urllib3"
    if level is None:
        from logging import INFO
        level = INFO
    if out is None:
        from sys import stdout
        out = stdout
    watcher = Watcher(logger)
    watcher.watch(level, out)

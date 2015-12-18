#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright 2011-2014, Nigel Small
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
__version__ = "2.1.0"

PRODUCT = ("py2neo", __version__)


from py2neo.primitive import Subgraph, TraversableSubgraph, traverse
from py2neo.core import *
from py2neo.env import *
from py2neo.http import *
from py2neo.status import BindError, Finished, GraphError
from py2neo.ext import *
from py2neo.packages.httpstream.watch import watch


__all__ = ["DBMS", "Graph",
           "Subgraph", "TraversableSubgraph", "traverse",
           "Node", "Relationship", "Path",
           "authenticate", "rewrite", "watch",
           "BindError", "Finished", "GraphError",
           "ServerPlugin", "UnmanagedExtension",
           "Entity", "Resource", "ResourceTemplate",
           "NEO4J_AUTH", "NEO4J_DIST", "NEO4J_HOME", "NEO4J_URI",
           "graphy", "node", "relationship"]

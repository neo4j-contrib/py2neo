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
__copyright__ = "2011-2014, Nigel Small"
__email__ = "nigel@py2neo.org"
__license__ = "Apache License, Version 2.0"
__package__ = "py2neo"
__version__ = "3.0.0"


from py2neo.core import *
from py2neo.env import *
from py2neo.error import *
from py2neo.packages.httpstream.watch import watch


__all__ = ["Graph", "Node", "Relationship", "Path", "NodePointer", "Rel", "Rev", "Subgraph",
           "ServiceRoot", "PropertySet", "LabelSet", "PropertyContainer",
           "authenticate", "familiar", "rewrite", "watch",
           "BindError", "Finished", "GraphError", "JoinError", "Unauthorized",
           "ServerPlugin", "UnmanagedExtension", "Service", "ResourceTemplate",
           "node", "rel",
           "NEO4J_AUTH", "NEO4J_DIST", "NEO4J_HOME", "NEO4J_HTTP_URI", "NEO4J_URI"]


node = Node.cast
rel = Relationship.cast

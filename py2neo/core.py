#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright 2011-2015, Nigel Small
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


from __future__ import division, unicode_literals

from py2neo import __version__, http


__all__ = ["Graph", "Node", "Relationship", "Path", "NodePointer", "Rel", "Rev", "Subgraph",
           "authenticate", "familiar",
           "ServerPlugin", "UnmanagedExtension"]


PRODUCT = ("py2neo", __version__)

authenticate = http.authenticate
familiar = http.familiar
Graph = http.Graph
Node = http.Node
NodePointer = http.NodePointer
Rel = http.Rel
Rev = http.Rev
Path = http.Path
Relationship = http.Relationship
Subgraph = http.Subgraph
ServerPlugin = http.ServerPlugin
UnmanagedExtension = http.UnmanagedExtension

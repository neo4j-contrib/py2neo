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


from py2neo.core import Node
from py2neo.cypher import DeleteStatement


def test_statement_representations_return_cypher(graph):
    node = Node()
    graph.create(node)
    statement = DeleteStatement(graph)
    statement.delete(node)
    assert node in statement
    s = 'MATCH (_0) WHERE id(_0)={_0}\nDELETE _0'
    assert statement.__repr__() == s
    assert statement.__str__() == s
    assert statement.__unicode__() == s

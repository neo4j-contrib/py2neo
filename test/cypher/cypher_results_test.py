#/usr/bin/env python
# -*- coding: utf-8 -*-

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


def test_can_output_correct_representation_with_one_row(graph):
    results = graph.cypher.execute("CREATE (a {name:'Alice',age:33}) RETURN a.name,a.age")
    representation = repr(results)
    assert representation == (" a.name | a.age \n"
                              "--------+-------\n"
                              " Alice  | 33    \n"
                              "(1 row)\n")


def test_can_output_correct_representation_with_no_rows(graph):
    alice = Node(name="Alice")
    graph.create(alice)
    results = graph.cypher.execute("START a=node({A}) MATCH (a)-[:KNOWS]->(x) RETURN x",
                                   {"A": alice})
    representation = repr(results)
    assert representation == (" x \n"
                              "---\n"
                              "(0 rows)\n")

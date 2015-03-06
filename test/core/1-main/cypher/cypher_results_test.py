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


from py2neo.cypher import RecordProducer, RecordList


def test_can_output_correct_representation_with_one_row(graph):
    results = graph.cypher.execute("CREATE (a {name:'Alice',age:33}) RETURN a.name,a.age")
    representation = repr(results)
    assert len(representation.splitlines()) == 3


def test_one_value_when_none_returned(graph):
    result = graph.cypher.execute("CREATE (a {name:'Alice',age:33})")
    value = result.one
    assert value is None


def test_one_value_in_result(graph):
    result = graph.cypher.execute("CREATE (a {name:'Alice',age:33}) RETURN a.name")
    value = result.one
    assert value == "Alice"


def test_one_record_in_result(graph):
    result = graph.cypher.execute("CREATE (a {name:'Alice',age:33}) RETURN a.name,a.age")
    value = result.one
    assert value == ("Alice", 33)


def test_one_from_record_with_zero_columns():
    producer = RecordProducer([])
    record = producer.produce([])
    record_list = RecordList([], [record])
    value = record_list.one
    assert value is None

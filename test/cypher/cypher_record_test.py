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


import sys

from py2neo.cypher.core import RecordProducer


def test_record_field_access(graph):
    statement = "CREATE (a {name:'Alice',age:33}) RETURN a,a.name as name,a.age as age"
    for record in graph.cypher.stream(statement):
        alice = record.values[0]
        assert record.values[1] == alice.properties["name"]
        assert record.values[2] == alice.properties["age"]
        assert record.get("name") == alice.properties["name"]
        assert record.get("age") == alice.properties["age"]


def test_record_representation(graph):
    statement = "CREATE (a {name:'Alice',age:33}) RETURN a,a.name,a.age"
    for record in graph.cypher.stream(statement):
        alice_id = record.values[0]._id
        if sys.version_info >= (3,):
            assert repr(record) == ("Record(columns=('a', 'a.name', 'a.age'), "
                                    "values=((n%s {age:33,name:\"Alice\"}), "
                                    "'Alice', 33))" % alice_id)
        else:
            assert repr(record) == ("Record(columns=(u'a', u'a.name', u'a.age'), "
                                    "values=((n%s {age:33,name:\"Alice\"}), "
                                    "u'Alice', 33))" % alice_id)


def test_producer_representation():
    producer = RecordProducer(["apple", "banana", "carrot"])
    assert repr(producer) == "RecordProducer(columns=('apple', 'banana', 'carrot'))"


def test_producer_length():
    producer = RecordProducer(["apple", "banana", "carrot"])
    assert len(producer) == 3


def test_producer_column_indexes():
    producer = RecordProducer(["apple", "banana", "carrot"])
    assert producer.column_indexes == {"apple": 0, "banana": 1, "carrot": 2}

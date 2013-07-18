#/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright 2011-2013, Nigel Small
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


from py2neo import neo4j, node


def test_can_add_labels_to_node():
    graph_db = neo4j.GraphDatabaseService()
    alice, = graph_db.create(node(name="Alice"))
    labels = alice.get_labels()
    assert labels == set()
    alice.add_labels("human")
    labels = alice.get_labels()
    assert len(labels) == 1
    assert labels == {"human"}
    alice.add_labels("female")
    labels = alice.get_labels()
    assert labels == {"human", "female"}
    assert labels != {"female"}
    alice.add_labels("human")
    labels = alice.get_labels()
    assert labels == {"human", "female"}


def test_cannot_add_labels_to_abstract_nodes():
    alice = node(name="Alice")
    try:
        alice.add_labels("human", "female")
    except TypeError:
        assert True
    else:
        assert False

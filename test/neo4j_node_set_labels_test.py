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


from py2neo import node


def test_can_set_labels_on_node(graph):
    if not graph.supports_node_labels:
        return
    graph.clear()
    alice, = graph.create(node(name="Alice"))
    alice.add_labels("human", "female")
    labels = alice.get_labels()
    assert len(labels) == 2
    assert labels == {"human", "female"}
    alice.set_labels("mystery", "badger")
    labels = alice.get_labels()
    assert labels == {"mystery", "badger"}
    assert labels != {"human", "female"}
    found = graph.find("badger")
    assert list(found) == [alice]
    found = graph.find("badger", "name", "Alice")
    assert list(found) == [alice]

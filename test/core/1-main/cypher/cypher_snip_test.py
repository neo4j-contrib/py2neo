#/usr/bin/env python
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


from py2neo.cypher.snip import MergeNode


def test_can_build_simple_merge_node_snip():
    snip = MergeNode("Person", "name", "Alice")
    assert snip.statement == "MERGE (a:Person {name:{V}})\nRETURN a"
    assert snip.parameters == {"V": "Alice"}


def test_can_build_merge_node_snip_without_property():
    snip = MergeNode("Person")
    assert snip.statement == "MERGE (a:Person)\nRETURN a"
    assert snip.parameters == {}


def test_can_build_merge_node_snip_with_extra_values():
    snip = MergeNode("Person", "name", "Alice").set("Employee", employee_id=1234)
    assert snip.statement == "MERGE (a:Person {name:{V}})\nSET a:Employee\nSET a={P}\nRETURN a"
    assert snip.parameters == {"V": "Alice", "P": {"employee_id": 1234, "name": "Alice"}}

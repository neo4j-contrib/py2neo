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


from py2neo.core import Node, Relationship, Rel, Rev, Path
from py2neo.cypher.lang import Representation


def test_can_write_simple_identifier():
    r = Representation()
    r.write_identifier("foo")
    written = repr(r)
    assert written == "foo"


def test_can_write_identifier_with_odd_chars():
    r = Representation()
    r.write_identifier("foo bar")
    written = repr(r)
    assert written == "`foo bar`"


def test_can_write_identifier_containing_back_ticks():
    r = Representation()
    r.write_identifier("foo `bar`")
    written = repr(r)
    assert written == "`foo ``bar```"


def test_cannot_write_empty_identifier():
    r = Representation()
    try:
        r.write_identifier("")
    except ValueError:
        assert True
    else:
        assert False


def test_cannot_write_none_identifier():
    r = Representation()
    try:
        r.write_identifier(None)
    except ValueError:
        assert True
    else:
        assert False


def test_can_write_simple_node():
    r = Representation()
    r.write(Node())
    written = repr(r)
    assert written == "()"


def test_can_write_node_with_labels():
    r = Representation()
    r.write(Node("Dark Brown", "Chicken"))
    written = repr(r)
    assert written == '(:Chicken:`Dark Brown`)'


def test_can_write_node_with_properties():
    r = Representation()
    r.write(Node(name="Gertrude", age=3))
    written = repr(r)
    assert written == '({age:3,name:"Gertrude"})'


def test_can_write_node_with_labels_and_properties():
    r = Representation()
    r.write(Node("Dark Brown", "Chicken", name="Gertrude", age=3))
    written = repr(r)
    assert written == '(:Chicken:`Dark Brown` {age:3,name:"Gertrude"})'


def test_can_write_simple_relationship():
    r = Representation()
    r.write(Relationship({}, "KNOWS", {}))
    written = repr(r)
    assert written == "()-[:KNOWS]->()"


def test_can_write_relationship_with_properties():
    r = Representation()
    r.write(Relationship(
        {"name": "Fred"}, ("LIVES WITH", {"place": "Bedrock"}), {"name": "Wilma"}))
    written = repr(r)
    assert written == '({name:"Fred"})-[:`LIVES WITH` {place:"Bedrock"}]->({name:"Wilma"})'


def test_can_write_simple_rel():
    r = Representation()
    r.write(Rel("KNOWS"))
    written = repr(r)
    assert written == "-[:KNOWS]->"


def test_can_write_simple_rev():
    r = Representation()
    r.write(Rev("KNOWS"))
    written = repr(r)
    assert written == "<-[:KNOWS]-"


def test_can_write_simple_path():
    r = Representation()
    r.write(Path({}, "LOVES", {}, Rev("HATES"), {}, "KNOWS", {}))
    written = repr(r)
    assert written == "()-[:LOVES]->()<-[:HATES]-()-[:KNOWS]->()"


def test_can_write_array():
    r = Representation()
    r.write([1, 1, 2, 3, 5, 8, 13])
    written = repr(r)
    assert written == "[1,1,2,3,5,8,13]"


def test_can_write_mapping():
    r = Representation()
    r.write({"one": "eins", "two": "zwei", "three": "drei"})
    written = repr(r)
    assert written == '{one:"eins",three:"drei",two:"zwei"}'


def test_writing_none_writes_nothing():
    r = Representation()
    r.write(None)
    written = repr(r)
    assert written == ""

#/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright 2011-2012 Nigel Small
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

__author__    = "Nigel Small <nasmall@gmail.com>"
__copyright__ = "Copyright 2011-2012 Nigel Small"
__license__   = "Apache License, Version 2.0"

import logging
import unittest

from py2neo import neometry

logging.basicConfig(
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
    level=logging.DEBUG,
)


class CreateTest(unittest.TestCase):

    def test_can_create_graph(self):
        graph = neometry.Graph()
        graph["Neo"] = {"name": "Neo"}
        graph["Morpheus"] = {"name": "Morpheus"}
        graph["Trinity"] = {"name": "Trinity"}
        graph["Cypher"] = {"name": "Cypher"}
        graph["Smith"] = {"name": "Agent Smith"}
        graph["Architect"] = {"name": "The Architect"}
        graph["Barney"] = {"name": "Barney the Dinosaur"}
        graph.relate("Neo", "KNOWS", "Morpheus")
        graph.relate("Neo", "KNOWS", "Trinity")
        graph.relate("Neo", "LOVES", "Trinity")
        graph.relate("Neo", "LOVES", "Trinity", {"amount": "lots"})
        graph.relate("Morpheus", "KNOWS", "Trinity")
        graph.relate("Morpheus", "KNOWS", "Cypher")
        graph.relate("Trinity", "KNOWS", "Cypher")
        graph.relate("Cypher", "KNOWS", "Smith")
        graph.relate("Smith", "CODED_BY", "Architect", 1337)
        graph.relate("Neo", "KILLS", "Smith")
        assert len(graph) == 7
        assert graph["Neo"] == {"name": "Neo"}
        assert graph.nodes() == {
            'Neo': {'name': 'Neo'},
            'Cypher': {'name': 'Cypher'},
            'Smith': {'name': 'Agent Smith'},
            'Morpheus': {'name': 'Morpheus'},
            'Barney': {'name': 'Barney the Dinosaur'},
            'Architect': {'name': 'The Architect'},
            'Trinity': {'name': 'Trinity'}
        }
        assert graph.nodes(value={'name': 'Trinity'}) == {'Trinity': {'name': 'Trinity'}}
        assert graph.edges() == {
            ('Neo', 'KNOWS', 'Morpheus'): None,
            ('Neo', 'KILLS', 'Smith'): None,
            ('Morpheus', 'KNOWS', 'Cypher'): None,
            ('Neo', 'KNOWS', 'Trinity'): None,
            ('Smith', 'CODED_BY', 'Architect'): 1337,
            ('Morpheus', 'KNOWS', 'Trinity'): None,
            ('Neo', 'LOVES', 'Trinity'): {'amount': 'lots'},
            ('Trinity', 'KNOWS', 'Cypher'): None,
            ('Cypher', 'KNOWS', 'Smith'): None,
        }


class PathTest(unittest.TestCase):

    def setUp(self):
        self.graph = neometry.Graph()
        self.graph["foo"] = "bar"
        self.graph["Neo"] = {"name": "Neo"}
        self.graph["Morpheus"] = {"name": "Morpheus"}
        self.graph["Trinity"] = {"name": "Trinity"}
        self.graph["Cypher"] = {"name": "Cypher"}
        self.graph["Smith"] = {"name": "Agent Smith"}
        self.graph["Architect"] = {"name": "The Architect"}
        self.graph["Barney"] = {"name": "Barney the Dinosaur"}
        self.graph.relate("Neo", "KNOWS", "Morpheus")
        self.graph.relate("Neo", "KNOWS", "Trinity")
        self.graph.relate("Neo", "LOVES", "Trinity")
        self.graph.relate("Neo", "LOVES", "Trinity", {"amount": "lots"})
        self.graph.relate("Morpheus", "KNOWS", "Trinity")
        self.graph.relate("Morpheus", "KNOWS", "Cypher", 1337)
        self.graph.relate("Trinity", "KNOWS", "Cypher")
        self.graph.relate("Cypher", "KNOWS", "Smith")
        self.graph.relate("Smith", "CODED_BY", "Architect", 1337)
        self.graph.relate("Neo", "KILLS", "Smith", {})

    def test_can_remove_rels(self):
        self.graph.relate("Trinity", "LIKES", "Barney")
        self.graph.relate("Morpheus", "LIKES", "Barney")
        assert self.graph.edges(relationship="LIKES") == {('Trinity', 'LIKES', 'Barney'): None, ('Morpheus', 'LIKES', 'Barney'): None}
        self.graph.unrelate(relationship="LIKES", end="Barney")
        assert self.graph.edges(relationship="LIKES", end="Barney") == {}

    def test_can_query_rels(self):
        assert self.graph.edges(start="Neo") == {('Neo', 'LOVES', 'Trinity'): {'amount': 'lots'}, ('Neo', 'KNOWS', 'Morpheus'): None, ('Neo', 'KILLS', 'Smith'): {}, ('Neo', 'KNOWS', 'Trinity'): None}
        assert self.graph.edges(start="Neo", end="Trinity") == {('Neo', 'LOVES', 'Trinity'): {'amount': 'lots'}, ('Neo', 'KNOWS', 'Trinity'): None}
        assert self.graph.edges(start="Neo", relationship="KNOWS") == {('Neo', 'KNOWS', 'Morpheus'): None, ('Neo', 'KNOWS', 'Trinity'): None}
        assert self.graph.edges(start="Neo", relationship="LOVES", end="Trinity") == {('Neo', 'LOVES', 'Trinity'): {'amount': 'lots'}}
        assert self.graph.edges(end="Trinity") == {('Neo', 'LOVES', 'Trinity'): {'amount': 'lots'}, ('Morpheus', 'KNOWS', 'Trinity'): None, ('Neo', 'KNOWS', 'Trinity'): None}
        assert self.graph.edges(relationship="KNOWS", end="Trinity") == {('Morpheus', 'KNOWS', 'Trinity'): None, ('Neo', 'KNOWS', 'Trinity'): None}
        assert self.graph.edges(relationship="CODED_BY") == {('Smith', 'CODED_BY', 'Architect'): 1337}
        assert self.graph.edges(value=1337) == {('Smith', 'CODED_BY', 'Architect'): 1337, ('Morpheus', 'KNOWS', 'Cypher'): 1337}
        assert self.graph.edges(value={}) == {('Neo', 'KILLS', 'Smith'): {}}


class BigTest(unittest.TestCase):

    def test_big_graph(self):
        graph = neometry.Graph()
        graph["root"] = None
        for i in range(10000):
            graph[i] = i
            graph.relate("root", "NUMBER", i)
        assert len(graph) == 10001
        assert len(graph.edges(relationship="NUMBER")) == 10000


class PathTestCase(unittest.TestCase):

    def test_can_create_path(self):
        path = neometry.Path({"name": "Alice"}, "KNOWS", {"name": "Bob"})
        assert len(path) == 1
        assert path.nodes[0]["name"] == "Alice"
        assert path.edges[0] == "KNOWS"
        assert path.nodes[-1]["name"] == "Bob"
        path = neometry.Path.join(path, "KNOWS", {"name": "Carol"})
        assert len(path) == 2
        assert path.nodes[0]["name"] == "Alice"
        assert path.edges[0] == "KNOWS"
        assert path.nodes[1]["name"] == "Bob"
        path = neometry.Path.join({"name": "Zach"}, "KNOWS", path)
        assert len(path) == 3
        assert path.nodes[0]["name"] == "Zach"
        assert path.edges[0] == "KNOWS"
        assert path.nodes[1]["name"] == "Alice"
        assert path.edges[1] == "KNOWS"
        assert path.nodes[2]["name"] == "Bob"

    def test_can_slice_path(self):
        path = neometry.Path({"name": "Alice"},
            "KNOWS", {"name": "Bob"},
            "KNOWS", {"name": "Carol"},
            "KNOWS", {"name": "Dave"},
            "KNOWS", {"name": "Eve"},
            "KNOWS", {"name": "Frank"},
        )
        assert len(path) == 5
        assert path[0] == neometry.Path({"name": "Alice"}, "KNOWS", {"name": "Bob"})
        assert path[1] == neometry.Path({"name": "Bob"}, "KNOWS", {"name": "Carol"})
        assert path[2] == neometry.Path({"name": "Carol"}, "KNOWS", {"name": "Dave"})
        assert path[-1] == neometry.Path({"name": "Eve"}, "KNOWS", {"name": "Frank"})
        assert path[0:2] == neometry.Path({"name": "Alice"}, "KNOWS", {"name": "Bob"}, "KNOWS", {"name": "Carol"})
        assert path[3:5] == neometry.Path({"name": "Dave"}, "KNOWS", {"name": "Eve"}, "KNOWS", {"name": "Frank"})
        assert path[:] == neometry.Path({"name": "Alice"}, "KNOWS", {"name": "Bob"}, "KNOWS", {"name": "Carol"}, "KNOWS", {"name": "Dave"}, "KNOWS", {"name": "Eve"}, "KNOWS", {"name": "Frank"})

    def test_can_iterate_path(self):
        path = neometry.Path({"name": "Alice"},
            "KNOWS", {"name": "Bob"},
            "KNOWS", {"name": "Carol"},
            "KNOWS", {"name": "Dave"},
            "KNOWS", {"name": "Eve"},
            "KNOWS", {"name": "Frank"},
        )
        assert list(iter(path)) == [
            ({'name': 'Alice'}, 'KNOWS', {'name': 'Bob'}),
            ({'name': 'Bob'}, 'KNOWS', {'name': 'Carol'}),
            ({'name': 'Carol'}, 'KNOWS', {'name': 'Dave'}),
            ({'name': 'Dave'}, 'KNOWS', {'name': 'Eve'}),
            ({'name': 'Eve'}, 'KNOWS', {'name': 'Frank'}),
        ]
        assert list(enumerate(path)) == [
            (0, ({'name': 'Alice'}, 'KNOWS', {'name': 'Bob'})),
            (1, ({'name': 'Bob'}, 'KNOWS', {'name': 'Carol'})),
            (2, ({'name': 'Carol'}, 'KNOWS', {'name': 'Dave'})),
            (3, ({'name': 'Dave'}, 'KNOWS', {'name': 'Eve'})),
            (4, ({'name': 'Eve'}, 'KNOWS', {'name': 'Frank'}))
        ]

    def test_can_join_paths(self):
        path1 = neometry.Path({"name": "Alice"}, "KNOWS", {"name": "Bob"})
        path2 = neometry.Path({"name": "Carol"}, "KNOWS", {"name": "Dave"})
        path = neometry.Path.join(path1, "KNOWS", path2)
        assert list(iter(path)) == [
            ({'name': 'Alice'}, 'KNOWS', {'name': 'Bob'}),
            ({'name': 'Bob'}, 'KNOWS', {'name': 'Carol'}),
            ({'name': 'Carol'}, 'KNOWS', {'name': 'Dave'}),
        ]

    def test_path_representation(self):
        path = neometry.Path({"name": "Alice"}, "KNOWS", {"name": "Bob"})
        print str(path)
        assert str(path) == "{'name': 'Alice'}-KNOWS->{'name': 'Bob'}"
        print repr(path)
        assert repr(path) == "Path({'name': 'Alice'}, 'KNOWS', {'name': 'Bob'})"


if __name__ == '__main__':
    unittest.main()


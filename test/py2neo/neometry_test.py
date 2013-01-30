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


class PathTestCase(unittest.TestCase):

    def test_can_create_path(self):
        path = neometry.Path({"name": "Alice"}, "KNOWS", {"name": "Bob"})
        assert len(path) == 1
        assert path.nodes[0]["name"] == "Alice"
        assert path.relationships[0] == "KNOWS"
        assert path.nodes[-1]["name"] == "Bob"
        path = neometry.Path.join(path, "KNOWS", {"name": "Carol"})
        assert len(path) == 2
        assert path.nodes[0]["name"] == "Alice"
        assert path.relationships[0] == "KNOWS"
        assert path.nodes[1]["name"] == "Bob"
        path = neometry.Path.join({"name": "Zach"}, "KNOWS", path)
        assert len(path) == 3
        assert path.nodes[0]["name"] == "Zach"
        assert path.relationships[0] == "KNOWS"
        assert path.nodes[1]["name"] == "Alice"
        assert path.relationships[1] == "KNOWS"
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
        print(str(path))
        assert str(path) == "{'name': 'Alice'}-KNOWS->{'name': 'Bob'}"
        print(repr(path))
        assert repr(path) == "Path({'name': 'Alice'}, 'KNOWS', {'name': 'Bob'})"


if __name__ == '__main__':
    unittest.main()


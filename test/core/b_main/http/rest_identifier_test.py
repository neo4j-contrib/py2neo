#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright 2011-2015, Nigel Small
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


from unittest import TestCase, main

from py2neo.http.rest import Identifier


class IdentifierTestCase(TestCase):

    def test_can_parse_root_uri(self):
        identifier = Identifier("http://localhost:7474/")
        assert vars(identifier) == {
            "uri": "http://localhost:7474/",
            "auth": None,
            "host_port": "localhost:7474",
            "root_uri": "http://localhost:7474/",
            "graph_uri": "http://localhost:7474/db/data/",
            "ref": "",
            "name": "",
        }

    def test_can_parse_graph_uri(self):
        identifier = Identifier("http://localhost:7474/db/data/")
        assert vars(identifier) == {
            "uri": "http://localhost:7474/db/data/",
            "auth": None,
            "host_port": "localhost:7474",
            "root_uri": "http://localhost:7474/",
            "graph_uri": "http://localhost:7474/db/data/",
            "ref": "",
            "name": "",
        }

    def test_can_parse_graph_uri_with_auth(self):
        identifier = Identifier("http://neo4j:password@localhost:7474/db/data/")
        assert vars(identifier) == {
            "uri": "http://localhost:7474/db/data/",
            "auth": ("neo4j", "password"),
            "host_port": "localhost:7474",
            "root_uri": "http://localhost:7474/",
            "graph_uri": "http://localhost:7474/db/data/",
            "ref": "",
            "name": "",
        }

    def test_can_parse_node_uri(self):
        identifier = Identifier("http://localhost:7474/db/data/node/1")
        assert vars(identifier) == {
            "uri": "http://localhost:7474/db/data/node/1",
            "auth": None,
            "host_port": "localhost:7474",
            "root_uri": "http://localhost:7474/",
            "graph_uri": "http://localhost:7474/db/data/",
            "ref": "node/1",
            "name": "1",
        }

    def test_can_parse_relationship_uri(self):
        identifier = Identifier("http://localhost:7474/db/data/relationship/1")
        assert vars(identifier) == {
            "uri": "http://localhost:7474/db/data/relationship/1",
            "auth": None,
            "host_port": "localhost:7474",
            "root_uri": "http://localhost:7474/",
            "graph_uri": "http://localhost:7474/db/data/",
            "ref": "relationship/1",
            "name": "1",
        }


if __name__ == "__main__":
    main()

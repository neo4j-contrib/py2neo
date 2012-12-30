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

import sys
import time
from py2neo.util import PropertyCache

PY3K = sys.version_info[0] >= 3

__author__    = "Nigel Small <nasmall@gmail.com>"
__copyright__ = "Copyright 2011-2012 Nigel Small"
__license__   = "Apache License, Version 2.0"


from py2neo import rest, neo4j
import unittest


class PropertyCacheTest(unittest.TestCase):

    def setUp(self):
        pass

    def test_simple_property_cache(self):
        props = PropertyCache()
        props.update({
            "foo": "bar",
            "number": 42
        })
        self.assertEqual("bar", props["foo"])
        self.assertEqual(42, props["number"])

    def test_none_property_cache(self):
        props = PropertyCache(None)
        props.update({
            "foo": "bar",
            "number": 42
        })
        self.assertEqual("bar", props["foo"])
        self.assertEqual(42, props["number"])

    def test_populated_property_cache(self):
        props = PropertyCache({
            "foo": "bar",
            "number": 42
        })
        self.assertEqual("bar", props["foo"])
        self.assertEqual(42, props["number"])

    def test_property_cache_with_expiry(self):
        props = PropertyCache({
            "foo": "bar",
            "number": 42
        }, max_age=3)
        self.assertFalse(props.expired)
        self.assertEqual("bar", props["foo"])
        self.assertEqual(42, props["number"])
        time.sleep(3)
        self.assertTrue(props.expired)
        self.assertEqual("bar", props["foo"])
        self.assertEqual(42, props["number"])


class MetadataTest(unittest.TestCase):

    def test_can_detect_graph_database_service(self):
        uri = "test:uri"
        metadata = {
          "extensions" : {
            "CypherPlugin" : {
              "execute_query" : "http://localhost:7474/db/data/ext/CypherPlugin/graphdb/execute_query"
            },
            "GremlinPlugin" : {
              "execute_script" : "http://localhost:7474/db/data/ext/GremlinPlugin/graphdb/execute_script"
            },
            "GeoffPlugin" : {
              "merge" : "http://localhost:7474/db/data/ext/GeoffPlugin/graphdb/merge",
              "delete" : "http://localhost:7474/db/data/ext/GeoffPlugin/graphdb/delete",
              "insert" : "http://localhost:7474/db/data/ext/GeoffPlugin/graphdb/insert"
            }
          },
          "node" : "http://localhost:7474/db/data/node",
          "node_index" : "http://localhost:7474/db/data/index/node",
          "relationship_index" : "http://localhost:7474/db/data/index/relationship",
          "extensions_info" : "http://localhost:7474/db/data/ext",
          "relationship_types" : "http://localhost:7474/db/data/relationship/types",
          "batch" : "http://localhost:7474/db/data/batch",
          "cypher" : "http://localhost:7474/db/data/cypher",
          "neo4j_version" : "1.8.M07-147-g84d07a2"
        }
        neo4j._assert_expected_response(neo4j.GraphDatabaseService, uri, metadata)

    def test_can_detect_node(self):
        uri = "test:uri"
        metadata = {
          "extensions" : {
          },
          "paged_traverse" : "http://localhost:7474/db/data/node/2757/paged/traverse/{returnType}{?pageSize,leaseTime}",
          "outgoing_relationships" : "http://localhost:7474/db/data/node/2757/relationships/out",
          "traverse" : "http://localhost:7474/db/data/node/2757/traverse/{returnType}",
          "all_typed_relationships" : "http://localhost:7474/db/data/node/2757/relationships/all/{-list|&|types}",
          "property" : "http://localhost:7474/db/data/node/2757/properties/{key}",
          "all_relationships" : "http://localhost:7474/db/data/node/2757/relationships/all",
          "self" : "http://localhost:7474/db/data/node/2757",
          "properties" : "http://localhost:7474/db/data/node/2757/properties",
          "outgoing_typed_relationships" : "http://localhost:7474/db/data/node/2757/relationships/out/{-list|&|types}",
          "incoming_relationships" : "http://localhost:7474/db/data/node/2757/relationships/in",
          "incoming_typed_relationships" : "http://localhost:7474/db/data/node/2757/relationships/in/{-list|&|types}",
          "create_relationship" : "http://localhost:7474/db/data/node/2757/relationships",
          "data" : {
            "foo" : "bar"
          }
        }
        neo4j._assert_expected_response(neo4j.Node, uri, metadata)

    def test_can_detect_relationship(self):
        uri = "test:uri"
        metadata = {
          "extensions" : {
          },
          "start" : "http://localhost:7474/db/data/node/2757",
          "property" : "http://localhost:7474/db/data/relationship/7598/properties/{key}",
          "self" : "http://localhost:7474/db/data/relationship/7598",
          "properties" : "http://localhost:7474/db/data/relationship/7598/properties",
          "type" : "ALIAS_TEST_NODE",
          "end" : "http://localhost:7474/db/data/node/2758",
          "data" : {
            "foo" : "bar"
          }
        }
        neo4j._assert_expected_response(neo4j.Relationship, uri, metadata)


if __name__ == '__main__':
    unittest.main()


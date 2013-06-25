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

import sys
PY3K = sys.version_info[0] >= 3

from py2neo import neo4j

import logging
import unittest

logging.basicConfig(
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
    level=logging.DEBUG,
)


class TestBigBatches(unittest.TestCase):

    def _send_big_batch(self, node_count):
        graph_db = neo4j.GraphDatabaseService()
        graph_db.clear()
        print("creating batch of " + str(node_count))
        batch = neo4j.WriteBatch(graph_db)
        for i in range(node_count):
            batch.create_node({"number": i})
        print("submitting batch")
        nodes = batch.submit()
        print("checking batch")
        for node in nodes:
            assert isinstance(node, neo4j.Node)
        print("done")

    def test_can_send_batch_of_100(self):
        self._send_big_batch(100)

    def test_can_send_batch_of_1000(self):
        self._send_big_batch(1000)

#    def test_can_send_batch_of_10000(self):
#        self._send_big_batch(10000)

#    def test_can_send_batch_of_100000(self):
#        self._send_big_batch(100000)


if __name__ == "__main__":
    unittest.main()

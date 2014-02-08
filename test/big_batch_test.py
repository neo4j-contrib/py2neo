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


from py2neo import neo4j

import logging


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")

graph_db = neo4j.GraphDatabaseService()


def _execute_batch(node_count):
    batch = neo4j.WriteBatch(graph_db)
    for i in range(node_count):
        batch.create({"number": i})
    batch.run()


#def test_can_send_batch_of_100():
#    _send_big_batch(100)


def test_can_execute_4_batches_of_300():
    graph_db.clear()
    for i in range(4):
        _execute_batch(300)


#def test_can_send_batch_of_1000():
#    _send_big_batch(1000)


#    def test_can_send_batch_of_10000():
#        _send_big_batch(10000)


#    def test_can_send_batch_of_100000():
#        _send_big_batch(100000)

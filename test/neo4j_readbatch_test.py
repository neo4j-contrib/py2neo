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

import unittest

from py2neo import neo4j


class GetIndexedNodeTestCase(unittest.TestCase):

    def setUp(self):
        self.graph_db = neo4j.GraphDatabaseService()
        self.graph_db.clear()

    def test_can_retrieve_multiple_indexed_nodes(self):
        people = self.graph_db.get_or_create_index(neo4j.Node, "People")
        alice, bob, carol, dave, eve = self.graph_db.create(
            {"name": "Alice Smith"},
            {"name": "Bob Smith"},
            {"name": "Carol Smith"},
            {"name": "Dave Jones"},
            {"name": "Eve Jones"},
        )
        people.add("family_name", "Smith", alice)
        people.add("family_name", "Smith", bob)
        people.add("family_name", "Smith", carol)
        people.add("family_name", "Jones", dave)
        people.add("family_name", "Jones", eve)
        batch = neo4j.ReadBatch(self.graph_db)
        batch.get_indexed_nodes("People", "family_name", "Smith")
        batch.get_indexed_nodes("People", "family_name", "Jones")
        data = batch.submit()
        smiths = data[0]
        joneses = data[1]
        assert smiths == [alice, bob, carol]
        assert joneses == [dave, eve]


if __name__ == "__main__":
    unittest.main()

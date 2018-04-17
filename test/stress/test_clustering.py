#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright 2011-2018, Nigel Small
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


from unittest import TestCase
from uuid import uuid4

from py2neo.admin.clustering import LocalCluster
from py2neo.admin.install import Warehouse
from py2neo.database import Graph
from py2neo.watcher import watch


class LocalClusterTestCase(TestCase):

    def setUp(self):
        self.warehouse = Warehouse()

    def test_can_run_start_and_stop_local_cluster(self):
        watch("neo4j.bolt")
        name = uuid4().hex
        cluster = LocalCluster.install(self.warehouse, name, "3.4", alpha=(3, 1))
        try:
            cluster.update_auth("neo4j", "password")
            cluster.start()
            self.assertTrue(cluster.running())
            uris = cluster.bolt_routing_uris
            self.assertEqual(uris, {"bolt+routing://localhost:17100",
                                    "bolt+routing://localhost:17101",
                                    "bolt+routing://localhost:17102"})
            g = Graph(uris.pop(), auth=("neo4j", "password"))
            overview = g.run("CALL dbms.cluster.overview").data()
            roles = {}
            for member in overview:
                self.assertEqual(len(member["addresses"]), 3)
                role = member["role"]
                if role in roles:
                    roles[role] += 1
                else:
                    roles[role] = 1
                self.assertEqual(member["database"], "alpha")
            self.assertEqual(roles, {"LEADER": 1, "FOLLOWER": 2, "READ_REPLICA": 1})
            cluster.stop()
        finally:
            cluster.uninstall()

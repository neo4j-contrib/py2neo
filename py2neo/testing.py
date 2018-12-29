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


from os import getenv
from unittest import TestCase

from neobolt.exceptions import ServiceUnavailable

from py2neo.internal.connectors import Connector


class ClusterIntegrationTestCase(TestCase):

    cluster = None

    server_version = "3.4.10"

    @classmethod
    def setUpClass(cls):
        cls.bolt_uri = getenv("PY2NEO_BOLT_URI", "bolt+routing://:17100")
        cls.bolt_routing_uri = getenv("PY2NEO_BOLT_ROUTING_URI", "bolt+routing://:17100")
        cx = Connector(cls.bolt_routing_uri, auth=("neo4j", "password"))
        try:
            cls.server_agent = cx.server_agent
        except ServiceUnavailable:
            from py2neo.admin.install import Warehouse
            from py2neo.experimental.clustering import LocalCluster
            cls.cluster = LocalCluster.install(Warehouse(), "test", cls.server_version, alpha=3)
            cls.cluster.update_auth("neo4j", "password")
            cls.cluster.start()
            cls.server_agent = cx.server_agent

    @classmethod
    def tearDownClass(cls):
        if cls.cluster:
            cls.cluster.stop()
            cls.cluster.uninstall()
            cls.cluster = None

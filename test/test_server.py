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


from unittest import TestCase

from py2neo.server import dist_name, dist_archive_name, download
from test.compat import patch
from py2neo.packages import httpstream


class DistFunctionTestCase(TestCase):

    def test_dist_name(self):
        name = dist_name("community", "3.0.0")
        assert name == "neo4j-community-3.0.0"

    def test_dist_archive_name(self):
        name = dist_archive_name("community", "3.0.0")
        assert name == "neo4j-community-3.0.0-unix.tar.gz"

    def test_download(self):
        with patch.object(httpstream, "download") as mocked:
            download("community", "3.0.0", "/tmp")
            url = "http://dist.neo4j.org/neo4j-community-3.0.0-unix.tar.gz"
            file = "/tmp/neo4j-community-3.0.0-unix.tar.gz"
            mocked.assert_called_once_with(url, file)

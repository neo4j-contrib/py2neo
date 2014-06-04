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

import os
import sys

from py2neo import geoff

FIXTURES = os.path.join(os.path.dirname(__file__), "files")


class TestXMLTestCase(object):

    def test_can_create_subgraph_from_xml(self):
        xml_file = os.path.join(FIXTURES, "planets.xml")
        geoff_file = os.path.join(FIXTURES, "planets.geoff")
        planets = geoff.Subgraph.load_xml(open(xml_file))
        if sys.version_info >= (2, 7):
            assert planets.source == open(geoff_file).read().strip()

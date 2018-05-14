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


from unittest import SkipTest

from py2neo.testing import IntegrationTestCase


class TemporalTypeOutputTestCase(IntegrationTestCase):

    def test_date(self):
        if self.graph.database.kernel_version < (3, 4):
            raise SkipTest()
        date = self.graph.evaluate("RETURN date('1976-06-13')")
        self.assertEqual(date.year, 1976)
        self.assertEqual(date.month, 6)
        self.assertEqual(date.day, 13)

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


from test.util import GraphTestCase


class DataTableTestCase(GraphTestCase):

    def test_simple_usage(self):
        table = self.graph.data("UNWIND range(1, 3) AS n RETURN n, n * n AS n_sq")
        self.assertEqual(table.keys(), ["n", "n_sq"])
        name_field = table.field(0)
        self.assertEqual(name_field["type"], int)
        self.assertEqual(name_field["optional"], False)
        age_field = table.field(1)
        self.assertEqual(age_field["type"], int)
        self.assertEqual(age_field["optional"], False)

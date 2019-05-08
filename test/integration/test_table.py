#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright 2011-2019, Nigel Small
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


def test_simple_usage(graph):
    table = graph.run("UNWIND range(1, 3) AS n RETURN n, n * n AS n_sq").to_table()
    assert table.keys() == ["n", "n_sq"]
    name_field = table.field(0)
    assert name_field["type"] is int
    assert name_field["optional"] is False
    age_field = table.field(1)
    assert age_field["type"] is int
    assert age_field["optional"] is False


def test_html(graph):
    table = graph.run("RETURN 'number' AS Number, 1 AS Value").to_table()
    html = table._repr_html_()
    assert html == ('<table><tr><th>Number</th><th>Value</th></tr>'
                    '<tr><td style="text-align:left">number</td>'
                    '<td style="text-align:right">1</td></tr></table>')

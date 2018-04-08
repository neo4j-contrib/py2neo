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


from io import StringIO
from unittest import TestCase

from py2neo.data import DataList


class DataListTestCase(TestCase):

    def test_simple_usage(self):
        table = DataList([
            ["Alice", 33],
            ["Bob", 44],
            ["Carol", 55],
            ["Dave", 66],
        ], keys=["name", "age"])
        self.assertEqual(table.keys(), ["name", "age"])
        name_field = table.field(0)
        self.assertEqual(name_field["type"], str)
        self.assertEqual(name_field["optional"], False)
        age_field = table.field(1)
        self.assertEqual(age_field["type"], int)
        self.assertEqual(age_field["optional"], False)

    def test_missing_keys(self):
        with self.assertRaises(ValueError):
            _ = DataList([
                ["Alice", 33],
                ["Bob", 44],
                ["Carol", 55],
                ["Dave", 66],
            ])

    def test_optional_fields(self):
        table = DataList([
            ["Alice", 33],
            ["Bob", 44],
            ["Carol", None],
            [None, 66],
        ], keys=["name", "age"])
        self.assertEqual(table.keys(), ["name", "age"])
        name_field = table.field(0)
        self.assertEqual(name_field["type"], str)
        self.assertEqual(name_field["optional"], True)
        age_field = table.field(1)
        self.assertEqual(age_field["type"], int)
        self.assertEqual(age_field["optional"], True)

    def test_mixed_types(self):
        table = DataList([
            ["Alice", 33],
            ["Bob", 44],
            ["Carol", 55.5],
            ["Dave", 66.6],
        ], keys=["name", "age"])
        self.assertEqual(table.keys(), ["name", "age"])
        name_field = table.field(0)
        self.assertEqual(name_field["type"], str)
        self.assertEqual(name_field["optional"], False)
        age_field = table.field(1)
        self.assertEqual(set(age_field["type"]), {int, float})
        self.assertEqual(age_field["optional"], False)

    def test_fields_by_name_usage(self):
        table = DataList([
            ["Alice", 33],
            ["Bob", 44],
            ["Carol", 55],
            ["Dave", 66],
        ], keys=["name", "age"])
        self.assertEqual(table.keys(), ["name", "age"])
        name_field = table.field("name")
        self.assertEqual(name_field["type"], str)
        self.assertEqual(name_field["optional"], False)
        age_field = table.field("age")
        self.assertEqual(age_field["type"], int)
        self.assertEqual(age_field["optional"], False)
        with self.assertRaises(KeyError):
            _ = table.field("gender")

    def test_bad_typed_field_selector(self):
        table = DataList([
            ["Alice", 33],
            ["Bob", 44],
            ["Carol", 55],
            ["Dave", 66],
        ], keys=["name", "age"])
        with self.assertRaises(TypeError):
            _ = table.field(object)

    def test_repr(self):
        table = DataList([
            ["Alice", 33],
            ["Bob", 44],
            ["Carol", 55],
            ["Dave", 66],
        ], keys=["name", "age"])
        out = repr(table)
        self.assertEqual(out, ' name  | age \r\n'
                              '-------------\r\n'
                              ' Alice |  33 \r\n'
                              ' Bob   |  44 \r\n'
                              ' Carol |  55 \r\n'
                              ' Dave  |  66 \r\n')

    def test_write(self):
        table = DataList([
            ["Alice", 33],
            ["Bob", 44],
            ["Carol", 55],
            ["Dave", 66],
        ], keys=["name", "age"])
        out = StringIO()
        table.write(out)
        self.assertEqual(out.getvalue(), ' Alice | 33 \r\n'
                                         ' Bob   | 44 \r\n'
                                         ' Carol | 55 \r\n'
                                         ' Dave  | 66 \r\n')

    def test_write_with_newline_in_value(self):
        table = DataList([
            ["Alice\nSmith", 33],
            ["Bob", 44],
            ["Carol", 55],
            ["Dave", 66],
        ], keys=["name", "age"])
        out = StringIO()
        table.write(out)
        self.assertEqual(out.getvalue(), ' Alice | 33 \r\n'
                                         ' Smith |    \r\n'
                                         ' Bob   | 44 \r\n'
                                         ' Carol | 55 \r\n'
                                         ' Dave  | 66 \r\n')

    def test_write_with_style(self):
        table = DataList([
            ["Alice", 33],
            ["Bob", 44],
            ["Carol", 55],
            ["Dave", 66],
        ], keys=["name", "age"])
        out = StringIO()
        table.write(out, header={"fg": "red"})
        self.assertEqual(out.getvalue(), ' name  | age \r\n'
                                         '-------------\r\n'
                                         ' Alice |  33 \r\n'
                                         ' Bob   |  44 \r\n'
                                         ' Carol |  55 \r\n'
                                         ' Dave  |  66 \r\n')

    def test_write_with_skip(self):
        table = DataList([
            ["Alice", 33],
            ["Bob", 44],
            ["Carol", 55],
            ["Dave", 66],
        ], keys=["name", "age"])
        out = StringIO()
        table.write(out, skip=2)
        self.assertEqual(out.getvalue(), ' Carol | 55 \r\n'
                                         ' Dave  | 66 \r\n')

    def test_write_with_limit(self):
        table = DataList([
            ["Alice", 33],
            ["Bob", 44],
            ["Carol", 55],
            ["Dave", 66],
        ], keys=["name", "age"])
        out = StringIO()
        table.write(out, limit=2)
        self.assertEqual(out.getvalue(), ' Alice | 33 \r\n'
                                         ' Bob   | 44 \r\n')

    def test_write_with_skip_and_limit(self):
        table = DataList([
            ["Alice", 33],
            ["Bob", 44],
            ["Carol", 55],
            ["Dave", 66],
        ], keys=["name", "age"])
        out = StringIO()
        table.write(out, skip=1, limit=2)
        self.assertEqual(out.getvalue(), ' Bob   | 44 \r\n'
                                         ' Carol | 55 \r\n')

    def test_write_with_skip_and_limit_overflow(self):
        table = DataList([
            ["Alice", 33],
            ["Bob", 44],
            ["Carol", 55],
            ["Dave", 66],
        ], keys=["name", "age"])
        out = StringIO()
        table.write(out, skip=1, limit=10)
        self.assertEqual(out.getvalue(), ' Bob   | 44 \r\n'
                                         ' Carol | 55 \r\n'
                                         ' Dave  | 66 \r\n')

    def test_write_csv(self):
        table = DataList([
            ["Alice", 33],
            ["Bob", 44],
            ["Carol", 55],
            ["Dave", 66],
        ], keys=["name", "age"])
        out = StringIO()
        table.write_csv(out)
        self.assertEqual(out.getvalue(), 'Alice,33\r\n'
                                         'Bob,44\r\n'
                                         'Carol,55\r\n'
                                         'Dave,66\r\n')

    def test_write_csv_with_header(self):
        table = DataList([
            ["Alice", 33],
            ["Bob", 44],
            ["Carol", 55],
            ["Dave", 66],
        ], keys=["name", "age"])
        out = StringIO()
        table.write_csv(out, header=True)
        self.assertEqual(out.getvalue(), 'name,age\r\n'
                                         'Alice,33\r\n'
                                         'Bob,44\r\n'
                                         'Carol,55\r\n'
                                         'Dave,66\r\n')

    def test_write_csv_with_header_style(self):
        table = DataList([
            ["Alice", 33],
            ["Bob", 44],
            ["Carol", 55],
            ["Dave", 66],
        ], keys=["name", "age"])
        out = StringIO()
        table.write_csv(out, header={"fg": "cyan"})
        self.assertEqual(out.getvalue(), 'name,age\r\n'
                                         'Alice,33\r\n'
                                         'Bob,44\r\n'
                                         'Carol,55\r\n'
                                         'Dave,66\r\n')

    def test_write_csv_with_limit(self):
        table = DataList([
            ["Alice", 33],
            ["Bob", 44],
            ["Carol", 55],
            ["Dave", 66],
        ], keys=["name", "age"])
        out = StringIO()
        table.write_csv(out, limit=2)
        self.assertEqual(out.getvalue(), 'Alice,33\r\n'
                                         'Bob,44\r\n')

    def test_write_csv_with_comma_in_value(self):
        table = DataList([
            ["Alice", 33],
            ["Bob", 44],
            ["Carol", 55],
            ["Smith, Dave", 66],
        ], keys=["name", "age"])
        out = StringIO()
        table.write_csv(out)
        self.assertEqual(out.getvalue(), 'Alice,33\r\n'
                                         'Bob,44\r\n'
                                         'Carol,55\r\n'
                                         '"Smith, Dave",66\r\n')

    def test_write_csv_with_quotes_in_value(self):
        table = DataList([
            ["Alice", 33],
            ["Bob", 44],
            ["Carol", 55],
            ["Dave \"Nordberg\" Smith", 66],
        ], keys=["name", "age"])
        out = StringIO()
        table.write_csv(out)
        self.assertEqual(out.getvalue(), 'Alice,33\r\n'
                                         'Bob,44\r\n'
                                         'Carol,55\r\n'
                                         '"Dave ""Nordberg"" Smith",66\r\n')

    def test_write_csv_with_none_in_value(self):
        table = DataList([
            ["Alice", 33],
            ["Bob", 44],
            ["Carol", 55],
            ["Dave", None],
        ], keys=["name", "age"])
        out = StringIO()
        table.write_csv(out)
        self.assertEqual(out.getvalue(), 'Alice,33\r\n'
                                         'Bob,44\r\n'
                                         'Carol,55\r\n'
                                         'Dave,\r\n')

    def test_write_tsv(self):
        table = DataList([
            ["Alice", 33],
            ["Bob", 44],
            ["Carol", 55],
            ["Dave", 66],
        ], keys=["name", "age"])
        out = StringIO()
        table.write_tsv(out)
        self.assertEqual(out.getvalue(), 'Alice\t33\r\n'
                                         'Bob\t44\r\n'
                                         'Carol\t55\r\n'
                                         'Dave\t66\r\n')

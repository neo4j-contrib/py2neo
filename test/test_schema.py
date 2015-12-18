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


from py2neo import GraphError, Node, node
from py2neo.packages.httpstream import ClientError, Resource as _Resource
from test.util import Py2neoTestCase
from test.compat import patch


class NotFoundError(ClientError):
    status_code = 404


class DodgyClientError(ClientError):
    status_code = 499


class SchemaTestCase(Py2neoTestCase):

    def setUp(self):
        self.reset()

    def test_schema_index(self):
        label_1 = next(self.unique_string)
        label_2 = next(self.unique_string)
        munich = node({'name': "München", 'key': "09162000"})
        self.graph.create(munich)
        munich.clear_labels()
        munich.update_labels({label_1, label_2})
        self.schema.create_index(label_1, "name")
        self.schema.create_index(label_1, "key")
        self.schema.create_index(label_2, "name")
        self.schema.create_index(label_2, "key")
        found_borough_via_name = self.graph.find(label_1, "name", "München")
        found_borough_via_key = self.graph.find(label_1, "key", "09162000")
        found_county_via_name = self.graph.find(label_2, "name", "München")
        found_county_via_key = self.graph.find(label_2, "key", "09162000")
        assert list(found_borough_via_name) == list(found_borough_via_key)
        assert list(found_county_via_name) == list(found_county_via_key)
        assert list(found_borough_via_name) == list(found_county_via_name)
        keys = self.schema.get_indexes(label_1)
        assert "name" in keys
        assert "key" in keys
        self.schema.drop_index(label_1, "name")
        self.schema.drop_index(label_1, "key")
        self.schema.drop_index(label_2, "name")
        self.schema.drop_index(label_2, "key")
        with self.assertRaises(GraphError):
            self.schema.drop_index(label_2, "key")
        self.graph.delete(munich)

    def test_unique_constraint(self):
        label_1 = next(self.unique_string)
        borough = Node(label_1, name="Taufkirchen")
        self.graph.create(borough)
        self.schema.create_uniqueness_constraint(label_1, "name")
        constraints = self.schema.get_uniqueness_constraints(label_1)
        assert "name" in constraints
        with self.assertRaises(GraphError):
            self.graph.create(Node(label_1, name="Taufkirchen"))
        self.graph.delete(borough)

    def test_labels_constraints(self):
        label_1 = next(self.unique_string)
        a = Node(label_1, name="Alice")
        b = Node(label_1, name="Alice")
        self.graph.create(a | b)
        with self.assertRaises(GraphError):
            self.graph.schema.create_uniqueness_constraint(label_1, "name")
        b.remove_label(label_1)
        self.graph.push(b)
        self.schema.create_uniqueness_constraint(label_1, "name")
        a.remove_label(label_1)
        self.graph.push(a)
        b.add_label(label_1)
        self.graph.push(b)
        try:
            self.schema.drop_index(label_1, "name")
        except GraphError as error:
            # this is probably a server bug
            assert error.__cause__.status_code // 100 == 5
        else:
            assert False
        b.remove_label(label_1)
        self.graph.push(b)
        self.schema.drop_uniqueness_constraint(label_1, "name")
        with self.assertRaises(GraphError):
            self.schema.drop_uniqueness_constraint(label_1, "name")
        self.graph.delete(a | b)

    def test_drop_index_handles_404_errors_correctly(self):
        with patch.object(_Resource, "delete") as mocked:
            mocked.side_effect = NotFoundError
            with self.assertRaises(GraphError):
                self.schema.drop_index("Person", "name")

    def test_drop_index_handles_non_404_errors_correctly(self):
        with patch.object(_Resource, "delete") as mocked:
            mocked.side_effect = DodgyClientError
            try:
                self.schema.drop_index("Person", "name")
            except GraphError as error:
                assert isinstance(error.__cause__, DodgyClientError)
            else:
                assert False

    def test_drop_unique_constraint_handles_404_errors_correctly(self):
        with patch.object(_Resource, "delete") as mocked:
            mocked.side_effect = NotFoundError
            with self.assertRaises(GraphError):
                self.schema.drop_uniqueness_constraint("Person", "name")

    def test_drop_unique_constraint_handles_non_404_errors_correctly(self):
        with patch.object(_Resource, "delete") as mocked:
            mocked.side_effect = DodgyClientError
            try:
                self.schema.drop_uniqueness_constraint("Person", "name")
            except GraphError as error:
                assert isinstance(error.__cause__, DodgyClientError)
            else:
                assert False

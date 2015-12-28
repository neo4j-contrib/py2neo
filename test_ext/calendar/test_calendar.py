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


from py2neo import Node
from py2neo.ext.calendar import GregorianCalendar
from test.util import Py2neoTestCase


def assert_similar(a, b):
    assert isinstance(a, Node)
    assert isinstance(b, Node)
    assert a.labels() == b.labels()
    assert dict(a) == dict(b)


class CalendarTestCase(Py2neoTestCase):

    def setUp(self):
        self.calendar = GregorianCalendar(self.graph)
    
    def test_can_create_date(self):
        date = self.calendar.date(2000, 12, 25)
        assert_similar(date.year, Node("Year", key="2000", year=2000))
        assert_similar(date.month, Node("Month", key="2000-12", year=2000, month=12))
        assert_similar(date.day, Node("Day", key="2000-12-25", year=2000, month=12, day=25))
    
    def test_can_create_date_with_short_numbers(self):
        date = self.calendar.date(2000, 1, 2)
        assert_similar(date.year, Node("Year", key="2000", year=2000))
        assert_similar(date.month, Node("Month", key="2000-01", year=2000, month=1))
        assert_similar(date.day, Node("Day", key="2000-01-02", year=2000, month=1, day=2))
    
    def test_can_create_month_year(self):
        month_year = self.calendar.date(2000, 12)
        assert_similar(month_year.year, Node("Year", key="2000", year=2000))
        assert_similar(month_year.month, Node("Month", key="2000-12", year=2000, month=12))
    
    def test_can_create_year(self):
        year = self.calendar.date(2000)
        assert_similar(year.year, Node("Year", key="2000", year=2000))
    
    def test_example_code(self):
        from py2neo import Graph, Node, Relationship
        from py2neo.ext.calendar import GregorianCalendar
    
        graph = Graph()
        calendar = GregorianCalendar(graph)
    
        alice = Node("Person", name="Alice")
        birth = Relationship(alice, "BORN", calendar.date(1800, 1, 1).day)
        death = Relationship(alice, "DIED", calendar.date(1900, 12, 31).day)
        graph.create(alice | birth | death)
    
        assert birth.end_node()["year"] == 1800
        assert birth.end_node()["month"] == 1
        assert birth.end_node()["day"] == 1
    
        assert death.end_node()["year"] == 1900
        assert death.end_node()["month"] == 12
        assert death.end_node()["day"] == 31

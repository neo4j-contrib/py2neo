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

import logging

import pytest

from py2neo import neo4j, legacy
from py2neo.ext.calendar import GregorianCalendar

logging.basicConfig(
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
    level=logging.DEBUG,
)


@pytest.fixture(autouse=True)
def setup(request, graph):
    graph.clear()

    if request.instance:
        # Grab a handle to an index for linking to time data
        service = legacy.GraphDatabaseService()
        time = service.get_or_create_index(neo4j.Node, "TIME")
        request.instance.calendar = GregorianCalendar(time)


def test_can_create_date():
    date = GregorianCalendar.Date(2000, 12, 25)
    assert date.year == 2000
    assert date.month == 12
    assert date.day == 25
    assert str(date) == "2000-12-25"


def test_can_create_date_with_short_numbers():
    date = GregorianCalendar.Date(2000, 1, 2)
    assert date.year == 2000
    assert date.month == 1
    assert date.day == 2
    assert str(date) == "2000-01-02"


def test_can_create_month_year():
    month_year = GregorianCalendar.Date(2000, 12)
    assert month_year.year == 2000
    assert month_year.month == 12
    assert month_year.day is None
    assert str(month_year) == "2000-12"


def test_can_create_year():
    year = GregorianCalendar.Date(2000)
    assert year.year == 2000
    assert year.month is None
    assert year.day is None
    assert str(year) == "2000"


class TestExampleCode(object):
    def test_example_code_runs(self):
        from py2neo import neo4j
        from py2neo.ext.calendar import GregorianCalendar

        graph = legacy.GraphDatabaseService()
        time_index = graph.get_or_create_index(neo4j.Node, "TIME")
        calendar = GregorianCalendar(time_index)

        alice, birth, death = graph.create(
            {"name": "Alice"},
            (0, "BORN", calendar.day(1800, 1, 1)),
            (0, "DIED", calendar.day(1900, 12, 31)),
        )

        assert birth.end_node["year"] == 1800
        assert birth.end_node["month"] == 1
        assert birth.end_node["day"] == 1

        assert death.end_node["year"] == 1900
        assert death.end_node["month"] == 12
        assert death.end_node["day"] == 31


class TestDays(object):
    def test_can_get_day_node(self):
        christmas = self.calendar.day(2000, 12, 25)
        assert isinstance(christmas, neo4j.Node)
        assert christmas["year"] == 2000
        assert christmas["month"] == 12
        assert christmas["day"] == 25

    def test_will_always_get_same_day_node(self):
        first_christmas = self.calendar.day(2000, 12, 25)
        for i in range(40):
            next_christmas = self.calendar.day(2000, 12, 25)
            assert next_christmas == first_christmas

    def test_can_get_different_day_nodes(self):
        christmas = self.calendar.day(2000, 12, 25)
        boxing_day = self.calendar.day(2000, 12, 26)
        assert christmas != boxing_day


class TestMonths(object):
    def test_can_get_month_node(self):
        december = self.calendar.month(2000, 12)
        assert isinstance(december, neo4j.Node)
        assert december["year"] == 2000
        assert december["month"] == 12

    def test_will_always_get_same_month_node(self):
        first_december = self.calendar.month(2000, 12)
        for i in range(40):
            next_december = self.calendar.month(2000, 12)
            assert next_december == first_december

    def test_can_get_different_month_nodes(self):
        december = self.calendar.month(2000, 12)
        january = self.calendar.month(2001, 1)
        assert december != january


class TestYears(object):
    def test_can_get_year_node(self):
        millennium = self.calendar.year(2000)
        assert isinstance(millennium, neo4j.Node)
        assert millennium["year"] == 2000

    def test_will_always_get_same_month_node(self):
        first_millennium = self.calendar.year(2000)
        for i in range(40):
            next_millennium = self.calendar.year(2000)
            assert next_millennium == first_millennium

    def test_can_get_different_year_nodes(self):
        millennium_2000 = self.calendar.year(2000)
        millennium_2001 = self.calendar.year(2001)
        assert millennium_2000 != millennium_2001


class TestDateRanges(object):
    def test_can_get_date_range(self):
        xmas_year = self.calendar.date_range((2000, 12, 25), (2001, 12, 25))
        assert isinstance(xmas_year, neo4j.Node)
        assert xmas_year["start_date"] == "2000-12-25"
        assert xmas_year["end_date"] == "2001-12-25"
        rels = list(xmas_year.match_outgoing("START_DATE"))
        assert len(rels) == 1
        assert rels[0].end_node == self.calendar.date((2000, 12, 25))
        assert rels[0].end_node == self.calendar.day(2000, 12, 25)
        rels = list(xmas_year.match_outgoing("END_DATE"))
        assert len(rels) == 1
        assert rels[0].end_node == self.calendar.date((2001, 12, 25))
        assert rels[0].end_node == self.calendar.day(2001, 12, 25)

    def test_will_always_get_same_date_range_node(self):
        range1 = self.calendar.date_range((2000, 12, 25), (2001, 12, 25))
        range2 = self.calendar.date_range((2000, 12, 25), (2001, 12, 25))
        assert range1 == range2

    def test_can_get_different_date_range_nodes(self):
        range1 = self.calendar.date_range((2000, 12, 25), (2001, 12, 25))
        range2 = self.calendar.date_range((2000, 1, 1), (2000, 12, 31))
        assert range1 != range2

    def test_single_day_range(self):
        range_ = self.calendar.date_range((2000, 12, 25), (2000, 12, 25))
        assert range_ == self.calendar.day(2000, 12, 25)

    def test_range_within_month(self):
        advent = self.calendar.date_range((2000, 12, 1), (2000, 12, 24))
        rels = list(advent.match_incoming("DATE_RANGE"))
        assert len(rels) == 1
        assert rels[0].start_node == self.calendar.month(2000, 12)
        rels = list(advent.match_outgoing("START_DATE"))
        assert len(rels) == 1
        assert rels[0].end_node == self.calendar.date((2000, 12, 1))
        assert rels[0].end_node == self.calendar.day(2000, 12, 1)
        rels = list(advent.match_outgoing("END_DATE"))
        assert len(rels) == 1
        assert rels[0].end_node == self.calendar.date((2000, 12, 24))
        assert rels[0].end_node == self.calendar.day(2000, 12, 24)

    def test_range_within_year(self):
        range_ = self.calendar.date_range((2000, 4, 10), (2000, 12, 24))
        rels = list(range_.match_incoming("DATE_RANGE"))
        assert len(rels) == 1
        assert rels[0].start_node == self.calendar.year(2000)
        rels = list(range_.match_outgoing("START_DATE"))
        assert len(rels) == 1
        assert rels[0].end_node == self.calendar.date((2000, 4, 10))
        assert rels[0].end_node == self.calendar.day(2000, 4, 10)
        rels = list(range_.match_outgoing("END_DATE"))
        assert len(rels) == 1
        assert rels[0].end_node == self.calendar.date((2000, 12, 24))
        assert rels[0].end_node == self.calendar.day(2000, 12, 24)

    def test_open_start_range(self):
        range_ = self.calendar.date_range(None, (2000, 12, 25))
        rels = list(range_.match_outgoing("START_DATE"))
        assert len(rels) == 0
        rels = list(range_.match_outgoing("END_DATE"))
        assert len(rels) == 1
        assert rels[0].end_node == self.calendar.date((2000, 12, 25))
        assert rels[0].end_node == self.calendar.day(2000, 12, 25)

    def test_open_end_range(self):
        range_ = self.calendar.date_range((2000, 12, 25), None)
        rels = list(range_.match_outgoing("START_DATE"))
        assert len(rels) == 1
        assert rels[0].end_node == self.calendar.date((2000, 12, 25))
        assert rels[0].end_node == self.calendar.day(2000, 12, 25)
        rels = list(range_.match_outgoing("END_DATE"))
        assert len(rels) == 0

    def test_no_fully_open_date_range(self):
        try:
            self.calendar.date_range(None, None)
        except ValueError:
            return True
        else:
            return False

    def test_first_quarter(self):
        range_ = self.calendar.quarter(2000, 1)
        rels = list(range_.match_outgoing("START_DATE"))
        assert len(rels) == 1
        assert rels[0].end_node == self.calendar.day(2000, 1, 1)
        rels = list(range_.match_outgoing("END_DATE"))
        assert len(rels) == 1
        assert rels[0].end_node == self.calendar.day(2000, 3, 31)

    def test_second_quarter(self):
        range_ = self.calendar.quarter(2000, 2)
        rels = list(range_.match_outgoing("START_DATE"))
        assert len(rels) == 1
        assert rels[0].end_node == self.calendar.day(2000, 4, 1)
        rels = list(range_.match_outgoing("END_DATE"))
        assert len(rels) == 1
        assert rels[0].end_node == self.calendar.day(2000, 6, 30)

    def test_third_quarter(self):
        range_ = self.calendar.quarter(2000, 3)
        rels = list(range_.match_outgoing("START_DATE"))
        assert len(rels) == 1
        assert rels[0].end_node == self.calendar.day(2000, 7, 1)
        rels = list(range_.match_outgoing("END_DATE"))
        assert len(rels) == 1
        assert rels[0].end_node == self.calendar.day(2000, 9, 30)

    def test_fourth_quarter(self):
        range_ = self.calendar.quarter(2000, 4)
        rels = list(range_.match_outgoing("START_DATE"))
        assert len(rels) == 1
        assert rels[0].end_node == self.calendar.day(2000, 10, 1)
        rels = list(range_.match_outgoing("END_DATE"))
        assert len(rels) == 1
        assert rels[0].end_node == self.calendar.day(2000, 12, 31)

    def test_no_fifth_quarter(self):
        try:
            self.calendar.quarter(2000, 5)
        except ValueError:
            return True
        else:
            return False

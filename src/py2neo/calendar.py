#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright 2011-2012 Nigel Small
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

""" The `calendar` module provides standardised date management functionality
    based on a calendar subgraph::

        from py2neo import neo4j
        from py2neo.calendar import GregorianCalendar

        graph_db = neo4j.GraphDatabaseService()
        time_index = graph_db.get_or_create_index(neo4j.Node, "TIME")
        calendar = GregorianCalendar(time_index)

        graph_db.create(
            {"name": "Alice"},
            (0, "BORN", calendar.day(1800, 1, 1)),
            (0, "DIED", calendar.day(1900, 12, 31)),
        )

    The root calendar node is held within a dedicated node index which needs
    to be supplied to the calendar constructor.

    All dates managed by the :py:class:`GregorianCalendar` class adhere to a
    hierarchy such as::

        (CALENDAR)-[:YEAR]->(2000)-[:MONTH]->(12)-[:DAY]->(25)

"""

from datetime import date as _date
from . import cypher


class GregorianCalendar(object):

    def __init__(self, index):
        """ Create a new calendar instance pointed to by the
            index provided.
        """
        self._index = index
        self._graph_db = self._index._graph_db
        self._calendar = self._index.get_or_create("scheme", "Gregorian", {})

    def calendar(self):
        return self._calendar

    def day(self, year, month, day):
        """ Fetch the calendar node representing the day specified by `year`,
            `month` and `day`.
        """
        d = _date(year, month, day)
        date_path = self._calendar.get_or_create_path(
            ("YEAR",  {"year": d.year}),
            ("MONTH", {"year": d.year, "month": d.month}),
            ("DAY",   {"year": d.year, "month": d.month, "day": d.day}),
        )
        return date_path.nodes[-1]

    def month(self, year, month):
        """ Fetch the calendar node representing the month specified by `year`
            and `month`.
        """
        d = _date(year, month, 1)
        date_path = self._calendar.get_or_create_path(
            ("YEAR",  {"year": d.year}),
            ("MONTH", {"year": d.year, "month": d.month}),
        )
        return date_path.nodes[-1]

    def year(self, year):
        """ Fetch the calendar node representing the year specified by `year`.
        """
        d = _date(year, 1, 1)
        date_path = self._calendar.get_or_create_path(
            ("YEAR",  {"year": d.year}),
        )
        return date_path.nodes[-1]

    def date(self, date):
        bits = len(date)
        if bits == 3:
            return self.day(*date)
        elif bits == 2:
            return self.month(*date)
        elif bits == 1:
            return self.year(*date)
        else:
            raise ValueError(date)

    def date_range(self, start_date, end_date):
        #                         (CAL)
        #                           |
        #                       [:RANGE]
        #                           |
        #                           v
        # (START)<-[:START_DATE]-(RANGE)-[:END_DATE]->(END)
        query = "START cal=node({cal}), st=node({st}), en=node({en}) " \
                "CREATE UNIQUE (st)<-[:START_DATE]-(r {rp})-[:END_DATE]->(en), " \
                "              (cal)-[:DATE_RANGE]->(r {rp})" \
                "RETURN r"
        params = {
            "cal": self._calendar._id,
            "st": self.date(start_date)._id,
            "en": self.date(end_date)._id,
            "rp": {
                "start_date": "-".join("0" + str(d) if d < 10 else str(d) for d in start_date),
                "end_date": "-".join("0" + str(d) if d < 10 else str(d) for d in end_date),
            },
        }
        data, metadata = cypher.execute(self._graph_db, query, params)
        return data[0][0]

    def quarter(self, year, quarter):
        if quarter == 1:
            return self.date_range((year, 1, 1), (year, 3, 31))
        elif quarter == 2:
            return self.date_range((year, 4, 1), (year, 6, 30))
        elif quarter == 3:
            return self.date_range((year, 7, 1), (year, 9, 30))
        elif quarter == 4:
            return self.date_range((year, 10, 1), (year, 12, 31))
        else:
            raise ValueError("quarter must be in 1..4")

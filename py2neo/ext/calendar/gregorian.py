#!/usr/bin/env python
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


from py2neo import Node, Path, ConstraintViolation


class GregorianCalendar(object):

    __instances = {}

    def __new__(cls, graph):
        if not graph.supports_node_labels:
            raise ValueError("Graph does not support node labels")
        try:
            inst = cls.__instances[graph]
        except KeyError:
            inst = super(GregorianCalendar, cls).__new__(cls)
            inst.graph = graph
            try:
                inst.graph.schema.create_unique_constraint("Calendar", "name")
            except ConstraintViolation:
                pass
            inst.root_node = inst.graph.merge_one("Calendar", "name", "Gregorian")
            cls.__instances[graph] = inst
        return inst

    def year_node(self, year):
        path = Path(self.root_node, "YEAR", Node("Year", year=year))
        self.graph.create_unique(path)
        return path.end_node

    def month_node(self, year, month):
        path = Path(self.root_node, "YEAR", Node("Year", year=year),
                                    "MONTH", Node("Month", year=year, month=month))
        self.graph.create_unique(path)
        return path.end_node

    def day_node(self, year, month, day):
        path = Path(self.root_node, "YEAR", Node("Year", year=year),
                                    "MONTH", Node("Month", year=year, month=month),
                                    "DAY", Node("Day", year=year, month=month, day=day))
        self.graph.create_unique(path)
        return path.end_node

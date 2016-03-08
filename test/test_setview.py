#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright 2011-2016, Nigel Small
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


from py2neo.types import SetView
from test.util import Py2neoTestCase


class SetViewTestCase(Py2neoTestCase):

    weekdays = {"Monday", "Tuesday", "Wednesday", "Thursday", "Friday"}
    weekend = {"Friday", "Saturday", "Sunday"}
    weekday_view = SetView(weekdays)
    weekend_view = SetView(weekend)

    def test_repr(self):
        assert repr(self.weekday_view)

    def test_length(self):
        assert len(self.weekday_view) == 5

    def test_can_iterate(self):
        assert set(self.weekday_view) == self.weekdays

    def test_contains(self):
        assert "Monday" in self.weekday_view

    def test_intersection(self):
        assert self.weekday_view & self.weekend_view == {"Friday"}

    def test_conjunction(self):
        assert self.weekday_view | self.weekend_view == \
               {"Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"}

    def test_difference(self):
        assert self.weekday_view - self.weekend_view == \
               {"Monday", "Tuesday", "Wednesday", "Thursday"}

    def test_symmetric_difference(self):
        assert self.weekday_view ^ self.weekend_view == \
               {"Monday", "Tuesday", "Wednesday", "Thursday", "Saturday", "Sunday"}

#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright 2011-2020, Nigel Small
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


from unittest import TestCase

from py2neo.internal.versioning import Version


class VersioningTestCase(TestCase):

    def test_equality(self):
        assert Version.parse("3.4.0") == Version.parse("3.4.0")

    def test_inequality(self):
        assert Version.parse("3.4.0") != Version.parse("3.4.1")

    def test_component_parts(self):
        version = Version.parse("3.4.1")
        assert version.major == 3
        assert version.minor == 4
        assert version.patch == 1

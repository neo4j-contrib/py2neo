#/usr/bin/env python
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

import sys
import time

PY3K = sys.version_info[0] >= 3

__author__    = "Nigel Small <py2neo@nigelsmall.org>"
__copyright__ = "Copyright 2011-2012 Nigel Small"
__license__   = "Apache License, Version 2.0"


from py2neo import rest
import unittest


class PropertyCacheTest(unittest.TestCase):

    def setUp(self):
        pass

    def test_simple_property_cache(self):
        props = rest.PropertyCache()
        props.update({
            "foo": "bar",
            "number": 42
        })
        self.assertEqual("bar", props["foo"])
        self.assertEqual(42, props["number"])

    def test_none_property_cache(self):
        props = rest.PropertyCache(None)
        props.update({
            "foo": "bar",
            "number": 42
        })
        self.assertEqual("bar", props["foo"])
        self.assertEqual(42, props["number"])

    def test_populated_property_cache(self):
        props = rest.PropertyCache({
            "foo": "bar",
            "number": 42
        })
        self.assertEqual("bar", props["foo"])
        self.assertEqual(42, props["number"])

    def test_property_cache_with_expiry(self):
        props = rest.PropertyCache({
            "foo": "bar",
            "number": 42
        }, max_age=3)
        self.assertFalse(props.expired)
        self.assertEqual("bar", props["foo"])
        self.assertEqual(42, props["number"])
        time.sleep(3)
        self.assertTrue(props.expired)
        self.assertEqual("bar", props["foo"])
        self.assertEqual(42, props["number"])


if __name__ == '__main__':
    unittest.main()


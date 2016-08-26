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


from threading import Lock, local
from weakref import WeakValueDictionary


class EntityCache(object):

    def __init__(self, value_constructor):
        self.value_constructor = value_constructor
        self.lock = Lock()
        self._dict = WeakValueDictionary()

    def clear(self):
        self._dict.clear()

    def merge(self, key, value=None):
        """ Ensure a value exists for a given key, creating a new value if
        necessary but using an existing one if available.
        """
        with self.lock:
            if value is None:
                if key in self._dict:
                    # extract
                    value = self._dict[key]
                else:
                    # create and insert
                    self._dict[key] = value = self.value_constructor()
            else:
                # insert
                self._dict[key] = value
            return value

    def remove(self, key):
        """ Ensure no value exists for a given key.
        """
        with self.lock:
            try:
                del self._dict[key]
            except KeyError:
                pass

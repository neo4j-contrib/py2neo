#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright 2011-2017, Nigel Small
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


class ThreadLocalEntityCache(local):

    def __init__(self):
        self.lock = Lock()
        self._dict = WeakValueDictionary()

    def __contains__(self, key):
        return key in self._dict

    def __getitem__(self, key):
        return self._dict[key]

    def clear(self):
        self._dict.clear()

    def keys(self):
        return self._dict.keys()

    def update(self, key, value):
        """ Extract, insert or remove a value for a given key.
        """
        with self.lock:
            if value is None:
                # remove
                try:
                    del self._dict[key]
                except KeyError:
                    pass
                else:
                    return None
            elif callable(value):
                try:
                    # extract
                    return self._dict[key]
                except KeyError:
                    # construct and insert
                    new_value = value()
                    self._dict[key] = new_value
                    return new_value
            else:
                # insert or replace
                self._dict[key] = value
                return value

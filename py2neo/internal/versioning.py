#!/usr/bin/env python
# coding: utf-8

# Copyright 2011-2019, Nigel Small
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


from re import compile as re_compile


version_string_pattern = re_compile("(\d+)\.(\d+)\.(\d+)-?(.*)")


class Version(tuple):

    def __new__(cls, string):
        return tuple.__new__(cls, version_string_pattern.match(string).groups())

    def __str__(self):
        return ".".join(self[:3]) + "".join("-%s" % part for part in self[3:] if part)

    def __eq__(self, other):
        return (int(self[0]), int(self[1]), int(self[2]), self[3]) == (int(other[0]), int(other[1]), int(other[2]), other[3])

    def __ne__(self, other):
        return not self.__eq__(other)

    def __lt__(self, other):
        return (int(self[0]), int(self[1]), int(self[2]), self[3]) < (int(other[0]), int(other[1]), int(other[2]), other[3])

    @property
    def major(self):
        return int(self[0])

    @property
    def minor(self):
        return int(self[1])

    @property
    def patch(self):
        return int(self[2])

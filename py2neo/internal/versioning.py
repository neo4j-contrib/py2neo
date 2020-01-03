#!/usr/bin/env python
# coding: utf-8

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


from unicodedata import category


class Version(tuple):

    @classmethod
    def parse(cls, string):
        parts = []
        last_ch = None
        for ch in string:
            if last_ch is None:
                parts.append([ch])
            elif ch == ".":
                if last_ch in ".-":
                    parts[-1][-1] += "0"
                parts[-1].append("")
            elif ch == "-":
                if last_ch in ".-":
                    parts[-1][-1] += "0"
                parts.append([""])
            else:
                if last_ch not in ".-" and category(ch)[0] != category(last_ch)[0]:
                    parts.append([ch])
                else:
                    parts[-1][-1] += ch
            last_ch = ch
        for part in parts:
            for i, x in enumerate(part):
                try:
                    part[i] = int(x)
                except (ValueError, TypeError):
                    pass
            while len(part) > 1 and not part[-1]:
                part[:] = part[:-1]
        return cls(*map(tuple, parts))

    def __new__(cls, *parts):
        parts = list(parts)
        for i, part in enumerate(parts):
            if not isinstance(part, tuple):
                parts[i] = (part,)
        return super(Version, cls).__new__(cls, parts)

    def __repr__(self):
        return "%s%r" % (type(self).__name__, tuple(self))

    @property
    def primary(self):
        try:
            return self[0]
        except IndexError:
            return ()

    @property
    def major(self):
        try:
            return self.primary[0]
        except IndexError:
            return 0

    @property
    def minor(self):
        try:
            return self.primary[1]
        except IndexError:
            return 0

    @property
    def patch(self):
        try:
            return self.primary[2]
        except IndexError:
            return 0

    @property
    def major_minor(self):
        return self.major, self.minor

    @property
    def major_minor_patch(self):
        return self.major, self.minor, self.patch

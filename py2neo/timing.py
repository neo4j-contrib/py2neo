#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright 2011-2021, Nigel Small
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


from monotonic import monotonic


class Timer(object):

    def __init__(self, seconds):
        self.__t0 = t0 = monotonic()
        self.__t1 = t0 + (seconds or 0)

    def __bool__(self):
        return self.remaining() > 0

    __nonzero__ = __bool__

    def remaining(self):
        diff = self.__t1 - monotonic()
        return diff if diff > 0 else 0.0


def repeater(at_least, timeout):
    """ Yield an incrementing number at least `at_least` times,
    thereafter continuing until the timeout has been reached.
    """
    timer = Timer(timeout)
    repeat = 0
    while repeat < at_least or timer.remaining():
        yield repeat
        repeat += 1

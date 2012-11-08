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

""" Utility module
"""

import sys
__PY3K = sys.version_info[0] >= 3

import time

try:
    from urllib.parse import quote as _quote
except ImportError:
    from urllib import quote as _quote


if __PY3K:
    is_string = lambda value: isinstance(value, str)
else:
    is_string = lambda value: isinstance(value, (str, unicode))

def quote(string, safe='/'):
    """ Quote a string for use in URIs.
    """
    try:
        return _quote(string, safe)
    except UnicodeEncodeError:
        return string


def numberise(n):
    """ Convert a value to an integer if possible. If not, simply return
        the input value.
    """
    if n == "NaN":
        return None
    try:
        return int(n)
    except ValueError:
        return n

def execution_time(func, *args, **kwargs):
    if sys.platform == "win32":
        timer = time.clock
    else:
        timer = time.time
    t0 = timer()
    try:
        func(*args, **kwargs)
    finally:
        return timer() - t0

def compact(obj):
    """ Return a copy of an object with all :py:const:`None` values removed.
    """
    if isinstance(obj, dict):
        return dict((key, value) for key, value in obj.items() if value is not None)
    else:
        return obj.__class__(value for value in obj if value is not None)

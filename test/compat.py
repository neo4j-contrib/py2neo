#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright 2011-2015, Nigel Small
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


from sys import version_info
try:
    from unittest.mock import Mock, patch
except ImportError:
    from mock import Mock, patch


__all__ = ["Mock", "patch", "long", "assert_repr"]


if version_info >= (3,):
    long = int

else:
    long = long


def assert_repr(obj, repr_string, python2_repr_string=None):
    if version_info >= (3,) or python2_repr_string is None:
        assert repr(obj) == repr_string
    else:
        assert repr(obj) == python2_repr_string

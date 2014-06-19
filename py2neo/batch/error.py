#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright 2011-2014, Nigel Small
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


class BatchError(Exception):

    @classmethod
    def with_name(cls, name):
        try:
            return type(name, (cls,), {})
        except TypeError:
            # for Python 2.x
            return type(str(name), (cls,), {})

    def __init__(self, response):
        self._response = response
        Exception.__init__(self, self.message)

    @property
    def message(self):
        return self._response.message

    @property
    def exception(self):
        return self._response.exception

    @property
    def full_name(self):
        return self._response.full_name

    @property
    def stack_trace(self):
        return self._response.stack_trace

    @property
    def cause(self):
        return self._response.cause

    @property
    def request(self):
        return self._response.request

    @property
    def response(self):
        return self._response

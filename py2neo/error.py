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


from __future__ import unicode_literals


__all__ = ["BindError", "GraphError", "JoinError"]


class BindError(Exception):
    """ Raised when a local graph entity is not or cannot be bound to a remote graph entity.
    """


class GraphError(Exception):
    """ Default exception class for all errors returned by the
    Neo4j server. See also `CypherError` subclass and `BatchError`
    wrapper class which contain additional qualifying information.
    """

    @classmethod
    def hydrate(cls, data):
        try:
            exception = data["exception"]
            try:
                error_cls = type(exception, (cls,), {})
            except TypeError:
                # for Python 2.x
                error_cls = type(str(exception), (cls,), {})
        except KeyError:
            error_cls = cls
        message = data.pop("message", None)
        return error_cls(message, **data)

    def __init__(self, message, **kwargs):
        Exception.__init__(self, message)
        self.message = message
        self.full_name = kwargs.get("fullname")
        self.request = kwargs.get("request")
        self.response = kwargs.get("response")
        self.stack_trace = kwargs.get("stacktrace")
        try:
            self.cause = self.hydrate(kwargs["cause"])
        except Exception:
            self.cause = None


class JoinError(Exception):
    """ Raised when two graph entities cannot be joined together.
    """

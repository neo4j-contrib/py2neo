#!/usr/bin/env python
# -*- encoding: utf-8 -*-

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


from importlib import import_module
import json


class BindError(Exception):
    """ Raised when a local graph entity is not or cannot be bound
    to a remote graph entity.
    """


class Finished(Exception):
    """ Raised when actions are attempted against a finished object
    that is no longer available for use.
    """

    def __init__(self, obj):
        self.obj = obj

    def __repr__(self):
        return "%s finished" % self.obj.__class__.__name__


class GraphError(Exception):
    """ Default exception class for all errors returned by the
    Neo4j server.
    """

    __cause__ = None
    exception = None
    fullname = None
    request = None
    response = None
    stacktrace = None

    def __new__(cls, *args, **kwargs):
        try:
            exception = kwargs["exception"]
            try:
                error_cls = type(exception, (cls,), {})
            except TypeError:
                # for Python 2.x
                error_cls = type(str(exception), (cls,), {})
        except KeyError:
            error_cls = cls
        return Exception.__new__(error_cls, *args)

    def __init__(self, *args, **kwargs):
        Exception.__init__(self, *args)
        for key, value in kwargs.items():
            setattr(self, key.lower(), value)


class JoinError(Exception):
    """ Raised when two graph entities cannot be joined together.
    """


class Unauthorized(GraphError):
    """ Raised when a request is made to Neo4j without required credentials.
    """


class CypherError(GraphError):

    default_message = "The server returned a Cypher error"
    status_code_map = {}

    statement = None
    parameters = None

    def __init__(self, message, **kwargs):
        GraphError.__init__(self, message, **kwargs)
        if self.request:
            request_body = json.loads(self.request.body)
            self.statement = request_body.get("query")
            self.parameters = request_body.get("params")


class TransactionError(CypherError):
    """
    """

    code = None
    message = None

    @classmethod
    def hydrate(cls, data):
        code = data["code"]
        message = data["message"]
        _, classification, category, title = code.split(".")
        error_module = import_module("py2neo.status." + category.lower())
        error_cls = getattr(error_module, title)
        inst = error_cls(message)
        inst.code = code
        inst.message = message
        return inst


class ClientError(TransactionError):
    """ The Client sent a bad request - changing the request might yield a successful outcome.
    """


class DatabaseError(TransactionError):
    """ The database failed to service the request.
    """


class TransientError(TransactionError):
    """ The database cannot service the request right now, retrying later might yield a successful outcome.
    """

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


class Neo4jError(Exception):
    """ Exception class for modelling error status codes returned by
    Neo4j.

    For details of the status codes available, visit:
    https://neo4j.com/docs/status-codes/current/

    """

    @classmethod
    def hydrate(cls, data):
        code = data.get("code")
        message = data.get("message")
        return cls(message, code)

    def __init__(self, message, code):
        super(Neo4jError, self).__init__(message)
        self.code = code
        try:
            _, self.classification, self.category, self.title = self.code.split(".")
        except TypeError:
            self.classification = self.category = self.title = None

    def __str__(self):
        return "[%s.%s] %s" % (self.category, self.title, super(Neo4jError, self).__str__())

    @property
    def message(self):
        return self.args[0]

    def should_retry(self):
        return (self.should_invalidate_routing_table() or
                (self.classification == "TransientError" and
                 self.category == "Transaction" and
                 self.title not in ("Terminated", "LockClientStopped")))

    def should_invalidate_routing_table(self):
        return self.code == "Neo.ClientError.Cluster.NotALeader"

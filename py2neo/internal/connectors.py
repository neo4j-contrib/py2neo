#!/usr/bin/env python
# -*- encoding: utf-8 -*-

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


from __future__ import absolute_import

from py2neo.net.json import JSONHydrator
from py2neo.net.packstream import PackStreamHydrator


DEFAULT_MAX_CONNECTIONS = 40


class Connector(object):

    def __new__(cls, profile, **settings):
        if profile.protocol == "bolt":
            return object.__new__(BoltConnector)
        elif profile.protocol == "http":
            return object.__new__(HTTPConnector)
        else:
            raise ValueError("Unsupported protocol %r" % profile.protocol)

    def __init__(self, profile, user_agent=None, max_connections=None, **_):
        from py2neo.net import ConnectionPool
        max_size = max_connections or DEFAULT_MAX_CONNECTIONS
        self.profile = profile
        self.pool = ConnectionPool(profile, user_agent=user_agent, max_size=max_size, max_age=None)
        self.transactions = {}

    @property
    def server_agent(self):
        cx = self.pool.acquire()
        try:
            return cx.server_agent
        finally:
            self.pool.release(cx)

    @property
    def user_agent(self):
        return self.pool.user_agent

    def close(self):
        self.pool.close()

    def run(self, statement, parameters=None, tx=None, graph=None, entities=None):
        raise NotImplementedError()

    def is_valid_transaction(self, tx):
        return tx and tx in self.transactions

    def _assert_valid_tx(self, tx):
        from py2neo.database import TransactionError
        if not tx:
            raise TransactionError("No transaction")
        if tx not in self.transactions:
            raise TransactionError("Invalid transaction")

    def begin(self):
        raise NotImplementedError()

    def commit(self, tx):
        raise NotImplementedError()

    def rollback(self, tx):
        raise NotImplementedError()


class BoltConnector(Connector):

    def run(self, statement, parameters=None, tx=None, graph=None, entities=None):
        if tx is None:
            cx = self.pool.acquire()
            hydrator = PackStreamHydrator(cx.protocol_version, graph, entities)
            result = cx.auto_run(statement, hydrator.dehydrate(parameters))
        else:
            cx = self.transactions.get(tx)
            hydrator = PackStreamHydrator(cx.protocol_version, graph, entities)
            result = cx.run_in_tx(tx, statement, hydrator.dehydrate(parameters))
        cx.pull(result)
        cx.sync(result)
        return result, hydrator

    def begin(self):
        cx = self.pool.acquire()
        tx = cx.begin()
        self.transactions[tx] = cx
        return tx

    def commit(self, tx):
        self._assert_valid_tx(tx)
        cx = self.transactions.pop(tx)
        cx.commit(tx)

    def rollback(self, tx):
        self._assert_valid_tx(tx)
        cx = self.transactions.pop(tx)
        cx.rollback(tx)


class HTTPConnector(Connector):

    def run(self, statement, parameters=None, tx=None, graph=None, entities=None):
        cx = self.pool.acquire()
        if tx is None:
            hydrator = JSONHydrator("rest", graph, entities)
            result = cx.auto_run(statement, hydrator.dehydrate(parameters))
        else:
            hydrator = JSONHydrator("rest", graph, entities)
            result = cx.run_in_tx(tx, statement, hydrator.dehydrate(parameters))
        cx.pull(result)
        cx.sync(result)
        self.pool.release(cx)
        return result, hydrator

    def begin(self):
        cx = self.pool.acquire()
        tx = cx.begin()
        self.pool.release(cx)
        self.transactions[tx] = None
        return tx

    def commit(self, tx):
        self._assert_valid_tx(tx)
        self.transactions.pop(tx)
        cx = self.pool.acquire()
        cx.commit(tx)
        self.pool.release(cx)

    def rollback(self, tx):
        self._assert_valid_tx(tx)
        self.transactions.pop(tx)
        cx = self.pool.acquire()
        cx.rollback(tx)
        self.pool.release(cx)

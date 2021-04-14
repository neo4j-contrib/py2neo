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


from pytest import raises

from py2neo import Transaction


class FakeTransaction(object):

    def __init__(self, graph_name, readonly=False):
        self._graph_name = graph_name
        self._readonly = readonly

    @property
    def readonly(self):
        return self._readonly


class FakeConnector(object):

    def begin(self, graph_name, readonly=False):
        return FakeTransaction(graph_name, readonly=readonly)


class FakeService(object):

    @property
    def connector(self):
        return FakeConnector()


class FakeGraph(object):

    @property
    def name(self):
        return "fake"

    @property
    def service(self):
        return FakeService()


class FakeTransactionManager(object):

    @property
    def graph(self):
        return FakeGraph()


def test_should_fail_on_tx_create_object():
    tx = Transaction(FakeGraph())
    with raises(TypeError):
        tx.create(object())


def test_should_fail_on_tx_delete_object():
    tx = Transaction(FakeGraph())
    with raises(TypeError):
        tx.delete(object())


def test_should_fail_on_tx_merge_object():
    tx = Transaction(FakeGraph())
    with raises(TypeError):
        tx.merge(object())


def test_should_fail_on_tx_pull_object():
    tx = Transaction(FakeGraph())
    with raises(TypeError):
        tx.pull(object())


def test_should_fail_on_tx_push_object():
    tx = Transaction(FakeGraph())
    with raises(TypeError):
        tx.push(object())


def test_should_fail_on_tx_separate_object():
    tx = Transaction(FakeGraph())
    with raises(TypeError):
        tx.separate(object())

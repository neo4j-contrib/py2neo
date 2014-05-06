#/usr/bin/env python
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

import pytest
from py2neo import cypher

try:
    session = cypher.Session()
except NotImplementedError:
    supports_transactions = False
else:
    supports_transactions = True


@pytest.mark.skipif(not supports_transactions,
                    reason="Transactions not supported by this server version")
def test_can_execute_single_statement_transaction():
    tx = session.create_transaction()
    assert not tx.finished
    tx.append("CREATE (a) RETURN a")
    results = tx.commit()
    assert len(results) == 1
    for result in results:
        assert len(result) == 1
        for record in result:
            assert record.columns == ("a",)
    assert tx.finished


@pytest.mark.skipif(not supports_transactions,
                    reason="Transactions not supported by this server version")
def test_can_execute_multi_statement_transaction():
    tx = session.create_transaction()
    assert not tx.finished
    tx.append("CREATE (a) RETURN a")
    tx.append("CREATE (a) RETURN a")
    tx.append("CREATE (a) RETURN a")
    results = tx.commit()
    assert len(results) == 3
    for result in results:
        assert len(result) == 1
        for record in result:
            assert record.columns == ("a",)
    assert tx.finished


@pytest.mark.skipif(not supports_transactions,
                    reason="Transactions not supported by this server version")
def test_can_execute_multi_execute_transaction():
    tx = session.create_transaction()
    for i in range(10):
        assert not tx.finished
        tx.append("CREATE (a) RETURN a")
        tx.append("CREATE (a) RETURN a")
        tx.append("CREATE (a) RETURN a")
        results = tx.execute()
        assert len(results) == 3
        for result in results:
            assert len(result) == 1
            for record in result:
                assert record.columns == ("a",)
    tx.commit()
    assert tx.finished


@pytest.mark.skipif(not supports_transactions,
                    reason="Transactions not supported by this server version")
def test_can_rollback_transaction():
    tx = session.create_transaction()
    for i in range(10):
        assert not tx.finished
        tx.append("CREATE (a) RETURN a")
        tx.append("CREATE (a) RETURN a")
        tx.append("CREATE (a) RETURN a")
        results = tx.execute()
        assert len(results) == 3
        for result in results:
            assert len(result) == 1
            for record in result:
                assert record.columns == ("a",)
    tx.rollback()
    assert tx.finished


@pytest.mark.skipif(not supports_transactions,
                    reason="Transactions not supported by this server version")
def test_can_generate_transaction_error():
    tx = session.create_transaction()
    try:
        tx.append("CRAETE (a) RETURN a")
        tx.commit()
    except cypher.TransactionError as err:
        assert repr(err)
    else:
        assert False


@pytest.mark.skipif(not supports_transactions,
                    reason="Transactions not supported by this server version")
def test_cannot_append_after_transaction_finished():
    tx = session.create_transaction()
    tx.rollback()
    try:
        tx.append("CREATE (a) RETURN a")
    except cypher.TransactionFinished as err:
        assert repr(err) == "Transaction finished"
    else:
        assert False


@pytest.mark.skipif(not supports_transactions,
                    reason="Transactions not supported by this server version")
def test_single_execute():
    result = session.execute("CREATE (a) RETURN a")
    assert len(result) == 1
    for record in result:
        assert record.columns == ("a",)

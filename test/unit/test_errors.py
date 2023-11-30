#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright (c) "Neo4j"
# Neo4j Sweden AB [https://neo4j.com]
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

from py2neo.errors import Neo4jError, ClientError, DatabaseError, TransientError


def test_client_error():
    error = Neo4jError("Oops", "Neo.ClientError.General.Error")
    assert isinstance(error, ClientError)
    assert error.code == "Neo.ClientError.General.Error"
    assert error.classification == "ClientError"
    assert error.category == "General"
    assert error.title == "Error"
    assert error.message == "Oops"
    assert str(error) == "[General.Error] Oops"


def test_database_error():
    error = Neo4jError("Oops", "Neo.DatabaseError.General.Error")
    assert isinstance(error, DatabaseError)
    assert error.code == "Neo.DatabaseError.General.Error"
    assert error.classification == "DatabaseError"
    assert error.category == "General"
    assert error.title == "Error"
    assert error.message == "Oops"
    assert str(error) == "[General.Error] Oops"


def test_transient_error():
    error = Neo4jError("Oops", "Neo.TransientError.General.Error")
    assert isinstance(error, TransientError)
    assert error.code == "Neo.TransientError.General.Error"
    assert error.classification == "TransientError"
    assert error.category == "General"
    assert error.title == "Error"
    assert error.message == "Oops"
    assert str(error) == "[General.Error] Oops"


def test_unknown_error():
    error = Neo4jError("Oops", "Neo.UnknownError.General.Error")
    assert isinstance(error, Neo4jError)
    assert error.code == "Neo.UnknownError.General.Error"
    assert error.classification == "UnknownError"
    assert error.category == "General"
    assert error.title == "Error"
    assert error.message == "Oops"
    assert str(error) == "[General.Error] Oops"


def test_error_hydration():
    error = Neo4jError.hydrate({"code": "Neo.ClientError.General.Error", "message": "Oops"})
    assert isinstance(error, ClientError)
    assert error.code == "Neo.ClientError.General.Error"
    assert error.classification == "ClientError"
    assert error.category == "General"
    assert error.title == "Error"
    assert error.message == "Oops"


def test_bad_code_type():
    with raises(ValueError):
        _ = Neo4jError("Oops", None)


def test_bad_code_prefix():
    with raises(ValueError):
        _ = Neo4jError("Oops", "Fake.ClientError.General.Error")


def test_bad_code_part_count():
    with raises(ValueError):
        _ = Neo4jError("Oops", "Neo.ClientError.Error")


def test_base_error_should_not_retry():
    error = Neo4jError("Oops", "Neo.UnknownError.General.Error")
    assert not error.should_retry()


def test_client_error_should_not_retry():
    error = Neo4jError("Oops", "Neo.ClientError.General.Error")
    assert not error.should_retry()


def test_not_a_leader_error_should_retry():
    error = Neo4jError("Oops", "Neo.ClientError.Cluster.NotALeader")
    assert error.should_retry()


def test_database_error_should_not_retry():
    error = Neo4jError("Oops", "Neo.DatabaseError.General.Error")
    assert not error.should_retry()


def test_transient_error_should_retry():
    error = Neo4jError("Oops", "Neo.TransientError.General.Error")
    assert error.should_retry()


def test_base_error_should_not_invalidate_routing_table():
    error = Neo4jError("Oops", "Neo.UnknownError.General.Error")
    assert not error.should_invalidate_routing_table()


def test_client_error_should_not_invalidate_routing_table():
    error = Neo4jError("Oops", "Neo.ClientError.General.Error")
    assert not error.should_invalidate_routing_table()


def test_database_error_should_not_invalidate_routing_table():
    error = Neo4jError("Oops", "Neo.DatabaseError.General.Error")
    assert not error.should_invalidate_routing_table()


def test_transient_error_should_not_invalidate_routing_table():
    error = Neo4jError("Oops", "Neo.TransientError.General.Error")
    assert not error.should_invalidate_routing_table()


def test_not_a_leader_error_should_invalidate_routing_table():
    error = Neo4jError("Oops", "Neo.ClientError.Cluster.NotALeader")
    assert error.should_invalidate_routing_table()

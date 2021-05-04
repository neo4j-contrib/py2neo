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


from pytest import fixture, skip, raises

from py2neo.client.bolt import Bolt


@fixture(scope="session")
def bolt_profile(service_profile):
    if service_profile.protocol != "bolt":
        skip("Not a Bolt profile")
    return service_profile


@fixture()
def bolt(bolt_profile):
    bolt = Bolt.open(bolt_profile)
    try:
        yield bolt
    finally:
        bolt.close()


@fixture()
def rx_bolt(bolt):
    if bolt.protocol_version < (4, 0):
        skip("Bolt reactive not available")
    return bolt


def test_hello_goodbye(bolt):
    assert bolt.protocol_version


def test_auto_run_pull_then_pull_then_pull(rx_bolt):
    r = rx_bolt.auto_run("UNWIND range(1, 5) AS n RETURN n")
    assert list(rx_bolt.pull(r, 3).records) == [[1], [2], [3]]
    assert list(rx_bolt.pull(r, 3).records) == [[4], [5]]
    with raises(IndexError):
        rx_bolt.pull(r, 3)


def test_explicit_tx_run_pull_then_pull_then_pull(rx_bolt):
    tx = rx_bolt.begin(None)
    r = rx_bolt.run(tx, "UNWIND range(1, 5) AS n RETURN n")
    assert list(rx_bolt.pull(r, 3).records) == [[1], [2], [3]]
    assert list(rx_bolt.pull(r, 3).records) == [[4], [5]]
    with raises(IndexError):
        rx_bolt.pull(r, 3)
    rx_bolt.commit(tx)


def test_auto_run_pull_then_pull_all_then_pull(rx_bolt):
    r = rx_bolt.auto_run("UNWIND range(1, 5) AS n RETURN n")
    assert list(rx_bolt.pull(r, 3).records) == [[1], [2], [3]]
    assert list(rx_bolt.pull(r, -1).records) == [[4], [5]]
    with raises(IndexError):
        rx_bolt.pull(r, 3)


def test_explicit_tx_run_pull_then_pull_all_then_pull(rx_bolt):
    tx = rx_bolt.begin(None)
    r = rx_bolt.run(tx, "UNWIND range(1, 5) AS n RETURN n")
    assert list(rx_bolt.pull(r, 3).records) == [[1], [2], [3]]
    assert list(rx_bolt.pull(r, -1).records) == [[4], [5]]
    with raises(IndexError):
        rx_bolt.pull(r, 3)
    rx_bolt.commit(tx)


def test_auto_run_discard_then_discard(bolt):
    r = bolt.auto_run("UNWIND range(1, 5) AS n RETURN n")
    bolt.discard(r)
    with raises(IndexError):
        bolt.discard(r)


def test_explicit_tx_run_discard_then_discard(bolt):
    tx = bolt.begin(None)
    r = bolt.run(tx, "UNWIND range(1, 5) AS n RETURN n")
    bolt.discard(r)
    with raises(IndexError):
        bolt.discard(r)
    bolt.commit(tx)


def test_auto_run_discard_then_pull(bolt):
    r = bolt.auto_run("UNWIND range(1, 5) AS n RETURN n")
    bolt.discard(r)
    with raises(IndexError):
        bolt.pull(r)


def test_explicit_tx_run_discard_then_pull(bolt):
    tx = bolt.begin(None)
    r = bolt.run(tx, "UNWIND range(1, 5) AS n RETURN n")
    bolt.discard(r)
    with raises(IndexError):
        bolt.pull(r)
    bolt.commit(tx)


def test_auto_run_pull_then_discard(rx_bolt):
    r = rx_bolt.auto_run("UNWIND range(1, 5) AS n RETURN n")
    assert list(rx_bolt.pull(r, 3).records) == [[1], [2], [3]]
    rx_bolt.discard(r)


def test_explicit_tx_run_pull_then_discard(rx_bolt):
    tx = rx_bolt.begin(None)
    r = rx_bolt.run(tx, "UNWIND range(1, 5) AS n RETURN n")
    assert list(rx_bolt.pull(r, 3).records) == [[1], [2], [3]]
    rx_bolt.discard(r)
    rx_bolt.commit(tx)


def test_pull_without_run(bolt):
    with raises(TypeError):
        bolt.pull(None)


def test_discard_without_run(bolt):
    with raises(TypeError):
        bolt.discard(None)


def test_out_of_order_pull(rx_bolt):
    tx = rx_bolt.begin(None)
    r1 = rx_bolt.run(tx, "UNWIND range(1, 5) AS n RETURN 'a', n")
    assert list(rx_bolt.pull(r1, 3).records) == [["a", 1], ["a", 2], ["a", 3]]
    r2 = rx_bolt.run(tx, "UNWIND range(1, 5) AS n RETURN 'b', n")
    assert list(rx_bolt.pull(r2, 3).records) == [["b", 1], ["b", 2], ["b", 3]]
    assert list(rx_bolt.pull(r1, 3).records) == [["a", 4], ["a", 5]]
    assert list(rx_bolt.pull(r2, 3).records) == [["b", 4], ["b", 5]]
    rx_bolt.commit(tx)

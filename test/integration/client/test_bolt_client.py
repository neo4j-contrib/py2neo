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
def bolt_profile(connection_profile):
    if connection_profile.protocol != "bolt":
        skip("Not a Bolt profile")
    return connection_profile


def test_hello_goodbye(bolt_profile):
    bolt = Bolt.open(bolt_profile)
    assert bolt.protocol_version
    bolt.close()


def test_out_of_order_pull(bolt_profile):
    bolt = Bolt.open(bolt_profile)
    with raises(TypeError):
        bolt.pull(None)
    bolt.close()


def test_out_of_order_discard(bolt_profile):
    bolt = Bolt.open(bolt_profile)
    with raises(TypeError):
        bolt.discard(None)
    bolt.close()

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


from pytest import fixture, skip

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

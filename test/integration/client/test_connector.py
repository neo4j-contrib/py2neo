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


from py2neo.client import Connector


def test_auto_run_with_pull_all(service_profile):
    cx = Connector(service_profile)
    result = cx.auto_run("UNWIND range(1, 5) AS n RETURN n")
    assert list(result.records()) == [[1], [2], [3], [4], [5]]
    cx.close()


def test_auto_run_with_pull_3_then_pull_all(service_profile):
    from pansi.console import watch; watch("py2neo")
    cx = Connector(service_profile)
    result = cx.auto_run("UNWIND range(1, 5) AS n RETURN n", pull=3)
    assert list(result.records()) == [[1], [2], [3]]
    cx.pull(result)
    assert list(result.records()) == [[4], [5]]
    cx.close()

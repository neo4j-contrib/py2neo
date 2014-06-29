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


from py2neo.batch import Job


def test_can_create_batch_request():
    method = "POST"
    uri = "cypher"
    body = {"query": "CREATE (a) RETURN a"}
    request = Job(method, uri, body)
    assert request.method == method
    assert request.uri == uri
    assert request.body == body


def test_batch_requests_are_equal_if_same():
    method = "POST"
    uri = "cypher"
    body = {"query": "CREATE (a) RETURN a"}
    request_1 = Job(method, uri, body)
    request_2 = request_1
    assert request_1 == request_2
    assert hash(request_1) == hash(request_2)


def test_batch_requests_are_unequal_if_not_same():
    method = "POST"
    uri = "cypher"
    body = {"query": "CREATE (a) RETURN a"}
    request_1 = Job(method, uri, body)
    request_2 = Job(method, uri, body)
    assert request_1 != request_2
    assert hash(request_1) != hash(request_2)

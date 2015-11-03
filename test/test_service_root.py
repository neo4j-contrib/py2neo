#!/usr/bin/env python
# -*- encoding: utf-8 -*-

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


from py2neo import ServiceRoot
from test.util import Py2neoTestCase


class ServiceRootTestCase(Py2neoTestCase):

    def test_can_create_service_root_with_trailing_slash(self):
        uri = "http://localhost:7474/"
        service_root = ServiceRoot(uri)
        assert service_root.uri == uri
        index = service_root.resource.get().content
        assert "data" in index

    def test_can_create_service_root_without_trailing_slash(self):
        uri = "http://localhost:7474/"
        service_root = ServiceRoot(uri[:-1])
        assert service_root.uri == uri
        index = service_root.resource.get().content
        assert "data" in index

    def test_same_uri_gives_same_instance(self):
        uri = "http://localhost:7474/"
        service_root_1 = ServiceRoot(uri)
        service_root_2 = ServiceRoot(uri)
        assert service_root_1 is service_root_2

    def test_service_root_equality(self):
        uri = "http://localhost:7474/"
        service_root_1 = ServiceRoot(uri)
        service_root_2 = ServiceRoot(uri)
        assert service_root_1 == service_root_2

    def test_service_root_inequality(self):
        uri = "http://localhost:7474/"
        service_root_1 = ServiceRoot(uri)
        service_root_2 = ServiceRoot("http://remotehost:7474/")
        assert service_root_1 != service_root_2

    def test_service_root_is_not_equal_to_non_service_root(v):
        uri = "http://localhost:7474/"
        service_root = ServiceRoot(uri)
        assert service_root != object()

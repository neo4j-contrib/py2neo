#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright 2011-2015, Nigel Small
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


import logging
from unittest import skipUnless

from py2neo.database import DBMS
from py2neo.http import set_http_header, get_http_headers, http_rewrite, Resource
from py2neo.packages.httpstream import ClientError as _ClientError, ServerError as _ServerError, \
    Resource as _Resource, Response as _Response
from py2neo.status import GraphError
from py2neo.status.security import AuthorizationFailed
from test.util import Py2neoTestCase
from test.compat import patch


dbms = DBMS()
supports_auth = dbms.supports_auth()
supports_bolt = dbms.supports_bolt()


class DodgyServerError(_ServerError):

    status_code = 599


class TestHandler(logging.Handler):

    def __init__(self, counter, level):
        super(TestHandler, self).__init__(level)
        self.counter = counter
        self.counter.responses = []

    def emit(self, record):
        if record.msg.startswith("< "):
            self.counter.responses.append(record.args)


class HTTPCounter(object):

    def __init__(self):
        self.handler = TestHandler(self, logging.INFO)
        self.logger = logging.getLogger("httpstream")
        self.logger.addHandler(self.handler)
        self.logger.setLevel(logging.DEBUG)
        self.responses = []

    def __enter__(self):
        self.reset()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.handler = None

    def reset(self):
        self.responses = []

    @property
    def response_count(self):
        return len(self.responses)


class HeaderTestCase(Py2neoTestCase):

    def test_can_add_and_retrieve_global_header(self):
        set_http_header("Key1", "Value1")
        headers = get_http_headers("localhost:7474")
        assert headers["Key1"] == "Value1"

    def test_can_add_and_retrieve_header_for_specific_host_port(self):
        set_http_header("Key1", "Value1", "example.com:7474")
        set_http_header("Key1", "Value2", "example.net:7474")
        headers = get_http_headers("example.com:7474")
        assert headers["Key1"] == "Value1"

    def test_can_add_and_retrieve_multiple_headers_for_specific_host_port(self):
        set_http_header("Key1", "Value1", "example.com:7474")
        set_http_header("Key2", "Value2", "example.com:7474")
        headers = get_http_headers("example.com:7474")
        assert headers["Key1"] == "Value1"
        assert headers["Key2"] == "Value2"


class RewriteTestCase(Py2neoTestCase):

    def test_can_rewrite_uri(self):
        http_rewrite(("https", "localtoast", 4747), ("http", "localhost", 7474))
        assert Resource("https://localtoast:4747/").uri == "http://localhost:7474/"

    def test_can_remove_rewrite_uri(self):
        http_rewrite(("https", "localtoast", 4747), ("http", "localhost", 7474))
        http_rewrite(("https", "localtoast", 4747), None)
        assert Resource("https://localtoast:4747/").uri == "https://localtoast:4747/"

    def test_can_remove_unknown_rewrite_uri(self):
        http_rewrite(("https", "localnonsense", 4747), None)


class ClientErrorTestCase(Py2neoTestCase):

    def test_can_handle_400(self):
        resource = Resource("http://localhost:7474/db/data/cypher")
        try:
            resource.post()
        except GraphError as error:
            try:
                self.assert_error(
                    error, (GraphError,), "org.neo4j.server.rest.repr.BadInputException",
                    (_ClientError, _Response), 400)
            except AssertionError:
                self.assert_error(
                    error, (GraphError,), "org.neo4j.server.rest.repr.InvalidArgumentsException",
                    (_ClientError, _Response), 400)
        else:
            assert False

    def test_can_handle_404(self):
        node_id = self.get_non_existent_node_id()
        resource = Resource("http://localhost:7474/db/data/node/%s" % node_id)
        try:
            resource.get()
        except GraphError as error:
            self.assert_error(
                error, (GraphError,), "org.neo4j.server.rest.web.NodeNotFoundException",
                (_ClientError, _Response), 404)
        else:
            assert False


class ServerErrorTestCase(Py2neoTestCase):

    def setUp(self):
        self.non_existent_resource = Resource("http://localhost:7474/db/data/x")

    def test_can_handle_json_error_from_get(self):
        try:
            self.non_existent_resource.get()
        except GraphError as error:
            cause = error.__cause__
            assert isinstance(cause, _ClientError)
            assert isinstance(cause, _Response)
            assert cause.status_code == 404
        else:
            assert False

    def test_can_handle_json_error_from_put(self):
        try:
            self.non_existent_resource.put("")
        except GraphError as error:
            cause = error.__cause__
            assert isinstance(cause, _ClientError)
            assert isinstance(cause, _Response)
            assert cause.status_code == 404
        else:
            assert False

    def test_can_handle_json_error_from_post(self):
        try:
            self.non_existent_resource.post("")
        except GraphError as error:
            cause = error.__cause__
            assert isinstance(cause, _ClientError)
            assert isinstance(cause, _Response)
            assert cause.status_code == 404
        else:
            assert False

    def test_can_handle_json_error_from_delete(self):
        try:
            self.non_existent_resource.delete()
        except GraphError as error:
            cause = error.__cause__
            assert isinstance(cause, _ClientError)
            assert isinstance(cause, _Response)
            assert cause.status_code == 404
        else:
            assert False

    def test_can_handle_other_error_from_get(self):
        with patch.object(_Resource, "get") as mocked:
            mocked.side_effect = DodgyServerError
            resource = Resource("http://localhost:7474/db/data/node/spam")
            try:
                resource.get()
            except GraphError as error:
                assert isinstance(error.__cause__, DodgyServerError)
            else:
                assert False

    def test_can_handle_other_error_from_put(self):
        with patch.object(_Resource, "put") as mocked:
            mocked.side_effect = DodgyServerError
            resource = Resource("http://localhost:7474/db/data/node/spam")
            try:
                resource.put()
            except GraphError as error:
                assert isinstance(error.__cause__, DodgyServerError)
            else:
                assert False

    def test_can_handle_other_error_from_post(self):
        with patch.object(_Resource, "post") as mocked:
            mocked.side_effect = DodgyServerError
            resource = Resource("http://localhost:7474/db/data/node/spam")
            try:
                resource.post()
            except GraphError as error:
                assert isinstance(error.__cause__, DodgyServerError)
            else:
                assert False

    def test_can_handle_other_error_from_delete(self):
        with patch.object(_Resource, "delete") as mocked:
            mocked.side_effect = DodgyServerError
            resource = Resource("http://localhost:7474/db/data/node/spam")
            try:
                resource.delete()
            except GraphError as error:
                assert isinstance(error.__cause__, DodgyServerError)
            else:
                assert False


class AuthTestCase(Py2neoTestCase):

    @skipUnless(supports_auth, "Auth not supported")
    def test_can_raise_unauthorized_on_get(self):
        with self.assertRaises(AuthorizationFailed):
            _ = Resource("http://foo:bar@127.0.0.1:7474/db/data/").get().content

    @skipUnless(supports_auth, "Auth not supported")
    def test_can_raise_unauthorized_on_put(self):
        with self.assertRaises(AuthorizationFailed):
            _ = Resource("http://foo:bar@127.0.0.1:7474/db/data/").put({}).content

    @skipUnless(supports_auth, "Auth not supported")
    def test_can_raise_unauthorized_on_post(self):
        with self.assertRaises(AuthorizationFailed):
            _ = Resource("http://foo:bar@127.0.0.1:7474/db/data/").post({}).content

    @skipUnless(supports_auth, "Auth not supported")
    def test_can_raise_unauthorized_on_delete(self):
        with self.assertRaises(AuthorizationFailed):
            _ = Resource("http://foo:bar@127.0.0.1:7474/db/data/").delete().content

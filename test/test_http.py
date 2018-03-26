#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright 2011-2017, Nigel Small
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

from neo4j.v1 import GraphDatabase

from py2neo.graph import GraphError, CypherSyntaxError
from py2neo.http import HTTP
from py2neo.types.graph import Node

from test.util import HTTPGraphTestCase


class HTTPLoggingHandler(logging.Handler):

    def __init__(self, counter, level):
        super(HTTPLoggingHandler, self).__init__(level)
        self.counter = counter
        self.counter.responses = []

    def emit(self, record):
        if record.msg.startswith("< "):
            self.counter.responses.append(record.args)


class HTTPCounter(object):

    def __init__(self):
        self.handler = HTTPLoggingHandler(self, logging.INFO)
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


class ClientErrorTestCase(HTTPGraphTestCase):

    def test_can_handle_400(self):
        headers = {
            "Authorization": HTTP.authorization("neo4j", "password")
        }
        resource = HTTP("http://localhost:7474/db/data/cypher", **headers)
        try:
            resource.post("", {}, expected=(201,))
        except GraphError as error:
            try:
                self.assert_error(
                    error, (GraphError,), "org.neo4j.server.rest.repr.BadInputException")
            except AssertionError:
                self.assert_error(
                    error, (GraphError,), "org.neo4j.server.rest.repr.InvalidArgumentsException")
        else:
            assert False

    def test_can_handle_404(self):
        node_id = self.get_non_existent_node_id()
        headers = {
            "Authorization": HTTP.authorization("neo4j", "password")
        }
        resource = HTTP("http://localhost:7474/db/data/", **headers)
        try:
            resource.get_json("node/%s" % node_id)
        except GraphError as error:
            self.assert_error(
                error, (GraphError,), "org.neo4j.server.rest.web.NodeNotFoundException")
        else:
            assert False


class ServerErrorTestCase(HTTPGraphTestCase):

    def setUp(self):
        super(ServerErrorTestCase, self).setUp()
        headers = {
            "Authorization": HTTP.authorization("neo4j", "password")
        }
        self.non_existent_resource = HTTP("http://localhost:7474/db/data/x", **headers)

    def test_can_handle_json_error_from_get(self):
        try:
            self.non_existent_resource.get_json("")
        except GraphError as error:
            assert error.http_status_code == 404
        else:
            assert False

    def test_can_handle_json_error_from_post(self):
        try:
            self.non_existent_resource.post("", {}, expected=(201,)).close()
        except GraphError as error:
            assert error.http_status_code == 404
        else:
            assert False

    def test_can_handle_json_error_from_delete(self):
        try:
            self.non_existent_resource.delete("", expected=(204,)).close()
        except GraphError as error:
            assert error.http_status_code == 404
        else:
            assert False


class HTTPSchemeTestCase(HTTPGraphTestCase):

    @classmethod
    def setUpClass(cls):
        from py2neo.http import HTTPDriver
        HTTPDriver.register()

    def test_should_be_able_to_run_transaction_with_http_scheme(self):
        driver = GraphDatabase.driver("http://localhost:7474", auth=("neo4j", "password"))
        try:
            with driver.session() as session:
                with session.begin_transaction() as tx:
                    result = tx.run("UNWIND range(1, 3) AS n RETURN n")
                    records = list(result)
                    assert len(records) == 3
                    assert records[0][0] == 1
                    assert records[1][0] == 2
                    assert records[2][0] == 3
        finally:
            driver.close()

    def test_should_hydrate_node_to_py2neo_object(self):
        driver = GraphDatabase.driver("http://localhost:7474", auth=("neo4j", "password"))
        try:
            with driver.session() as session:
                with session.begin_transaction() as tx:
                    result = tx.run("CREATE (a) RETURN a")
                    records = list(result)
                    assert len(records) == 1
                    node = records[0][0]
                    assert isinstance(node, Node)
        finally:
            driver.close()

    def test_should_be_able_to_rollback(self):
        driver = GraphDatabase.driver("http://localhost:7474", auth=("neo4j", "password"))
        try:
            with driver.session() as session:
                with session.begin_transaction() as tx:
                    result = tx.run("CREATE (a) RETURN id(a)")
                    records = list(result)
                    assert len(records) == 1
                    node_id = records[0][0]
                    tx.success = False
                result = session.run("MATCH (a) WHERE id(a) = {x} RETURN count(a)", x=node_id)
                records = list(result)
                assert len(records) == 1
                count = records[0][0]
                assert count == 0
        finally:
            driver.close()

    def test_should_fail_on_bad_cypher(self):
        driver = GraphDatabase.driver("http://localhost:7474", auth=("neo4j", "password"))
        try:
            with driver.session() as session:
                _ = session.run("X")
                with self.assertRaises(CypherSyntaxError):
                    session.sync()
        finally:
            driver.close()

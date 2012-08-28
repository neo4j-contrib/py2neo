#/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright 2011-2012 Nigel Small
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

import sys
PY3K = sys.version_info[0] >= 3

import logging
import socket
import unittest

from py2neo import rest


__author__    = "Nigel Small <py2neo@nigelsmall.org>"
__copyright__ = "Copyright 2011-2012 Nigel Small"
__license__   = "Apache License, Version 2.0"

logging.basicConfig(
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
    level=logging.DEBUG,
)

class RESTClientTestCase(unittest.TestCase):

    def test_rest_client(self):
        rest_client = rest.Client()
        uri = "http://localhost:7474/db/data/"
        rs = rest_client.send(rest.Request(None, "GET", uri))
        self.assertEqual(200, rs.status)
        self.assertEqual("http://localhost:7474/db/data/", rs.uri)
        self.assertTrue(rs.body)

    def test_rest_client_with_redirect(self):
        rest_client = rest.Client()
        uri = "http://localhost:7474/db/data"
        rs = rest_client.send(rest.Request(None, "GET", uri))
        self.assertEqual(200, rs.status)
        self.assertEqual("http://localhost:7474/db/data/", rs.uri)
        self.assertTrue(rs.body)

    def test_rest_client_with_bad_path(self):
        rest_client = rest.Client()
        rest.http_headers.add("X-Foo", "bar", ".*/foo/")
        uri = "http://localhost:7474/foo/"
        rs = rest_client.send(rest.Request(None, "GET", uri))
        self.assertEqual(404, rs.status)
        self.assertEqual("http://localhost:7474/foo/", rs.uri)
        self.assertFalse(rs.body)

    def test_rest_client_with_bad_host(self):
        rest_client = rest.Client()
        uri = "http://localtoast:7474/db/data/"
        self.assertRaises(socket.error, rest_client.send, rest.Request(None, "GET", uri))

    def test_rest_client_with_bad_port(self):
        rest_client = rest.Client()
        uri = "http://localhost:7575/db/data/"
        self.assertRaises(socket.error, rest_client.send, rest.Request(None, "GET", uri))

    def test_rest_client_with_post(self):
        rest_client = rest.Client()
        uri = "http://localhost:7474/db/data/node"
        rs = rest_client.send(rest.Request(None, "POST", uri, {
            "name": "Alice",
        }))
        self.assertEqual(201, rs.status)
        self.assertEqual("http://localhost:7474/db/data/node", rs.uri)
        self.assertTrue(rs.location.startswith("http://localhost:7474/db/data/node/"))
        self.assertTrue(rs.body)


if __name__ == '__main__':
    unittest.main()

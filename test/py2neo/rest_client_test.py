#/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
PY3K = sys.version_info[0] >= 3

import socket
import unittest

from py2neo import rest


__author__    = "Nigel Small <py2neo@nigelsmall.org>"
__copyright__ = "Copyright 2011 Nigel Small"
__license__   = "Apache License, Version 2.0"


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

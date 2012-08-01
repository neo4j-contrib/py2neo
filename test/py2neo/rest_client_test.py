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
        status, location, headers, data = rest_client.get(uri)
        self.assertEqual(200, status)
        self.assertEqual("http://localhost:7474/db/data/", location)
        self.assertTrue(data)

    def test_rest_client_with_redirect(self):
        rest_client = rest.Client()
        uri = "http://localhost:7474/db/data"
        status, location, headers, data = rest_client.get(uri)
        self.assertEqual(200, status)
        self.assertEqual("http://localhost:7474/db/data/", location)
        self.assertTrue(data)

    def test_rest_client_with_bad_path(self):
        rest_client = rest.Client()
        uri = "http://localhost:7474/foo/"
        status, location, headers, data = rest_client.get(uri)
        self.assertEqual(404, status)
        self.assertEqual("http://localhost:7474/foo/", location)
        self.assertFalse(data)

    def test_rest_client_with_bad_host(self):
        rest_client = rest.Client()
        uri = "http://localtoast:7474/db/data/"
        self.assertRaises(socket.error, rest_client.get, uri)

    def test_rest_client_with_bad_port(self):
        rest_client = rest.Client()
        uri = "http://localhost:7575/db/data/"
        self.assertRaises(socket.error, rest_client.get, uri)

    def test_rest_client_with_post(self):
        rest_client = rest.Client()
        uri = "http://localhost:7474/db/data/node"
        status, location, headers, data = rest_client.post(uri, {
            "name": "Alice",
        })
        self.assertEqual(201, status)
        self.assertEqual("http://localhost:7474/db/data/node", location)
        self.assertTrue(dict(headers)["location"].startswith("http://localhost:7474/db/data/node/"))
        self.assertTrue(data)


if __name__ == '__main__':
    unittest.main()

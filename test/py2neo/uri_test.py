#/usr/bin/env python
# -*- coding: utf-8 -*-

import sys

PY3K = sys.version_info[0] >= 3

__author__    = "Nigel Small <py2neo@nigelsmall.org>"
__copyright__ = "Copyright 2011 Nigel Small"
__license__   = "Apache License, Version 2.0"


from py2neo import rest, neo4j
import unittest


class URITest(unittest.TestCase):

    def setUp(self):
        pass

    def test_graph_db_uri_construction(self):
        uri = rest.URI("http://localhost:7474/db/data/", "/")
        self.assertEqual("http://localhost:7474/db/data", uri.base)
        self.assertEqual("/", uri.reference)

    def test_node_uri_construction(self):
        uri = rest.URI("http://localhost:7474/db/data/node", "/node")
        self.assertEqual("http://localhost:7474/db/data", uri.base)
        self.assertEqual("/node", uri.reference)
        uri = rest.URI("http://localhost:7474/db/data/node/123", "/node")
        self.assertEqual("http://localhost:7474/db/data", uri.base)
        self.assertEqual("/node/123", uri.reference)

    def test_relationship_uri_construction(self):
        uri = rest.URI("http://localhost:7474/db/data/relationship", "/relationship")
        self.assertEqual("http://localhost:7474/db/data", uri.base)
        self.assertEqual("/relationship", uri.reference)
        uri = rest.URI("http://localhost:7474/db/data/relationship/123", "/relationship")
        self.assertEqual("http://localhost:7474/db/data", uri.base)
        self.assertEqual("/relationship/123", uri.reference)

    def test_node_index_uri_construction(self):
        uri = rest.URI("http://localhost:7474/db/data/index/node/people", "/index")
        self.assertEqual("http://localhost:7474/db/data", uri.base)
        self.assertEqual("/index/node/people", uri.reference)

    def test_relationship_index_uri_construction(self):
        uri = rest.URI("http://localhost:7474/db/data/index/relationship/friendships", "/index")
        self.assertEqual("http://localhost:7474/db/data", uri.base)
        self.assertEqual("/index/relationship/friendships", uri.reference)


class IndirectURITest(unittest.TestCase):

    def setUp(self):
        pass

    def test_graph_db_uri_construction(self):
        graph_db = neo4j.GraphDatabaseService("http://localhost:7474/db/data/")
        self.assertEqual("http://localhost:7474/db/data/", graph_db._uri)
        self.assertEqual("http://localhost:7474/db/data", graph_db._uri.base)
        self.assertEqual("/", graph_db._uri.reference)

    def test_node_uri_construction(self):
        node = neo4j.Node("http://localhost:7474/db/data/node/123")
        self.assertEqual("http://localhost:7474/db/data/node/123", node._uri)
        self.assertEqual("http://localhost:7474/db/data", node._uri.base)
        self.assertEqual("/node/123", node._uri.reference)

    def test_relationship_uri_construction(self):
        rel = neo4j.Relationship("http://localhost:7474/db/data/relationship/123")
        self.assertEqual("http://localhost:7474/db/data/relationship/123", rel._uri)
        self.assertEqual("http://localhost:7474/db/data", rel._uri.base)
        self.assertEqual("/relationship/123", rel._uri.reference)


if __name__ == '__main__':
    unittest.main()


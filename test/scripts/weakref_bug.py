#!/usr/bin/env python
# -*- encoding: utf-8 -*-

"""
Test code for https://github.com/nigelsmall/py2neo/issues/391
"""

import threading
import py2neo
import random
from py2neo import Graph, Node, Relationship

def worker(graph):
    """
        target function for thread
        generates an edge between two somewhat randomly selected test nodes
    """
    nodes = list(graph.find('test'))
    source = random.choice(nodes[:len(nodes) // 2])
    target = random.choice(nodes[len(nodes) // 2 + 1:])


    relationship = Relationship(source, 'TEST_CONNECT', target)
    graph.create(relationship)

def populate(graph):
    """
        called once to populate graph with test nodes
    """
    for i in range(10):
        n = Node('test')
        graph.create(n)

    nodes = list(graph.find('test'))

    # need to create the edges in the main thread in order
    # to generate the error; not sure why
    for i in range(3):
        source = random.choice(nodes[:len(nodes) // 2])
        target = random.choice(nodes[len(nodes) // 2 + 1:])
        r = Relationship(source, 'TEST_CONNECT', target)
        graph.create(r)

def generate_bug():
    graph = Graph("http://neo4j:password@localhost:7474/db/data/")
    populate(graph)

    thread_pool = []
    for thread_id in range(3):
        t = threading.Thread(name=thread_id, target=worker, args=(graph,))
        t.start()
        thread_pool.append(t)

    for thread in thread_pool:
        t.join()


    for r in graph.match(None,'TEST_CONNECT', None):
        print(r)

if __name__ == '__main__':
    generate_bug()

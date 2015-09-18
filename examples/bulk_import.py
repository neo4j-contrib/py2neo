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


"""
This module creates a random graph of a specified size that has
characteristics of a simple social network. For the size given, that
many nodes will be created with random properties; a similar (slightly
smaller) number of relationships will also be created, the number
differing due to the use of CREATE UNIQUE and the chance of clashes.
Creation of nodes and relationships alternates.

The main aim of this module is to allow exploration of varying
transaction and process batch size to see its effect on overall load
time. Each run consists of a number of transactions of `tx_size`
statements, each of which will send a batch of statements to the
server for processing every `rq_size` statements. Therefore, for a
total size of 1_200_000 nodes, a transaction size of 20_000 and a
process interval of 1_000, 60 transactions will be committed, each
over 20 individual HTTP requests.

Once created, the data can be explored in the browser using a query
such as `MATCH (p:Person {user_id:1}) RETURN p`.
"""


from __future__ import division, print_function

import random
from time import time

from py2neo import Graph, GraphError
from py2neo.http.cypher import CreateNode

CONSONANTS = "bcdfghjklmnprstvwz"
VOWELS = "aeiou"

CREATE_UNIQUE_RELATIONSHIP = """\
MATCH (a:Person) WHERE a.user_id = {A}
MATCH (b:Person) WHERE b.user_id = {B}
CREATE UNIQUE (a)-[:FOLLOWS]->(b)
"""


def random_name_generator():
    while True:
        words = []
        for n in range(2):
            word = [random.choice(CONSONANTS).upper()]
            for syllable in range(random.randint(1, 4)):
                word.append(random.choice(VOWELS))
                word.append(random.choice(CONSONANTS))
            words.append("".join(word))
        yield " ".join(words)


random_name = random_name_generator()


class CreatePerson(CreateNode):

    def __init__(self, user_id):
        CreateNode.__init__(self, "Person", user_id=user_id,
                            name=next(random_name), born=random.randint(1900, 1999))


class RandomGraphGenerator(object):

    def __init__(self, graph):
        self.graph = graph
        try:
            self.graph.schema.create_uniqueness_constraint("Person", "user_id")
        except GraphError:
            print("Finding highest user_id\r", end="", flush=True)
            self.max_user_id = graph.cypher.execute_one("MATCH (p:Person) RETURN max(p.user_id)")
            print("Highest user_id is %d" % self.max_user_id)
        else:
            self.max_user_id = 0

    def create_nodes(self, count, process_every):
        """ Create a number of nodes in a single Cypher transaction.
        """
        tx = self.graph.cypher.begin()
        for i in range(1, count + 1):
            self.max_user_id += 1
            tx.append(CreatePerson(self.max_user_id))
            if i % process_every == 0:
                if i < count:
                    tx.process()
                print("Created %d nodes\r" % i, end="", flush=True)
        tx.commit()

    def create_unique_relationships(self, count, process_every):
        """ Create a number of unique relationships in a single Cypher transaction.
        """
        tx = self.graph.cypher.begin()
        for i in range(1, count + 1):
            start_user_id = random.randint(1, self.max_user_id)
            end_user_id = start_user_id
            while end_user_id == start_user_id:
                end_user_id = random.randint(1, self.max_user_id)
            parameters = {
                "A": start_user_id,
                "B": end_user_id,
            }
            tx.append(CREATE_UNIQUE_RELATIONSHIP, parameters)
            if i % process_every == 0:
                if i < count:
                    tx.process()
                print("Created %d unique relationships\r" % i, end="", flush=True)
        tx.commit()


def main():
    total_size = 1200000  # divisible in lots of ways
    tx_size = 6000        # commit frequency (balance between no of commits and commit size)
    rq_size = 1000        # process frequency (balance between no of requests and request size)
    graph = Graph()
    print("Creating %d nodes and %d unique relationships in transactions of %d "
          "and processing every %d" % (total_size, total_size, tx_size, rq_size))
    generator = RandomGraphGenerator(graph)
    t0 = time()
    for i in range(total_size // tx_size):
        # Create nodes
        t1 = time()
        generator.create_nodes(tx_size, rq_size)
        t2 = time()
        print("Created %d nodes in %f seconds" % (tx_size, t2 - t1))
        # Create unique relationships
        t3 = time()
        generator.create_unique_relationships(tx_size, rq_size)
        t4 = time()
        print("Created %d unique relationships in %f seconds" % (tx_size, t4 - t3))
    t5 = time()
    print("Entire bulk import took %f seconds" % (t5 - t0))


if __name__ == "__main__":
    main()

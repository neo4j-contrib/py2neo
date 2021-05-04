#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright 2011-2021, Nigel Small
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


from multiprocessing import Process

from py2neo import Graph


class Worker(Process):

    graph = None

    n_nodes = 0

    def setup(self, uri, n_nodes):
        self.graph = Graph(uri)
        self.n_nodes = n_nodes

    def run(self):
        tx = self.graph.begin()
        for n in range(self.n_nodes):
            tx.run("CREATE (n:Thing {n:$n})", n=n)
        tx.commit()
        self.graph.service.connector.close()


def test_multiple_processes(uri):
    n_workers = 20
    n_nodes_per_worker = 100
    workers = []
    for i in range(n_workers):
        worker = Worker()
        worker.setup(uri, n_nodes_per_worker)
        workers.append(worker)
    for worker in workers:
        worker.start()
    for worker in workers:
        worker.join()
    g = Graph(uri)
    count = g.evaluate("MATCH (n:Thing) RETURN count(n)")
    assert count == n_workers * n_nodes_per_worker

#!/usr/bin/env python
# coding: utf-8

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


import random
from threading import Thread
from time import perf_counter

from py2neo import Graph
from pansi.console import Console


class Bench(Console):

    def __init__(self, graph, max_tx=10000, timeout=10.0, concurrency=10):
        super(Bench, self).__init__("py2neo", verbosity=0)
        self.graph = graph
        self.max_tx = max_tx
        self.timeout = timeout
        self.concurrency = concurrency
        self.work = []
        self.threads = []
        # stats
        self.t0 = None
        self.time = None
        self.good_tx = 0
        self.bad_tx = 0
        self.per_server = {}

    @property
    def num_tx(self):
        return self.good_tx + self.bad_tx

    def expired(self):
        return self.num_tx >= self.max_tx or \
               (perf_counter() - self.t0) >= self.timeout

    def transact(self):
        mode, work = random.choice(self.work)
        try:
            out = self.graph.play(work, readonly=(mode == "ro"))
        except Exception:
            self.bad_tx += 1
        else:
            for summary in out:
                self.per_server.setdefault(summary.profile, {"good": 0, "bad": 0, "times": []})
                self.per_server[summary.profile]["good"] += 1
                self.per_server[summary.profile]["times"].append(summary.time)
            self.good_tx += 1

    def start(self):
        for i in range(self.concurrency):
            self.threads.append(Worker(self))
        self.t0 = perf_counter()
        for thread in self.threads:
            thread.start()

    def join(self):
        for thread in self.threads:
            thread.join()
        self.time = perf_counter() - self.t0

    def report(self):
        # TODO: phase output by routing table changes,
        #  e.g. Phase 1 with table X, Phase 2 with table Y
        print("Profile: {}".format(self.graph.service.profile))
        print("Total tx: {} ({} good, {} bad)".format(self.num_tx, self.good_tx, self.bad_tx))
        print("Time taken: {:.03f} seconds".format(self.time))
        print("Tx/sec: {:.03f}".format(self.num_tx / self.time))
        print("Bytes sent: {}".format(self.graph.service.connector.bytes_sent))
        print("Bytes received: {}".format(self.graph.service.connector.bytes_received))
        print()
        for profile in sorted(self.per_server, key=lambda p: p.address):
            stats = self.per_server[profile]
            print(profile.address)
            # FIXME: "bad" not actually recorded yet
            print("  Total tx: %r (%r good, %r bad)" % (stats["good"] + stats["bad"],
                                                        stats["good"], stats["bad"]))
            print("  Avg tx time: {:.03f}s".format(sum(stats["times"]) / len(stats["times"])))
            print("  Min tx time: {:.03f}s".format(min(stats["times"])))
            print("  Max tx time: {:.03f}s".format(max(stats["times"])))
            print()


class Worker(Thread):

    def __init__(self, bench):
        super(Worker, self).__init__()
        self.bench = bench

    def run(self):
        while not self.bench.expired():
            self.bench.transact()


def rw1(tx):
    tx.run("CREATE ()").data()


def ro1(tx):
    tx.run("RETURN 1").data()


def ro2(tx):
    tx.run("UNWIND range(1, 1000) AS n RETURN n").data()


if __name__ == "__main__":
    g = Graph("bolt://localhost:17601", auth=("neo4j", "password2"), routing=True)
    b = Bench(g)
    b.work.append(("rw", rw1))
    b.work.append(("ro", ro1))
    b.work.append(("ro", ro2))
    b.start()
    b.join()
    b.report()

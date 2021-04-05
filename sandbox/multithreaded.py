from collections import deque
from random import uniform, choices, randint
from threading import Thread
from time import sleep

from monotonic import monotonic

from py2neo import Graph, Neo4jError, TransactionFailed

#from logging import basicConfig, DEBUG; basicConfig(level=DEBUG)


graph = Graph(port=17601, routing=True, max_size=50)

stats = {
    "write_successes": 0,
    "write_failures": deque(),
    "read_successes": 0,
    "read_failures": deque(),
}


class WriteThread(Thread):

    def __init__(self):
        super(WriteThread, self).__init__()
        self.running = True

    def run(self):
        while self.running:
            try:
                graph.play(self.merge_node)
            except TransactionFailed as failure:
                stats["write_failures"].append((monotonic(), failure))
                #raise
            else:
                stats["write_successes"] += 1
            finally:
                sleep(uniform(0.0, 0.1))

    @classmethod
    def merge_node(cls, tx):
        tx.update("MERGE (a:Thing {xid: 0})")


class ReadThread(Thread):

    def __init__(self):
        super(ReadThread, self).__init__()
        self.running = True

    def run(self):
        while self.running:
            try:
                if randint(1, 10000) < 10000:  # 1 in every 10000 queries will fail
                    graph.read("RETURN 1")
                else:
                    graph.read("XXXXXXXX")
            except Neo4jError as failure:
                stats["read_failures"].append((monotonic(), failure))
                #raise
            else:
                stats["read_successes"] += 1
            finally:
                sleep(uniform(0.0, 0.1))


class Monitor(Thread):

    def __init__(self):
        super(Monitor, self).__init__()
        self.running = True

    def run(self):
        while self.running:
            print(graph.service.connector)
            print("(%d reads, %d failed; %d writes, %d failed)" % (
                stats["read_successes"], len(stats["read_failures"]),
                stats["write_successes"], len(stats["write_failures"])))
            print()
            sleep(1.0)


if __name__ == "__main__":
    threads = []
    for i in range(120):
        cls = choices([ReadThread, WriteThread], weights=[4, 1])[0]
        threads.append(cls())
    monitor = Monitor()
    monitor.start()
    try:
        for thread in threads:
            thread.start()
    except KeyboardInterrupt:
        for thread in threads:
            thread.running = False
        for thread in threads:
            thread.join()
        monitor.running = False
        monitor.join()

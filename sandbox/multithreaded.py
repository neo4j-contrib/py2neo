from collections import deque
from random import uniform, choices, randint
from threading import Thread
from time import sleep

from monotonic import monotonic

from py2neo import Graph, Neo4jError, ServiceUnavailable


graph = Graph(port=17601, routing=True, routing_refresh_ttl=5.0, max_size=50)

stats = {
    "write_available": None,
    "write_successes": 0,
    "write_failures": deque(),
    "read_available": None,
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
                if randint(1, 10000) < 10000:  # 1 in every 10000 queries will fail
                    graph.update("MERGE (a:Thing {xid: 0})")
                else:
                    graph.update("XXXXXXXXXXXXXXXXXXXXXXXX")
            except Neo4jError as failure:
                stats["write_available"] = True
                stats["write_failures"].append((monotonic(), failure))
                #raise
            except ServiceUnavailable:
                stats["write_available"] = False
            except Exception:
                print(4)
                raise
            else:
                stats["write_available"] = True
                stats["write_successes"] += 1
            finally:
                sleep(uniform(0.0, 0.1))
        print(7)


class ReadThread(Thread):

    def __init__(self):
        super(ReadThread, self).__init__()
        self.running = True

    def run(self):
        while self.running:
            value = None
            try:
                if randint(1, 10000) < 10000:  # 1 in every 10000 queries will fail
                    value = graph.query("RETURN 1").evaluate()
                else:
                    graph.query("XXXXXXXX")
            except Neo4jError as failure:
                stats["read_available"] = True
                stats["read_failures"].append((monotonic(), failure))
                #raise
            except ServiceUnavailable as error:
                stats["read_available"] = False
            except Exception as error:
                raise
            else:
                stats["read_available"] = True
                try:
                    assert value == 1
                except AssertionError as error:
                    stats["read_failures"].append((monotonic(), error))
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

            if stats["read_available"]:
                print("Reads: %d (%d failed)" % (
                    stats["read_successes"], len(stats["read_failures"])))
            else:
                print("Reads unavailable")

            if stats["write_available"]:
                print("Writes: %d (%d failed)" % (
                    stats["write_successes"], len(stats["write_failures"])))
            else:
                print("Writes unavailable")

            print()
            sleep(1.0)


def main():
    monitor = Monitor()
    monitor.start()
    threads = []
    for i in range(120):
        cls = choices([ReadThread, WriteThread], weights=[4, 1])[0]
        threads.append(cls())
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


if __name__ == "__main__":
    main()

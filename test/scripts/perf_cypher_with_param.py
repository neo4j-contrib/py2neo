#!/usr/bin/env python
# -*- encoding: utf-8 -*-


from py2neo import Graph


graph = Graph("http://neo4j:password@localhost:7474/db/data/")
cypher = graph.cypher

cypher.execute_one("MERGE (a:Thing {foo:123})")
statement = "MATCH (a:Thing) WHERE a.foo IN {values} RETURN a"
parameters = {"values": [123]}


def main(count):
    for i in range(count):
        _ = cypher.execute(statement, parameters)


if __name__ == "__main__":
    main(10000)

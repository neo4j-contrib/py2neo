from logging import basicConfig, DEBUG

from py2neo import Graph

basicConfig(level=DEBUG)

g = Graph("bolt://localhost:7687", auth=("neo4j", "password"))

cnt = 0
query = """
  UNWIND range(1, 1000000) AS row
  RETURN row, [_ IN range(1, 256) | rand()] as fauxEmbedding
"""
for result in g.query(query):
    cnt = cnt + 1
    if (cnt % 25000 == 0):
        print(f"row {cnt}")

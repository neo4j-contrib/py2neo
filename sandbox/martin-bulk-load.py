from py2neo import Graph
from linetimer import CodeTimer

graph = Graph(scheme="bolt")

graph.run('MATCH (a) DETACH DELETE a')

graph.run("""UNWIND range(0, 250000) as i CREATE (t:Test) 
SET t.a = 'aaaaaaaaa', t.b = 'bbbbbbbbb', t.c = 'ccccccccc'
""")

# large data query
query = 'MATCH (t:Test) RETURN t.a as a, t.b as b, t.c as c'

results = graph.run(query)
with CodeTimer('Iterate over list of results', unit='s'):
    for x in results:
        pass

results = graph.run(query)
with CodeTimer('.to_data_frame()', unit='s'):
    results.to_data_frame()

results = graph.run(query)
with CodeTimer('.data()', unit='s'):
    results.data()

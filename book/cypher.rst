Cypher
======

`Cypher <http://www.neo4j.org/learn/cypher>`_ is to Neo4j as SQL is to
relational databases. It is a declarative language which provides facilities
to query and manipulate data within a graph database using a syntax which is
both quick to learn and easy to read. Py2neo provides a function which allows
Cypher queries to be executed either synchronously or asynchronously within
the ``cypher`` module:

.. autofunction:: py2neo.cypher.execute

Synchronous Execution
---------------------

Simple queries can be executed synchronously with the
:py:func:`execute <py2neo.cypher.execute>` function. This simply requires the
graph database and query to be supplied along with any necessary query
parameters. The following code shows a simple example of Cypher query
execution::

    from py2neo import neo4j, cypher
    graph_db = neo4j.GraphDatabaseService()
    query = "START a=node(1) RETURN a"
    data, metadata = cypher.execute(graph_db, query)
    a = data[0][0]  # first row, first column

Asynchronous Execution
----------------------

The same ``execute`` function can be used to execute queries asynchronously,
processing each row of data as it's returned. This is achieved using a callback
function attached to the ``row_handler`` argument.

Below is an example of an asynchronous query::

    from py2neo import neo4j, cypher
    graph_db = neo4j.GraphDatabaseService()
    query = "START n=node(*) RETURN n"
    def print_node(row):
        print(row[0])
    cypher.execute(graph_db, query, row_handler=print_row)


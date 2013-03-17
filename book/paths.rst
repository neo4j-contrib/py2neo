Paths
=====

Beyond simple nodes and relationships, more advanced structures come into play
when writing Neo4j-based applications. The simplest of these is the *path*
which is a linear sequence of nodes and relationships chained together end to
end. Py2neo provides the :py:class:`Path <py2neo.neo4j.Path>` class which is
used to represent these structures:

.. autoclass:: py2neo.neo4j.Path
    :members:

Paths can be returned from some `Cypher <cypher>`_ queries and are used
internally within :py:func:`create_path <py2neo.neo4j.Node.create_path>`
and :py:func:`get_or_create_path <py2neo.neo4j.Node.get_or_create_path>`.

:mod:`neo4j` Module
===================

.. automodule:: py2neo.neo4j

Connecting to a Graph
---------------------

A default installation of Neo4j will use the URI below for the root of the
graph database service:

.. autoattribute:: py2neo.neo4j.DEFAULT_URI

.. autoclass:: py2neo.neo4j.GraphDatabaseService
    :members:
    :show-inheritance:

Authentication
~~~~~~~~~~~~~~

.. autofunction:: py2neo.neo4j.authenticate

URI Rewriting
~~~~~~~~~~~~~

.. autofunction:: py2neo.neo4j.rewrite

Nodes and Relationships
-----------------------

.. autoclass:: py2neo.neo4j.Node
    :members: abstract, __metadata__, __str__, __uri__, _id,
        create_path, delete, delete_properties, delete_related, exists,
        get_or_create_path, get_properties, is_abstract, isolate, match,
        match_one, set_properties, update_properties

.. autoclass:: py2neo.neo4j.Relationship
    :members: abstract, __metadata__, __str__, __uri__, _id,
        delete, delete_properties, end_node, exists,
        get_properties, is_abstract,
        set_properties, start_node, type, update_properties

Paths
-----

.. autoclass:: py2neo.neo4j.Path
    :members:
    :show-inheritance:

Indexes
-------

.. autoclass:: py2neo.neo4j.Index
    :members:
    :show-inheritance:

Batches
-------

.. autoclass:: py2neo.neo4j.ReadBatch
    :members:
    :inherited-members:

.. autoclass:: py2neo.neo4j.WriteBatch
    :members:
    :inherited-members:

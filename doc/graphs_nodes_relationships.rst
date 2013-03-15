Graphs, Nodes & Relationships
=============================

GraphDatabaseService
--------------------

.. autoclass:: py2neo.neo4j.GraphDatabaseService
    :members:
    :show-inheritance:

Nodes & Relationships
---------------------

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

Graphs, Nodes & Relationships
=============================

GraphDatabaseService
--------------------

.. autoclass:: py2neo.neo4j.GraphDatabaseService
    :members: get_instance, clear, create, cypher, delete, find,
        get_properties, match, match_one, neo4j_version, node, node_labels,
        order, relationship, relationship_types, schema, size,
        supports_index_uniqueness_modes, supports_node_labels, supports_schema_indexes,
        get_indexes, get_index, get_or_create_index, delete_index,
        get_indexed_node, get_or_create_indexed_node, get_indexed_relationship

Nodes & Relationships
---------------------

.. autoclass:: py2neo.neo4j.Node
    :members: abstract, __metadata__, __str__, __uri__, _id,
        add_labels, create_path, delete, delete_properties, delete_related,
        exists, get_or_create_path, get_labels, get_properties, is_abstract,
        isolate, match, match_incoming, match_outgoing, remove_labels,
        set_labels, set_properties, update_properties

.. autoclass:: py2neo.neo4j.Relationship
    :members: abstract, __metadata__, __str__, __uri__, _id,
        delete, delete_properties, end_node, exists, get_properties,
        is_abstract, set_properties, start_node, type, update_properties

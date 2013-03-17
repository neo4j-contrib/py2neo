Batches
=======

Py2neo interacts with Neo4j via its REST API interface and so every interaction
requires a separate HTTP request to be sent. However, when working at scale,
for tasks such as bulk data loading, communication can be prohibitively slow
using this step-by-step approach.

Batches allow multiple requests to be grouped and sent together, cutting down
on network traffic and latency. Such requests also have the advantage of being
executed within a single transaction.

Unfortunately, it is not practical to mix both read and write operations into
a single batch due to restrictions within the underlying implementation. For
this reason, py2neo provides two separate batch classes:
:py:class:`ReadBatch <py2neo.neo4j.ReadBatch>` and
:py:class:`WriteBatch <py2neo.neo4j.WriteBatch>`.
The latter is the more comprehensive, as can be seen below.

.. autoclass:: py2neo.neo4j.ReadBatch
    :members: clear, submit, get_properties, get_indexed_nodes

.. autoclass:: py2neo.neo4j.WriteBatch
    :members: clear, submit, create, delete, delete_properties,
        delete_property, get_or_create, set_properties, set_property,
        add_indexed_node, add_indexed_node_or_fail, add_indexed_relationship,
        add_indexed_relationship_or_fail, create_indexed_node_or_fail,
        create_indexed_relationship_or_fail, get_or_add_indexed_node,
        get_or_add_indexed_relationship, get_or_create_indexed_node,
        get_or_create_indexed_relationship, remove_indexed_node,
        remove_indexed_relationship

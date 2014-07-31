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
:py:class:`ReadBatch <py2neo.batch.ReadBatch>` and
:py:class:`WriteBatch <py2neo.batch.WriteBatch>`.
The latter is the more comprehensive, as can be seen below.

.. autoclass:: py2neo.batch.ReadBatch
    :members: clear, stream, submit, append_cypher, get_indexed_nodes

.. autoclass:: py2neo.batch.WriteBatch
    :members: clear, run, stream, submit, append_cypher, create, create_path,
        delete, delete_properties, delete_property, get_or_create_path,
        set_properties, set_property, add_labels, remove_label, set_labels,
        add_to_index, add_to_index_or_fail, get_or_add_to_index,
        create_in_index_or_fail, get_or_create_in_index, remove_from_index

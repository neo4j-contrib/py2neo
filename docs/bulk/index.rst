***************************************
``py2neo.bulk`` -- Bulk data operations
***************************************

.. automodule:: py2neo.bulk


Bulk Load Operations
====================

Each of the following bulk load functions accepts a transaction object
as its first argument; it is in this transaction that the operation is
carried out. The remainder of the arguments depend on the nature of the
operation.

These functions wrap well-tuned Cypher queries, and can avoid the need
to manually implement these operations. As an example,
:func:`.create_nodes` uses the fast ``UNWIND ... CREATE`` method to
iterate through a list of raw node data and create each node in turn.

.. autofunction:: create_nodes

.. autofunction:: merge_nodes

.. autofunction:: create_relationships

.. autofunction:: merge_relationships

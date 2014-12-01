==========
API: Batch
==========

The Neo4j batch resource is designed to allow multiple REST calls to be passed to the server in a
single request and be executed in a single transaction. While this remains a good choice for some
legacy use cases, many are better served by a `Cypher transaction <py2neo.cypher.CypherTransaction>`_
instead.

Batches offer a limited capability to refer to one job from another **within the same batch**. This
can be useful when building a batch that creates both nodes and relationships between those new
nodes. Note though that certain combinations of such cross-referencing are not possible,
particularly when creating nodes within a legacy index.

Labels and schema indexes are also poorly supported by the batch facility and it is recommended to
use Cypher transactions instead when working with these.


Batch Resource
==============

.. autoclass:: py2neo.batch.BatchResource
   :members:


Batch Instances
===============

.. autoclass:: py2neo.batch.Batch
   :members:

.. autoclass:: py2neo.batch.PullBatch
   :members:

.. autoclass:: py2neo.batch.PushBatch
   :members:


Jobs
====

.. autoclass:: py2neo.batch.Job
   :members:

.. autoclass:: py2neo.batch.JobResult
   :members:

.. autoclass:: py2neo.batch.Target
   :members:

.. autoclass:: py2neo.NodePointer
   :members:


Job Types
---------

.. autoclass:: py2neo.batch.AddNodeLabelsJob
   :members:

.. autoclass:: py2neo.batch.CreateNodeJob
   :members:

.. autoclass:: py2neo.batch.CreatePathJob
   :members:

.. autoclass:: py2neo.batch.CreateRelationshipJob
   :members:

.. autoclass:: py2neo.batch.CreateUniquePathJob
   :members:

.. autoclass:: py2neo.batch.CypherJob
   :members:

.. autoclass:: py2neo.batch.DeleteEntityJob
   :members:

.. autoclass:: py2neo.batch.DeletePropertiesJob
   :members:

.. autoclass:: py2neo.batch.DeletePropertyJob
   :members:

.. autoclass:: py2neo.batch.PullNodeLabelsJob
   :members:

.. autoclass:: py2neo.batch.PullPropertiesJob
   :members:

.. autoclass:: py2neo.batch.PullRelationshipJob
   :members:

.. autoclass:: py2neo.batch.PushNodeLabelsJob
   :members:

.. autoclass:: py2neo.batch.PushPropertiesJob
   :members:

.. autoclass:: py2neo.batch.PushPropertyJob
   :members:

.. autoclass:: py2neo.batch.RemoveNodeLabelJob
   :members:


Exceptions
==========

.. autoclass:: py2neo.batch.BatchError
   :members:

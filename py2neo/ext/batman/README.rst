************************************************
``py2neo.ext.batman`` -- Batch & Manual Indexing
************************************************

Maintained by: Nigel Small <nigel@py2neo.org>

The Neo4j batch resource is designed to allow multiple REST calls to be passed to the server in a
single request and be executed in a single transaction. While this remains a good choice for some
legacy use cases, many are better served by a Cypher :class:`.Transaction` instead.

Batches offer a limited capability to refer to one job from another **within the same batch**. This
can be useful when building a batch that creates both nodes and relationships between those new
nodes. Note though that certain combinations of such cross-referencing are not possible,
particularly when creating nodes within a legacy index.

Labels and schema indexes are also poorly supported by the batch facility and it is recommended to
use Cypher transactions instead when working with these.


Batch Resource
==============

.. autoclass:: py2neo.ext.batman.BatchRunner
   :members:


Batch Instances
===============

.. autoclass:: py2neo.ext.batman.Batch
   :members:


Manual Indexing
===============

.. autoclass:: py2neo.ext.batman.ManualIndexManager
   :members:

.. autoclass:: py2neo.ext.batman.ManualIndex
   :members:

.. autoclass:: py2neo.ext.batman.ManualIndexReadBatch
   :members:

.. autoclass:: py2neo.ext.batman.ManualIndexWriteBatch
   :members:


Jobs
====

.. autoclass:: py2neo.ext.batman.Job
   :members:

.. autoclass:: py2neo.ext.batman.JobResult
   :members:

.. autoclass:: py2neo.ext.batman.Target
   :members:

.. autoclass:: py2neo.ext.batman.NodePointer
   :members:


Job Types
---------

.. autoclass:: py2neo.ext.batman.AddNodeLabelsJob
   :members:

.. autoclass:: py2neo.ext.batman.CreateNodeJob
   :members:

.. autoclass:: py2neo.ext.batman.CreatePathJob
   :members:

.. autoclass:: py2neo.ext.batman.CreateRelationshipJob
   :members:

.. autoclass:: py2neo.ext.batman.CreateUniquePathJob
   :members:

.. autoclass:: py2neo.ext.batman.CypherJob
   :members:

.. autoclass:: py2neo.ext.batman.DeleteEntityJob
   :members:

.. autoclass:: py2neo.ext.batman.DeletePropertiesJob
   :members:

.. autoclass:: py2neo.ext.batman.DeletePropertyJob
   :members:

.. autoclass:: py2neo.ext.batman.PullNodeLabelsJob
   :members:

.. autoclass:: py2neo.ext.batman.PullPropertiesJob
   :members:

.. autoclass:: py2neo.ext.batman.PullRelationshipJob
   :members:

.. autoclass:: py2neo.ext.batman.PushNodeLabelsJob
   :members:

.. autoclass:: py2neo.ext.batman.PushPropertiesJob
   :members:

.. autoclass:: py2neo.ext.batman.PushPropertyJob
   :members:

.. autoclass:: py2neo.ext.batman.RemoveNodeLabelJob
   :members:


Exceptions
==========

.. autoclass:: py2neo.ext.batman.BatchError
   :members:

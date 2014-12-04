===================
API: The Essentials
===================

The ``py2neo`` top-level package contains the functions and classes that are used directly and
ubiquitously across the whole library.


The Graph
=========

.. autoclass:: py2neo.Graph
   :members:
   :inherited-members:


Authentication
==============

Neo4j 2.2 introduces token-based authentication for database servers. To use a server with
authentication enabled, an auth token must first be obtained and then either supplied to the
:func:`.set_auth_token` function or set as the value of the ``NEO4J_AUTH_TOKEN`` environment
variable.

There are two ways to set up authentication for a new server installation:

1. Set an initial password for the ``neo4j`` user.
2. Copy auth details from another (initialised) server.

Py2neo provides a command line tool to help with setting the password and retrieving the auth
token. For a new installation, use::

    $ neoauth neo4j neo4j my-p4ssword
    4ff5167fbeedb3082974c3695bc948dc

For subsequent usage, after the initial password has been set::

    $ neoauth neo4j my-p4ssword
    4ff5167fbeedb3082974c3695bc948dc

Alternatively, authentication can be disabled completely by editing the value of the
``dbms.security.authorization_enabled`` setting in the ``conf/neo4j-server.properties`` file.

.. autofunction:: py2neo.set_auth_token


Nodes
=====

.. autoclass:: py2neo.Node
   :members:
   :inherited-members:


Relationships
=============

.. autoclass:: py2neo.Relationship
   :members:
   :inherited-members:

.. autoclass:: py2neo.Rel
   :members:
   :inherited-members:

.. autoclass:: py2neo.Rev
   :members:
   :inherited-members:


Paths
=====

.. autoclass:: py2neo.Path
   :members:
   :inherited-members:


Labels & Properties
===================

.. autoclass:: py2neo.LabelSet
   :members:

.. autoclass:: py2neo.PropertySet
   :members:


Exceptions
==========

.. autoexception:: py2neo.BindError
   :members:

.. autoexception:: py2neo.Finished
   :members:

.. autoexception:: py2neo.GraphError
   :members:

.. autoexception:: py2neo.JoinError
   :members:

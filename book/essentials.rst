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

Neo4j 2.2 introduces optional authentication for database servers, enabled by default.
To use a server with authentication enabled, a user name and password must be specified for the `host:port` combination.
This can either be passed in code using the :func:`.authenticate` function or specified in the  ``NEO4J_AUTH`` environment variable.
By default the user name and password are ``neo4j`` and ``neo4j`` respectively.
This default password generally requires an initial change before the database can be used.

There are two ways to set up authentication for a new server installation:

1. Set an initial password for the ``neo4j`` user.
2. Copy auth details from another (initialised) server.

Py2neo provides a command line tool to help with changing user passwords as well as checking whether a password change is required.
For a new installation, use::

    $ neoauth neo4j neo4j my-p4ssword
    Password change succeeded

After a password has been set, the tool can also be used to validate credentials::

    $ neoauth neo4j my-p4ssword
    Password change not required

Alternatively, authentication can be disabled completely by editing the value of the ``dbms.security.authorization_enabled`` setting in the ``conf/neo4j-server.properties`` file.

.. autofunction:: py2neo.authenticate


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

**********************
The Py2neo v5 Handbook
**********************

**Py2neo** is a client library and toolkit for working with Neo4j_ from within Python_ applications and from the command line.
The library supports both Bolt and HTTP and provides a high level API, an OGM, admin tools, an interactive console, a Cypher lexer for Pygments, and many other bells and whistles.
Unlike previous releases, Py2neo does not require an HTTP-enabled server and can work entirely through Bolt.

When considering whether to use py2neo or the `official Python Driver for Neo4j <https://github.com/neo4j/neo4j-python-driver>`_, there is a trade-off to be made.
Py2neo offers a higher level API and an OGM, but the official driver provides mechanisms to work with clusters, such as automatic retries.
If you are new to Neo4j, need an OGM, do not want to learn Cypher immediately, or require data science integrations, py2neo may be the better choice.
If you are building a high-availability Enterprise application, or are using a cluster, you likely need the official driver.


Installation
============

To install the latest stable release of py2neo, simply use pip_::

    $ pip install --upgrade py2neo


To install the latest pre-release of py2neo, use pip_ with the ``--pre`` flag::

    $ pip install --upgrade --pre py2neo


Requirements
============

The following versions of Python and Neo4j are supported:

- Python 2.7 / 3.5 / 3.6 / 3.7 / 3.8
- Neo4j 3.4 / 3.5 / 4.0 (the latest point release of each version is recommended)

While either Neo4j Community or Enterprise edition may be used, py2neo does not yet fully support all Enterprise-only features, such as `Causal Clustering`_.
Py2neo does however provide support for the multi-database functionality added in Neo4j 4.0.
More about this can be found in the documentation for the :class:`.Graph` class.

Note that Py2neo is developed and tested under **Linux** using standard CPython distributions.
While other operating systems and Python distributions may work, support for these is not available.


Core Graph API
==============
Py2neo consists of several layers of API, and at the heart of those is the Graph API.
This has evolved from the original, foundational API included with early versions of the library, and remains relevant for general purpose use today.
The backbone of this API is the :class:`.Graph` class, which represents a graph database exposed by a Neo4j service running on a single instance or cluster.
The service itself is represented by a :class:`.GraphService` object.

:class:`.Node` and :class:`.Relationship` objects are also key to this API, both of which extend the :class:`.Subgraph` class.
A comprehensive set of graph structure data types and operations are provided, allowing great flexibility in how graph data can be used.

.. toctree::
   :maxdepth: 2
   :numbered:

   data
   database
   matching


Cypher Tools
============

.. toctree::
   :maxdepth: 2
   :numbered:

   cypher/index
   cypher/lexer


Object-Graph Mapping
====================

.. toctree::
   :maxdepth: 2
   :numbered:

   ogm


Clients & Servers
=================

.. toctree::
   :maxdepth: 2
   :numbered:

   client/index
   client/config
   client/bolt
   client/http
   server/index
   wiring


Command Line Usage
==================

.. toctree::
   :maxdepth: 2
   :numbered:

   cli


.. _Neo4j: https://neo4j.com/
.. _pip: https://pip.pypa.io/
.. _Python: https://www.python.org/
.. _Causal Clustering: https://neo4j.com/docs/operations-manual/current/clustering/

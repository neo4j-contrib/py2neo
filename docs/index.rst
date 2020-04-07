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

To install the latest stable version of py2neo, simply use pip_::

    $ pip install py2neo


To install the latest bleeding edge code directly from GitHub, use::

    $ pip install git+https://github.com/technige/py2neo.git#egg=py2neo

Note that code installed directly from GitHub is likely to be unstable.
Your mileage may vary.


Requirements
============

The following versions of Python and Neo4j are supported:

- Python 2.7 / 3.5 / 3.6 / 3.7 / 3.8
- Neo4j 3.2 / 3.3 / 3.4 / 3.5 / 4.0 (the latest point release of each version is recommended)

While either Neo4j Community or Enterprise edition may be used, py2neo does not yet fully support all Enterprise-only features, such as `Causal Clustering`_.

Note also that Py2neo is developed and tested under **Linux** using standard CPython distributions.
While other operating systems and Python distributions may work, support for these is not available.


Library Reference
=================

.. toctree::
   :maxdepth: 2
   :numbered:

   data
   database
   matching
   ogm
   cypher/index
   cypher/lexer
   cli


.. _Neo4j: https://neo4j.com/
.. _pip: https://pip.pypa.io/
.. _Python: https://www.python.org/
.. _Causal Clustering: https://neo4j.com/docs/operations-manual/current/clustering/

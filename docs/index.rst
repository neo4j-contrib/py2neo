*******************
The Py2neo Handbook
*******************

**Py2neo** is a client library and toolkit for working with Neo4j_ from within Python_ applications and from the command line.
The library supports both Bolt and HTTP and provides a high level API, an OGM, admin tools, an interactive console, a Cypher lexer for Pygments, and many other bells and whistles.

As of version 2021.1, Py2neo contains full support for routing, as exposed by a Neo4j cluster.
This can be enabled using a ``neo4j://...`` URI or by passing ``routing=True`` to a :class:`.Graph` constructor.

Remember to take a look at the full :ref:`release notes <Version 2021.1>` for version 2021.1.


Quick Example
=============

To run a query against a local database is straightforward::

    >>> from py2neo import Graph
    >>> graph = Graph("bolt://localhost:7687", auth=("neo4j", "password"))
    >>> graph.run("UNWIND range(1, 3) AS n RETURN n, n * n as n_sq")
       n | n_sq
    -----|------
       1 |    1
       2 |    4
       3 |    9


Releases & Versioning
=====================

As of 2020, py2neo has switched to `Calendar Versioning <https://calver.org/>`_, using a scheme of ``YYYY.N.M``.
Here, ``N`` is an incrementing zero-based number for each year, and ``M`` is a revision within that version (also zero-based).

No compatibility guarantees are given between versions, but as a general rule, a change in ``M`` should require little-to-no work within client applications,
whereas a change in ``N`` may require some work. A change to the year is likely to require a more significant amount of work to upgrade.

Note that py2neo is developed on a rolling basis, so patches are not made to old versions.
Users will instead need to install the latest release to adopt bug fixes.


Installation
============

To install the latest release of py2neo, simply use:

.. code-block:: bash

    $ pip install --upgrade py2neo

To install the latest stable code from the GitHub master branch, use:

.. code-block:: bash

    $ pip install git+https://github.com/technige/py2neo.git@master#egg=py2neo


Requirements
============

The following versions of Python and Neo4j (all editions) are supported:

- Python 2.7 / 3.4 / 3.5 / 3.6 / 3.7 / 3.8 / 3.9
- Neo4j 3.4 / 3.5 / 4.0 / 4.1 / 4.2 / 4.3 (the latest point release of each version is recommended)

Py2neo provides support for the multi-database functionality added in Neo4j 4.0.
More about this can be found in the documentation for the :class:`.Graph` class.

Note that Py2neo is developed and tested under **Linux** using standard CPython distributions.
While other operating systems and Python distributions may work, support for these is not available.

Core Graph API
==============

.. automodule:: py2neo
    :noindex:

.. toctree::
    :maxdepth: 2

    profiles
    workflow
    matching
    errors


Data Types
==========

The :class:`.Node` and :class:`.Relationship` objects are key to this library, both of which extend the :class:`.Subgraph` class.
A comprehensive set of graph structure data types and operations are provided, allowing great flexibility in how graph data can be used.

.. toctree::
    :maxdepth: 2

    data/index
    data/spatial


Cypher
======

.. toctree::
    :maxdepth: 2

    cypher/index
    cypher/lexer
    cypher/queries


Bulk Operations
===============

.. toctree::
    :maxdepth: 2

    bulk/index
    bulk/export


Object-Graph Mapping
====================

.. toctree::
    :maxdepth: 2

    ogm/index
    ogm/models/index


Python DB API 2.0 Compatibility
===============================

.. toctree::
    :maxdepth: 2

    pep249/index


Command Line Tools
==================

.. toctree::
    :maxdepth: 2

    cli


.. _Neo4j: https://neo4j.com/
.. _pip: https://pip.pypa.io/
.. _Python: https://www.python.org/
.. _Causal Clustering: https://neo4j.com/docs/operations-manual/current/clustering/

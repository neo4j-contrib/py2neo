*******************
The Py2neo Handbook
*******************

**Py2neo** is a client library and toolkit for working with Neo4j_ from within Python_ applications and from the command line.
The library supports both Bolt and HTTP and provides a high level API, an OGM, admin tools, an interactive console, a Cypher lexer for Pygments, and many other bells and whistles.
Unlike previous releases, Py2neo does not require an HTTP-enabled server and can work entirely through Bolt.

When considering whether to use py2neo or the `official Python Driver for Neo4j <https://github.com/neo4j/neo4j-python-driver>`_, there is a trade-off to be made.
Py2neo offers a larger surface, with both a higher level API and an OGM, but the official driver is fully supported by Neo4j.
If you are new to Neo4j, need an OGM, do not want to learn Cypher immediately, or require data science integrations, py2neo may be the better choice.
If you are in an Enterprise environment where you require support, you likely need the official driver.

As of version 2020.1.0, Py2neo contains **experimental** Bolt routing support, enabled using ``g = Graph(..., routing=True)``.
Constructive feedback on this feature is very welcome, but note that it is not yet guaranteed to be stable in a production environment.


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
- Neo4j 3.4 / 3.5 / 4.0 / 4.1 (the latest point release of each version is recommended)

Py2neo provides support for the multi-database functionality added in Neo4j 4.0.
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

    database/index
    database/work
    matching
    data/index
    data/spatial
    data/operations


Object-Graph Mapping
====================

.. toctree::
    :maxdepth: 2

    ogm/index
    ogm/models/index


Cypher Language Tools
=====================

.. toctree::
    :maxdepth: 2

    cypher/index
    cypher/lexer
    cypher/queries


Command Line Usage
==================

.. toctree::
    :maxdepth: 2

    cli


.. _Neo4j: https://neo4j.com/
.. _pip: https://pip.pypa.io/
.. _Python: https://www.python.org/
.. _Causal Clustering: https://neo4j.com/docs/operations-manual/current/clustering/

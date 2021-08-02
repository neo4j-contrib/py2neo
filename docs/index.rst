*******************
The Py2neo Handbook
*******************
.. image:: https://img.shields.io/pypi/v/py2neo.svg
   :target: https://pypi.python.org/pypi/py2neo
   :alt: PyPI version

.. image:: https://img.shields.io/pypi/dm/py2neo
   :target: https://pypi.python.org/pypi/py2neo
   :alt: PyPI Downloads

.. image:: https://img.shields.io/github/license/technige/py2neo.svg
   :target: https://www.apache.org/licenses/LICENSE-2.0
   :alt: License

.. image:: https://coveralls.io/repos/github/technige/py2neo/badge.svg?branch=master
   :target: https://coveralls.io/github/technige/py2neo?branch=master
   :alt: Coverage Status


**Py2neo** is a client library and toolkit for working with Neo4j_ from within Python_ applications.
The library supports both Bolt and HTTP and provides a high level API, an OGM, admin tools, a Cypher lexer for Pygments, and many other bells and whistles.

Command line tooling has been removed from the library in py2neo 2021.2.
This functionality now exists in the separate **ipy2neo** project.

As of version 2021.1, Py2neo contains full support for routing, as exposed by a Neo4j cluster.
This can be enabled using a ``neo4j://...`` URI or by passing ``routing=True`` to a :class:`.Graph` constructor.

Py2neo also provides support for the multi-database functionality added in Neo4j 4.0.
More about this can be found in the documentation for the :class:`.Graph` class.

Remember to take a look at the full :ref:`release notes <Version 2021.2>` for version 2021.2.


Installation & Compatibility
============================

To install the latest release of py2neo, simply use:

.. code-block:: bash

    $ pip install py2neo

The following versions of Python and Neo4j (all editions) are supported:

.. list-table::
    :header-rows: 1

    * - Neo4j
      - Python 3.5+
      - Python 2.7
    * - 4.3
      - |test-neo43-py35+|
      - |test-neo43-py27|
    * - 4.2
      - |test-neo42-py35+|
      - |test-neo42-py27|
    * - 4.1
      - |test-neo41-py35+|
      - |test-neo41-py27|
    * - 4.0
      - |test-neo40-py35+|
      - |test-neo40-py27|
    * - 3.5
      - |test-neo35-py35+|
      - |test-neo35-py27|
    * - 3.4
      - |test-neo34-py35+|
      - |test-neo34-py27|

Note that py2neo is developed and tested under **Linux** using standard CPython distributions.
While other operating systems and Python distributions may work, support for these is not available.


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
    data/time
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


.. _Neo4j: https://neo4j.com/
.. _pip: https://pip.pypa.io/
.. _Python: https://www.python.org/
.. _Causal Clustering: https://neo4j.com/docs/operations-manual/current/clustering/


.. |test-neo43-py27| image:: https://img.shields.io/github/workflow/status/technige/py2neo/test-neo43-py27
   :target: https://github.com/technige/py2neo/actions?query=workflow%3A"test-neo43-py27"
   :alt: GitHub workflow status for tests against Neo4j 4.3 using py27

.. |test-neo43-py35+| image:: https://img.shields.io/github/workflow/status/technige/py2neo/test-neo43-py35+
   :target: https://github.com/technige/py2neo/actions?query=workflow%3A"test-neo43-py35+"
   :alt: GitHub workflow status for tests against Neo4j 4.3 using py35+

.. |test-neo42-py27| image:: https://img.shields.io/github/workflow/status/technige/py2neo/test-neo42-py27
   :target: https://github.com/technige/py2neo/actions?query=workflow%3A"test-neo42-py27"
   :alt: GitHub workflow status for tests against Neo4j 4.2 using py27

.. |test-neo42-py35+| image:: https://img.shields.io/github/workflow/status/technige/py2neo/test-neo42-py35+
   :target: https://github.com/technige/py2neo/actions?query=workflow%3A"test-neo42-py35+"
   :alt: GitHub workflow status for tests against Neo4j 4.2 using py35+

.. |test-neo41-py27| image:: https://img.shields.io/github/workflow/status/technige/py2neo/test-neo41-py27
   :target: https://github.com/technige/py2neo/actions?query=workflow%3A"test-neo41-py27"
   :alt: GitHub workflow status for tests against Neo4j 4.1 using py27

.. |test-neo41-py35+| image:: https://img.shields.io/github/workflow/status/technige/py2neo/test-neo41-py35+
   :target: https://github.com/technige/py2neo/actions?query=workflow%3A"test-neo41-py35+"
   :alt: GitHub workflow status for tests against Neo4j 4.1 using py35+

.. |test-neo40-py27| image:: https://img.shields.io/github/workflow/status/technige/py2neo/test-neo40-py27
   :target: https://github.com/technige/py2neo/actions?query=workflow%3A"test-neo40-py27"
   :alt: GitHub workflow status for tests against Neo4j 4.0 using py27

.. |test-neo40-py35+| image:: https://img.shields.io/github/workflow/status/technige/py2neo/test-neo40-py35+
   :target: https://github.com/technige/py2neo/actions?query=workflow%3A"test-neo40-py35+"
   :alt: GitHub workflow status for tests against Neo4j 4.0 using py35+

.. |test-neo35-py27| image:: https://img.shields.io/github/workflow/status/technige/py2neo/test-neo35-py27
   :target: https://github.com/technige/py2neo/actions?query=workflow%3A"test-neo35-py27"
   :alt: GitHub workflow status for tests against Neo4j 3.5 using py27

.. |test-neo35-py35+| image:: https://img.shields.io/github/workflow/status/technige/py2neo/test-neo35-py35+
   :target: https://github.com/technige/py2neo/actions?query=workflow%3A"test-neo35-py35+"
   :alt: GitHub workflow status for tests against Neo4j 3.5 using py35+

.. |test-neo34-py27| image:: https://img.shields.io/github/workflow/status/technige/py2neo/test-neo34-py27
   :target: https://github.com/technige/py2neo/actions?query=workflow%3A"test-neo34-py27"
   :alt: GitHub workflow status for tests against Neo4j 3.4 using py27

.. |test-neo34-py35+| image:: https://img.shields.io/github/workflow/status/technige/py2neo/test-neo34-py35+
   :target: https://github.com/technige/py2neo/actions?query=workflow%3A"test-neo34-py35+"
   :alt: GitHub workflow status for tests against Neo4j 3.4 using py35+

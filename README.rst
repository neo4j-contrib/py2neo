Py2neo
======

** **Py2neo has now been officially picked up by Neo4j and all code and copyrights have been transferred. In return, Neo4j have very kindly made a generous donation to The Bone Cancer Research Trust <https://www.bcrt.org.uk/>. The new project home is at <https://github.com/neo4j-contrib/py2neo>. I am hugely grateful to everyone who has contributed to and used this project over the years. --Nigel** **

The version history of py2neo on PyPI got lost.
If you need an older version, you can install [`py2neo-history`](https://pypi.org/project/py2neo-history/) instead.

.. image:: https://img.shields.io/pypi/v/py2neo.svg
   :target: https://pypi.python.org/pypi/py2neo
   :alt: PyPI version

.. image:: https://img.shields.io/pypi/dm/py2neo
   :target: https://pypi.python.org/pypi/py2neo
   :alt: PyPI Downloads

.. image:: https://img.shields.io/github/license/neo4j-contrib/py2neo.svg
   :target: https://www.apache.org/licenses/LICENSE-2.0
   :alt: License

.. image:: https://coveralls.io/repos/github/neo4j-contrib/py2neo/badge.svg?branch=master
   :target: https://coveralls.io/github/neo4j-contrib/py2neo?branch=master
   :alt: Coverage Status


**Py2neo** is a client library and toolkit for working with `Neo4j <https://neo4j.com/>`_ from within `Python <https://www.python.org/>`_ applications.
The library supports both Bolt and HTTP and provides a high level API, an OGM, admin tools, a Cypher lexer for Pygments, and many other bells and whistles.

Command line tooling has been removed from the library in py2neo 2021.2.
This functionality now exists in the separate **ipy2neo** project.

As of version 2021.1, py2neo contains full support for routing, as exposed by a Neo4j cluster.
This can be enabled using a ``neo4j://...`` URI or by passing ``routing=True`` to a ``Graph`` constructor.


Installation & Compatibility
----------------------------

To install the latest release of py2neo, simply use:

.. code-block:: bash

    $ pip install py2neo

The following versions of Python and Neo4j (all editions) are supported:

.. list-table::
    :header-rows: 1

    * - Neo4j
      - Python 3.5+
      - Python 2.7
    * - 4.4
      - |test-neo44-py35+|
      - |test-neo44-py27|
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
-------------

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
---------------------

As of 2020, py2neo has switched to `Calendar Versioning <https://calver.org/>`_, using a scheme of ``YYYY.N.M``.
Here, ``N`` is an incrementing zero-based number for each year, and ``M`` is a revision within that version (also zero-based).

No compatibility guarantees are given between versions, but as a general rule, a change in ``M`` should require little-to-no work within client applications,
whereas a change in ``N`` may require some work. A change to the year is likely to require a more significant amount of work to upgrade.

Note that py2neo is developed on a rolling basis, so patches are not made to old versions.
Users will instead need to install the latest release to adopt bug fixes.


More
----

For more information, read the `handbook <http://py2neo.org/>`_.


.. |test-neo44-py27| image:: https://img.shields.io/github/actions/workflow/status/neo4j-contrib/py2neo/test-neo44-py27.yml
   :target: https://github.com/neo4j-contrib/py2neo/actions/workflows/test-neo44-py27.yml?query=branch%3Amaster
   :alt: GitHub workflow status for tests against Neo4j 4.4 using py27

.. |test-neo44-py35+| image:: https://img.shields.io/github/actions/workflow/status/neo4j-contrib/py2neo/test-neo44-py35+.yml
   :target: https://github.com/neo4j-contrib/py2neo/actions/workflows/test-neo44-py35+.yml?query=branch%3Amaster
   :alt: GitHub workflow status for tests against Neo4j 4.4 using py35+

.. |test-neo43-py27| image:: https://img.shields.io/github/actions/workflow/status/neo4j-contrib/py2neo/test-neo43-py27.yml
   :target: https://github.com/neo4j-contrib/py2neo/actions/workflows/test-neo43-py27.yml?query=branch%3Amaster
   :alt: GitHub workflow status for tests against Neo4j 4.3 using py27

.. |test-neo43-py35+| image:: https://img.shields.io/github/actions/workflow/status/neo4j-contrib/py2neo/test-neo43-py35+.yml
   :target: https://github.com/neo4j-contrib/py2neo/actions/workflows/test-neo43-py35+.yml?query=branch%3Amaster
   :alt: GitHub workflow status for tests against Neo4j 4.3 using py35+

.. |test-neo42-py27| image:: https://img.shields.io/github/actions/workflow/status/neo4j-contrib/py2neo/test-neo42-py27.yml
   :target: https://github.com/neo4j-contrib/py2neo/actions/workflows/test-neo42-py27.yml?query=branch%3Amaster
   :alt: GitHub workflow status for tests against Neo4j 4.2 using py27

.. |test-neo42-py35+| image:: https://img.shields.io/github/actions/workflow/status/neo4j-contrib/py2neo/test-neo42-py35+.yml
   :target: https://github.com/neo4j-contrib/py2neo/actions/workflows/test-neo42-py35+.yml?query=branch%3Amaster
   :alt: GitHub workflow status for tests against Neo4j 4.2 using py35+

.. |test-neo41-py27| image:: https://img.shields.io/github/actions/workflow/status/neo4j-contrib/py2neo/test-neo41-py27.yml
   :target: https://github.com/neo4j-contrib/py2neo/actions/workflows/test-neo41-py27.yml?query=branch%3Amaster
   :alt: GitHub workflow status for tests against Neo4j 4.1 using py27

.. |test-neo41-py35+| image:: https://img.shields.io/github/actions/workflow/status/neo4j-contrib/py2neo/test-neo41-py35+.yml
   :target: https://github.com/neo4j-contrib/py2neo/actions/workflows/test-neo41-py35+.yml?query=branch%3Amaster
   :alt: GitHub workflow status for tests against Neo4j 4.1 using py35+

.. |test-neo40-py27| image:: https://img.shields.io/github/actions/workflow/status/neo4j-contrib/py2neo/test-neo40-py27.yml
   :target: https://github.com/neo4j-contrib/py2neo/actions/workflows/test-neo40-py27.yml?query=branch%3Amaster
   :alt: GitHub workflow status for tests against Neo4j 4.0 using py27

.. |test-neo40-py35+| image:: https://img.shields.io/github/actions/workflow/status/neo4j-contrib/py2neo/test-neo40-py35+.yml
   :target: https://github.com/neo4j-contrib/py2neo/actions/workflows/test-neo40-py35+.yml?query=branch%3Amaster
   :alt: GitHub workflow status for tests against Neo4j 4.0 using py35+

.. |test-neo35-py27| image:: https://img.shields.io/github/actions/workflow/status/neo4j-contrib/py2neo/test-neo35-py27.yml
   :target: https://github.com/neo4j-contrib/py2neo/actions/workflows/test-neo35-py27.yml?query=branch%3Amaster
   :alt: GitHub workflow status for tests against Neo4j 3.5 using py27

.. |test-neo35-py35+| image:: https://img.shields.io/github/actions/workflow/status/neo4j-contrib/py2neo/test-neo35-py35+.yml
   :target: https://github.com/neo4j-contrib/py2neo/actions/workflows/test-neo35-py35+.yml?query=branch%3Amaster
   :alt: GitHub workflow status for tests against Neo4j 3.5 using py35+

.. |test-neo34-py27| image:: https://img.shields.io/github/actions/workflow/status/neo4j-contrib/py2neo/test-neo34-py27.yml
   :target: https://github.com/neo4j-contrib/py2neo/actions/workflows/test-neo34-py27.yml?query=branch%3Amaster
   :alt: GitHub workflow status for tests against Neo4j 3.4 using py27

.. |test-neo34-py35+| image:: https://img.shields.io/github/actions/workflow/status/neo4j-contrib/py2neo/test-neo34-py35+.yml
   :target: https://github.com/neo4j-contrib/py2neo/actions/workflows/test-neo34-py35+.yml?query=branch%3Amaster
   :alt: GitHub workflow status for tests against Neo4j 3.4 using py35+

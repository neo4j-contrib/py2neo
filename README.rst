Py2neo
======
.. image:: https://img.shields.io/github/v/release/technige/py2neo?sort=semver
   :target: https://github.com/technige/py2neo
   :alt: GitHub release

.. image:: https://img.shields.io/github/license/technige/py2neo.svg
   :target: https://www.apache.org/licenses/LICENSE-2.0
   :alt: License

.. image:: https://img.shields.io/travis/technige/py2neo/master.svg
   :target: https://travis-ci.org/technige/py2neo
   :alt: Build Status

.. image:: https://coveralls.io/repos/github/technige/py2neo/badge.svg?branch=master
   :target: https://coveralls.io/github/technige/py2neo?branch=master
   :alt: Coverage Status


**Py2neo** is a client library and toolkit for working with `Neo4j <https://neo4j.com/>`_ from within `Python <https://www.python.org/>`_ applications and from the command line.
The library supports both Bolt and HTTP and provides a high level API, an OGM, admin tools, an interactive console, a Cypher lexer for Pygments, and many other bells and whistles.

When considering whether to use py2neo or the `official Python Driver for Neo4j <https://github.com/neo4j/neo4j-python-driver>`_, there is a trade-off to be made.
Py2neo offers a higher level API and an OGM, but the official driver provides mechanisms to work with clusters, such as automatic retries.
If you are new to Neo4j, need an OGM, do not want to learn Cypher immediately, or require data science integrations, py2neo may be the better choice.
If you are building a high-availability Enterprise application, or are using a cluster, you likely need the official driver.


Installation
------------
.. image:: https://img.shields.io/pypi/v/py2neo.svg
   :target: https://pypi.python.org/pypi/py2neo
   :alt: PyPI version

.. image:: https://img.shields.io/pypi/dm/py2neo
   :target: https://pypi.python.org/pypi/py2neo
   :alt: PyPI Downloads

To install the latest release of py2neo, simply use:

.. code-block::

    $ pip install --upgrade py2neo

To install the latest stable code from the GitHub master branch, use:

.. code-block::

    $ pip install git+https://github.com/technige/py2neo.git@master#egg=py2neo


Requirements
------------
.. image:: https://img.shields.io/pypi/pyversions/py2neo.svg
   :target: https://www.python.org/
   :alt: Python versions

.. image:: https://img.shields.io/badge/neo4j-3.4%20%7C%203.5%20%7C%204.0-blue.svg
   :target: https://neo4j.com/
   :alt: Neo4j versions

The following versions of Python and Neo4j are supported:

- Python 2.7 / 3.5 / 3.6 / 3.7 / 3.8
- Neo4j 3.4 / 3.5 / 4.0 (the latest point release of each version is recommended)

While either Neo4j Community or Enterprise edition may be used, py2neo does not yet fully support all Enterprise-only features, such as `Causal Clustering <https://neo4j.com/docs/operations-manual/current/clustering/>`_.
Py2neo does however provide support for the multi-database functionality added in Neo4j 4.0.
More about this can be found in the documentation for the ``Graph`` class.

Note also that Py2neo is developed and tested under **Linux** using standard CPython distributions.
While other operating systems and Python distributions may work, support for these is not available.


More
----

For more information, read the `handbook <http://py2neo.org/>`_.

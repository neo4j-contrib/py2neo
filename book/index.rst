=======================
The Py2neo 2.1 Handbook
=======================

Py2neo is a client library and comprehensive toolkit for working with Neo4j from within Python
applications and from the command line. The core library has no external dependencies and has been
carefully designed to be easy and intuitive to use.

Releases
========

The latest release of py2neo is **2.1.0**.


Requirements
============

- Python 2.7 / 3.3 / 3.4
- Neo4j 2.0 / 2.1 / 2.2 / 2.3 (the latest point release of each version is recommended)

Note that py2neo is developed and tested exclusively under Linux using standard CPython distributions.
While other operating systems and Python distributions may work, I cannot offer support for these.


Installation
============

To install, run the following::

    $ pip install py2neo


Contents
========

.. toctree::
   :maxdepth: 2

   intro
   essentials
   cypher
   schema
   internals
   server
   store
   batch
   legacy
   ext/calendar
   ext/geoff
   ext/gremlin
   ext/neobox
   ext/ogm
   ext/spatial
   cookbook

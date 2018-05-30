**********************
The Py2neo v4 Handbook
**********************

**Py2neo** is a client library and toolkit for working with Neo4j_ from within Python_ applications and from the command line.
The library wraps the official driver adding support for HTTP, a higher level API, an OGM, admin tools, an interactive console, a Cypher lexer for Pygments and many other bells and whistles.
Unlike previous releases, Py2neo v4 no longer requires HTTP and can work entirely through Bolt.

.. note:: Documentation for Py2neo v3 is available `here <http://py2neo.org/v3>`_.


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

- Python 2.7 / 3.4 / 3.5 / 3.6 / 3.7-dev / PyPy
- Neo4j 3.0 / 3.1 / 3.2 / 3.3 / 3.4 (the latest point release of each version is recommended)

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
   console
   admin


.. _Neo4j: https://neo4j.com/
.. _pip: https://pip.pypa.io/
.. _Python: https://www.python.org/

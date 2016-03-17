*****************************
The Py2neo v3 beta 2 Handbook
*****************************

**Py2neo** is a client library and toolkit for working with Neo4j_ from within Python_ applications and from the command line.
The core library has no external dependencies and has been carefully designed to be easy and intuitive to use.


Installation
============

To install the latest stable version of py2neo, simply use pip_::

    $ pip install py2neo


The latest stable release of py2neo is **2.0.8**.
Documentation for the 2.0 series is available `here <http://py2neo.org/2.0>`_.

To install the latest beta, use::

   $ pip install py2neo==3b2

The latest beta release is **3b2**.

To install the latest bleeding edge code directly from GitHub, use::

    $ pip install git+https://github.com/nigelsmall/py2neo.git#egg=py2neo

Note that this code is likely to be unstable.
Your mileage may vary.


Requirements
============

The following versions of Python and Neo4j are supported:

- Python 2.7 / 3.3 / 3.4 / 3.5
- Neo4j 2.0 / 2.1 / 2.2 / 2.3 / 3.0 (the latest point release of each version is recommended)

Note also that Py2neo is developed and tested exclusively under **Linux** using standard CPython distributions.
While other operating systems and Python distributions may work, support for these is not available.


Library Reference
=================

.. toctree::
   :maxdepth: 2
   :numbered:

   types
   database
   neokit
   ext/batman
   ext/calendar
   ext/ogm


----


Miscellany
==========


.. _Neo4j: http://neo4j.com/
.. _pip: https://pip.pypa.io/
.. _Python: https://www.python.org/

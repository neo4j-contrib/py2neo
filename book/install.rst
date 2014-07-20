===============
Getting Started
===============

Requirements
============


Neo4j
-----
Neo4j versions **1.9**, **2.0** and **2.1** are supported.


Python
------
Python versions **2.7**, **3.3** and **3.4** are supported.

Jython version **2.7** is also supported but has been less thoroughly tested.

If you are using a different Python variant such as PyPy or IronPython then please help to improve
support by providing feedback.


Dependencies
------------
Py2neo is self-contained and requires no third-party dependencies to be installed. It does however
come bundled with `HTTPStream <http://nigelsmall.com/httpstream>`_,
`JSONStream <http://nigelsmall.com/jsonstream>`_ and `URIMagic <http://nigelsmall.com/urimagic>`_
which are used internally for HTTP communication.


Installation
============
Py2neo can be installed from the Python Package Index (PyPI) using ``pip`` or ``easy_install``::

    pip install py2neo


Source Code
===========

The source code is also available from GitHub but be sure to checkout the correct release branch.

::

    git clone git@github.com:nigelsmall/py2neo.git
    git checkout release/2.0.0


Py2neo v4
=========
[![License](https://img.shields.io/github/license/technige/py2neo.svg)](https://www.apache.org/licenses/LICENSE-2.0)

**Py2neo** is a client library and toolkit for working with [Neo4j](https://neo4j.com/) from within [Python](https://www.python.org/) applications and from the command line.
The library wraps the [official driver](https://github.com/neo4j/neo4j-python-driver), adding support for HTTP, a higher level API, an OGM, admin tools, an interactive console, a Cypher lexer for Pygments and many other bells and whistles. 
Unlike previous releases, Py2neo v4 no longer requires HTTP and can work entirely through Bolt.


Installation
------------
[![PyPI version](https://img.shields.io/pypi/v/py2neo.svg)](https://pypi.python.org/pypi/py2neo)
[![Build Status](https://img.shields.io/travis/technige/py2neo/v4.svg)](https://travis-ci.org/technige/py2neo)
[![Coverage Status](https://img.shields.io/coveralls/github/technige/py2neo/v4.svg)](https://coveralls.io/github/technige/py2neo?branch=v4)

To install the latest stable version of py2neo, simply use pip:

```
$ pip install py2neo
```

Or to install the latest bleeding edge code directly from GitHub, use:

```
$ pip install git+https://github.com/technige/py2neo.git#egg=py2neo
```

Note that code installed directly from GitHub is likely to be unstable.
Your mileage may vary.


Requirements
------------
[![Python versions](https://img.shields.io/pypi/pyversions/py2neo.svg)](https://www.python.org/)
[![Neo4j versions](https://img.shields.io/badge/neo4j-3.0%2C%203.1%2C%203.2%2C%203.3%2C%203.4-blue.svg)](https://neo4j.com/)

The following versions of Python and Neo4j are supported:

- Python 2.7 / 3.3 / 3.4 / 3.5 / 3.6 / 3.7-dev
- Neo4j 3.0 / 3.1 / 3.2 / 3.3 / 3.4 (the latest point release of each version is recommended)

Note also that Py2neo is developed and tested exclusively under **Linux** using standard CPython distributions.
While other operating systems and Python distributions may work, support for these is not available.


Contact
-------

For more information, read the [handbook](http://py2neo.org/v4).

To get in touch, contact me via [email](mailto:py2neo@nige.tech) or on [Twitter](https://twitter.com/technige).

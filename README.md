py2neo
======

The py2neo project provides bindings between Python and Neo4j via its RESTful
web service interface. It attempts to be both Pythonic and consistent with the
core Neo4j API.

In addition, the project provides support for the Graph Export Object File
Format (GEOFF) as well as a couple of command line tools for extra sugar.


Website:  http://py2neo.org/
PyPI:     http://pypi.python.org/pypi/py2neo
GitHub:   https://github.com/nigelsmall/py2neo
Email:    Nigel Small <py2neo@nigelsmall.org>


Installation
------------

The easiest way to install py2neo is from the Python Package Index (PyPI). This
generally requires superuser privileges, so on Debian or Ubuntu, simply execute
the following:

```
$ sudo easy_install py2neo
```

If you wish to make use of the command line tools, you may prefer to create
symbolic links to your system path. To achieve this, execute the following
(ensuring you replace the paths specified here with the appropriate equivalents
for your system and versions):

```
$ sudo ln -s /usr/local/lib/python2.7/dist-packages/py2neo-0.98-py2.7.egg/py2neo/cypher.py /usr/bin/cypher
$ sudo ln -s /usr/local/lib/python2.7/dist-packages/py2neo-0.98-py2.7.egg/py2neo/geoff.py /usr/bin/geoff
```


Getting Started
---------------

The following short programme illustrates a simple usage of the py2neo library:

```python
#!/usr/bin/env python

"""
Simple first example showing connection and traversal
"""

# Import Neo4j modules
from py2neo import neo4j

# Attach to the graph db instance
gdb = neo4j.GraphDatabaseService("http://localhost:7474/db/data")

# Obtain a link to the reference node of the db
ref_node = gdb.get_reference_node()

# Obtain a traverser instance relative to reference node
traverser = ref_node.traverse(order="depth_first", max_depth=2)

# Output all the paths from this traversal
for path in traverser.paths:
	print path
```


Command Line Usage
------------------

If symbolic links have been added, as detailed above, the `cypher` and `geoff`
commands will be available from direct usage from a terminal or within a shell
script. The `cypher` command allows Cypher queries to be executed against a
local or remote database, via its RESTful web interface, with the results
displayed on stdout:

```
usage: cypher [-h] [-u DATABASE_URI] [-d] [-j] [-g] query

Execute Cypher queries against a Neo4j database server and output the results.

positional arguments:
  query            the Cypher query to execute

optional arguments:
  -h, --help       show this help message and exit
  -u DATABASE_URI  the URI of the source Neo4j database server
  -d               output all values in delimited format (default)
  -j               output all values as a single JSON array
  -g               output nodes and relationships in GEOFF format
```

Similarly, the `geoff` command will allow a GEOFF data to be loaded into a
database from either a file or stdin:

```
usage: geoff [-h] [-u U] [-f F]

Import graph data from a GEOFF file into a Neo4j database.

optional arguments:
  -h, --help  show this help message and exit
  -u U        the URI of the destination Neo4j database server
  -f F        the GEOFF file to load
```

These commands may of course be piped together, allowing data to be fed from
one database into another. For example:

```
cypher -g "START n=node(23) match (n)-[r]-(x)--(y) return n, x.name, n, r, x, x.name, x.\`birth.date\`, y, y.name, r.\`marriage.date\`?" | geoff -u http://12.34.56.78:7474/db/data/
```

Note that if no database URI is specified, a default of
`http://localhost:7474/db/data/` will be used.

---

Copyright 2011 Nigel Small


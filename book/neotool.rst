Command Line Tool
=================

Py2neo provides a generic command line tool which taps into some of the
functionality available within the library. This tool can be used by direct
access to the Python module::

    python -m py2neo.tool --help

Alternatively, the simpler script wrapper can be used::

    neotool --help

The commands available via Neotool are outlined below:

Clearing the Database
---------------------
::

    neotool clear

Cypher Execution
----------------
::

    neotool cypher "start n=node(1) return n"

Delimited Output
~~~~~~~~~~~~~~~~
::

    neotool cypher-csv "start n=node(1) return n"
    neotool cypher-tsv "start n=node(1) return n"

JSON Output
~~~~~~~~~~~
::

    neotool cypher-json "start n=node(1) return n"

Geoff Output
~~~~~~~~~~~~
::

    neotool cypher-geoff "start n=node(1) return n"

Inserting/Merging Geoff Data
----------------------------
::

    neotool geoff-insert example.geoff
    neotool geoff-merge example.geoff

Inserting/Merging XML Data
--------------------------
::

    neotool geoff-insert example.xml
    neotool geoff-merge example.xml


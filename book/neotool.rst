Command Line Tool
=================

Py2neo provides a generic command line tool which taps into some of the
functionality available within the library. This tool can be used by direct
access to the Python module::

    python -m py2neo.tool --help

Alternatively, the simpler script wrapper can be used::

    neotool --help

The general form for commands is::

    neotool [<options>] <command> <args>
    
General options are available to change the database connection used. These
are::

    -S/--scheme <scheme>  Set the URI scheme, e.g. (http, https)
    -H/--host <host>      Set the URI host
    -P/--port <port>      Set the URI port

The set of commands available via Neotool are outlined below:

Clearing the Database
---------------------
::

    neotool clear

.. warning::

    This will remove all nodes and relationships from the graph database
    (including the reference node).

Cypher Execution
----------------
::

    neotool cypher "start n=node(1) return n"

Cypher queries passed will be executed and the results returned in an ASCII art
table, such as the one below::

    +---+
    +---+

Delimited Output
~~~~~~~~~~~~~~~~
::

    neotool cypher-csv "start n=node(1) return n.name, n.age?"
    neotool cypher-tsv "start n=node(1) return n.name, n.age?"

The results of Cypher queries can instead be returned as comma or tab separated
values. Each value is encoded as JSON, so strings are output between double
quotes whereas numbers and booleans are not. Arrays may also be output.

Although it is possible to output nodes and relationships, this format is most
useful when returning individual properties.

JSON Output
~~~~~~~~~~~
::

    neotool cypher-json "start n=node(1) return n"

The ``cypher-json`` command outputs query results within a JSON object. Nodes
and relationships within the results produce nested objects similar to, but
notably different from, the raw REST API results returned by the server. The
output below shows an example::

    {
        ...
    }

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


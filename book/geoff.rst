Geoff
=====

.. warning::

    The entire geoff.py module has been deprecated in py2neo 1.6.2 and will be
    removed completely in 1.7.0.

    Geoff should now be used via the `load2neo <http://nigelsmall.com/load2neo>`_
    extension that provides more efficient and more consistent behaviour. This
    facility can be accessed client-side by using the `load_geoff
    <../graphs_nodes_relationships/#py2neo.neo4j.GraphDatabaseService.load_geoff>`_
    method when the extension has been installed on the server.

.. note::

    The geoff.py module requires server version 1.8.2 or above.

Geoff is a textual interchange format for graph data, designed with Neo4j in
mind. It can be seen as a graphical equivalent of CSV and is intended to be
clear and familiar with a shallow learning curve.

The ``geoff`` module represents an evolution of the format originally packaged
with py2neo and the one provided by the server plugin. While this module is
fully backward compatible with the older, simpler syntax, use of the new syntax
is encouraged wherever possible. The server plugin will be enhanced at some
point in the future.

Overview
--------

Geoff can be used to represent a snapshot of graph data. It is purely
declarative and borrows much of its syntax from Cypher. A Geoff file is
represents a single *subgraph* and contains a number of *elements* separated by
whitespace. These elements may be *paths*, *index entries* or *comments*. The
example below shows all three of these element types within a single subgraph::

    /* Link to Alice and Bob from within the People index */
    |People {"email":"alice@example.com"}|=>(alice)
    |People {"email":"bob@example.org"}|=>(bob)

    /* Alice knows Bob */
    (alice {"name":"Alice"})-[:KNOWS]->(bob {"name":"Bob"})

Paths
-----

A Geoff path consists of one or more nodes connected by relationships. The
simplest path is a single node which may or may not contain properties::

    (alice)
    (alice {"name":"Alice"})
    
Any number of nodes may be chained together with relationships, for example::

    (alice)-[:KNOWS]->(bob {"name":"Bob"})
    (bob)-[:KNOWS]->(carol {"name":"Carol"})-[:KNOWS]->(dave {"name":"Dave"})

Path relationships do not all have to flow in the same direction::

    (homer)<-[:FATHER]-(bart)-[:MOTHER]->(marge)
    
Relationships may also have properties::

    (sun)-[:PLANET {"distance":149600000,"unit":"km"}]->(earth)

Index Entries
-------------

Index entries denote entries within Neo4j node indexes bound to a single
key-value pair. These elements use pipe symbols (``|``) and heavy arrows
(``=>``)::

    |People {"email":"alice@example.com"}|=>(alice {"name":"Alice"})
    (bob {"name":"Bob"})<=|People {"email":"bob@example.net"}|

.. note::

    Relationship index entries are no longer supported within Geoff.

Comments
--------

Comment elements may be included between other elements and have the same
syntax as C/Java multiline comments, between ``/*`` and ``*/``.

Subgraphs
---------

.. autoclass:: py2neo.geoff.Subgraph
    :members: load, load_xml, save, source, insert_into, merge_into

.. autoclass:: py2neo.geoff.ConstraintViolation

Module Functions
----------------

.. autofunction:: py2neo.geoff.insert

.. autofunction:: py2neo.geoff.merge

.. autofunction:: py2neo.geoff.insert_xml

.. autofunction:: py2neo.geoff.merge_xml

Full Geoff Syntax Specification (version 2)
-------------------------------------------

::

    subgraph       := [element (_ element)*]
    element        := path | index_entry | comment

    path           := node (forward_path | reverse_path)*
    forward_path   := "-" relationship "->" node
    reverse_path   := "<-" relationship "-" node

    index_entry    := forward_entry | reverse_entry
    forward_entry  := "|" ~ index_name _ property_pair ~ "|" "=>" node
    reverse_entry  := node "<=" "|" ~ index_name _ property_pair ~ "|"
    index_name     := name | JSON_STRING

    comment        := "/*" <<any text excluding sequence "*/">> "*/"

    node           := named_node | anonymous_node
    named_node     := "(" ~ node_name [_ property_map] ~ ")"
    anonymous_node := "(" ~ [property_map ~] ")"
    relationship   := "[" ~ ":" type [_ property_map] ~ "]"
    property_pair  := "{" ~ key_value ~ "}"
    property_map   := "{" ~ [key_value (~ "," ~ key_value)* ~] "}"
    node_name      := name | JSON_STRING
    name           := (ALPHA | DIGIT | "_")+
    type           := name | JSON_STRING
    key_value      := key ~ ":" ~ value
    key            := name | JSON_STRING
    value          := array | JSON_STRING | JSON_NUMBER | JSON_BOOLEAN | JSON_NULL

    array          := empty_array | string_array | numeric_array | boolean_array
    empty_array    := "[" ~ "]"
    string_array   := "[" ~ JSON_STRING (~ "," ~ JSON_STRING)* ~ "]"
    numeric_array  := "[" ~ JSON_NUMBER (~ "," ~ JSON_NUMBER)* ~ "]"
    boolean_array  := "[" ~ JSON_BOOLEAN (~ "," ~ JSON_BOOLEAN)* ~ "]"

    * Mandatory whitespace is represented by "_" and optional whitespace by "~"


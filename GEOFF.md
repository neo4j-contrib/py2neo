# GEOFF

GEOFF (Graph Export Object File Format) is a file format designed to hold a simple serialisation of graph data within
a text file. Although it has been drawn up with [Neo4j](http://neo4j.org/) in mind, it is intended to be flexible
enough to represent a variety of graphic data, regardless of origin.

The format borrows a [Cypher](http://docs.neo4j.org/chunked/stable/cypher-query-lang.html)-like notation for entity
labels and uses [JSON](http://json.org/) for associated data. Primarily, GEOFF aims to be human-readable, easily
editable and, unlike a number of XML-based formats with a similar purpose, non-verbose.

At the most basic level, a GEOFF file consists of a collection of *descriptors*, often coupled with associated
properties, each represented as a single line of text. A descriptor may represent a *node* (vertex), a *relationship*
(edge) or an inclusion within a database index. In addition to these descriptors, blank lines and comments (lines
beginning with a `#` symbol) can also be included.

## Nodes

The simplest entity which can be represented within GEOFF is a node. Each node consists of a identifier surrounded by
parentheses plus a set of properties applicable to that node. The identifier itself is used only as a local reference
to that node within the same file. As an example, the following line describes a simple node:

```
(bert) {"name": "Albert Einstein", "date_of_birth": "1879-03-14"}
```

The line consists of two parts - the node descriptor and the property data belonging to that node - separated by
linear white space. Here, the name `bert` defines a unique identifier for this node with the properties provided as
key:value pairs in JSON format. While the GEOFF format makes no restrictions on the types of the data provided as
properties, the underlying database may do so.

## Relationships

A relationship is a typed connection between two nodes and is represented by two node descriptors connected by an
ASCII art arrow and relationship metadata within square brackets, as per the following example:

```
(bert)                         {"name": "Albert Einstein", "date_of_birth": "1879-03-14"}
(genrel)                       {"name": "General Theory of Relativity"}
(bert)-[:PUBLISHED]->(genrel)  {"year_of_publication": 1916}
```

This describes two nodes `(bert)` and `(genrel)` and describes an unnamed relationship between them, also with
attached properties. Should an identifier be required for the relationship (as may be required for indexing) it can
be inserted before the colon, thus:

```
(bert)-[pub1:PUBLISHED]->(genrel)  {"year_of_publication": 1916}
```

## Hooks

So far, the relationships defined have connected nodes which are defined within the same GEOFF file. Sometimes however
it is necessary to refer to nodes which are defined externally, often within an existing database instance, and define
relationships against those. A *hook* is such an externally-defined entity and may refer to either a node or a
relationship.

Hooks are supplied as parameters passed into a GEOFF parser and are designated within the file by enclosing the
parameter name within braces. The following example illustrates a relationship between a node hook and a node defined
within the GEOFF file:

```
(bert)                      {"name": "Albert Einstein", "date_of_birth": "1879-03-14"}
{sci}-[:SCIENTIST]->(bert)  {"year_of_publication": 1916}
```

Here, `{sci}` could refer to a "SCIENTISTS" [subreference node](http://wiki.neo4j.org/content/Design_Guide#Subreferences),
for example. A node hook may be supplied as either or both the start node or end node of a relationship.

Node and relationship hooks may also be referenced individually. The data values supplied list *all* properties
registered against that entity and could be used to update that entity's property list.

```
{sci}    {"last_updated": "13:45:09"}
```

## Index Inclusions

It is also possible to specify inclusions in database indexes within a GEOFF file; this can apply to nodes,
relationships or hooks and all use a similar syntax, which includes the index name between pipe `|` symbols. The data
values supplied to these descriptors provide the values under which the entities are indexed and may be restricted by
underlying database software. The following example shows one index inclusion for each allowed syntax:

```
# This indexes the node "bert" within the "Scientists" index under "name=Einstein"
(bert)<=|Scientists|    {"name": "Einstein"}
# This indexes the relationship "pub1" within the "Publications" index under "year=1916"
[pub1]<=|Publications|  {"year": 1916}
# This indexes the hook "foo" within the "Things" index under "foo=bar"
{foo}<=|Things|         {"foo": "bar"}
```

## Composite Descriptors

A composite descriptor allows a number of individual descriptors to be specified within a single line of text.
Rendered as a JSON object, each key:value pair holds a descriptor and its data respectively. The following example
combines three descriptor:data pairs, the second of third of which have empty sets of properties.

```
{"(bert)": {"name": "Albert Einstein"}, "(genrel)": {}, "(bert)-[:PUBLISHED]->(genrel)": null}
```

The ordering of the items should be unimportant to a GEOFF parser as the items can be thought of as loading "in
parallel". The *py2neo* loader implementation sorts all items within a composite descriptor set so that hooks and
nodes are loaded first, followed by relationships and finally by index entries. To that end, the following composite
descriptor can be seen as exactly equivalent to the one above:

```
{"(genrel)": {}, "(bert)-[:PUBLISHED]->(genrel)": null, "(bert)": {"name": "Albert Einstein"}}
```

## Specification

The following definitions describe the full GEOFF syntax using an augmented BNF [as defined within RFC822](http://www.w3.org/Protocols/rfc822/#z25).

```
EOL                      = CR / LF / ( CR LF )
LWSP                     = SP / HT

geoff-file               = geoff-line *( EOL geoff-line )
geoff-line               = blank-line
                         / comment
                         / descriptor [ 1*LWSP data ]
                         / composite-descriptor
data                     = <JSON object>

blank-line               = *LWSP
comment                  = "#" CHAR

descriptor               = hook-descriptor
                         / node-descriptor
                         / relationship-descriptor
                         / index-inclusion

composite-descriptor     = "{" *LWSP descriptor-data-pair *LWSP *( "," descriptor-data-pair ) "}"
descriptor-data-pair     = '"' descriptor '"' *LWSP ":" *LWSP data

hook-ref                 = "{" entity-name "}"
node-ref                 = "(" entity-name ")"
relationship-ref         = "[" entity-name "]"
index-ref                = "|" entity-name "|"

hook-descriptor          = hook-ref
node-descriptor          = node-ref
connectable              = hook-ref / node-ref
relationship-descriptor  = connectable "-[" [ entity-name ] ":" entity-type "]->" connectable
indexable                = hook-ref / node-ref / relationship-ref
index-inclusion          = indexable "<=" index-ref

entity-name              = 1*( ALPHA / DIGIT / "_" )
entity-type              = 1*( ALPHA / DIGIT / "_" )
```

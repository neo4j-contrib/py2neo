=============
Release Notes
=============

Version 1.6.1
=============
- Cypher transactions
- Implicit authentication when user info passed in URI
- Improved reconnection handling on MacOS
- Renamed schema.get_index to 'get_indexed_property_keys'
- Miscellaneous bug fixes (see issue list for further details)

Version 1.6
===========

General
-------
- HTTP/REST transport layer rewritten to use HTTPStream library
- Minimum supported server version increased to 1.8
- Changed some server calls to be "lazy", particularly on creation of GraphDatabaseService
- Added feature detection methods (GraphDatabaseService.supports_*)
- Changed Node.exists and Relationship.exists from function to property
- Changed Node.is_abstract and Relationship.is_abstract from function to property
- Changed Node.id and Relationship.id to '_id'
- Changed GDB.order and size from function to property
- Changed Path.order and size from function to property
- Removed all previously deprecated features
- DEFAULT_URI now points to service root, not graph database (i.e. without trailing /db/data/)

Cypher
------
- Deprecated cypher module in favour of CypherQuery class
- Revised execution methods: run, execute, execute_one, stream
- Results returns as CypherResults or IterableCypherResults

Batch
-----
- Batch methods return BatchRequest object for use in other requests
- Added create_path and get_or_create_path to WriteBatch
- Revised all WriteBatch methods for better consistency
- Improved comments and documentation on batch classes
- Batches no longer auto-clear on submission
- Removed ReadBatch.get_properties, use GraphDatabaseService.get_properties instead
- Cypher queries now supported in batches (append_cypher method)
- Revised execution methods: run, stream, submit

Match
-----
- Changed all match methods to return iterators
- Removed bidirectional argument from Node.match (now always bidirectional)
- Added Node.match_incoming
- Added Node.match_outgoing
- Removed Node.match_one

Labels & Schema Indexes
-----------------------
- Label support in Node and WriteBatch classes
- GraphDatabaseService.find for iterating through labelled nodes
- Added schema resource to GraphDatabaseService

Indexes
-------
- Changed GraphDatabaseService.delete_index to throw LookupError if not found
- Index.query now returns iterator instead of list

Neotool
-------
- Command line scripts for cypher/geoff removed in favour of unified neotool
- Changed neotool text output to match minimal format used by PostgreSQL
- Neotool now fully supports unicode

Gremlin
-------
- Removed Gremlin module

Admin
-----
- Monitor class moved from admin module into neo4j module, admin module removed

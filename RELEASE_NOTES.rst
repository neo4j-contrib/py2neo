=============
Release Notes
=============

Version 1.6
===========
- Label support in Node and WriteBatch classes
- GraphDatabaseService.find for iterating through labelled nodes
- Rewritten HTTP layer to use HTTPStream
- Removed all previously deprecated features
- Added Node.match_incoming
- Changed GraphDatabaseService.delete_index to throw LookupError if not found
- Deprecated cypher module in favour of GraphDatabaseService.cypher resource
- Monitor moved from admin module into neo4j module

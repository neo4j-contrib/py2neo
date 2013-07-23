=============
Release Notes
=============

Version 1.6
===========
- Increased minimum supported server version to 1.8
- Label support in Node and WriteBatch classes
- GraphDatabaseService.find for iterating through labelled nodes
- Rewritten HTTP layer to use HTTPStream
- Removed all previously deprecated features
- Changed GraphDatabaseService.delete_index to throw LookupError if not found
- Deprecated cypher module in favour of GraphDatabaseService.cypher resource
- Monitor moved from admin module into neo4j module
- Reconfigured match methods
  - Changed all match methods to return iterators
  - Removed bidirectional argument from Node.match (now default)
  - Added Node.match_incoming
  - Added Node.match_outgoing
  - Removed Node.match_one
- Removed Gremlin module
- Batch methods return integer positions
- Added create_path and get_or_create_path to WriteBatch

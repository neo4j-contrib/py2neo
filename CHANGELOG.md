# Change Log
Notable changes to this project are documented herein.

## [Unreleased]

### Added
- Support for Neo4j 3.1.
- Support for Python 3.6.

### Changed
- Official driver 1.1.0 dependency replaces embedded 1.0 series driver. This is the first (and so far only) project dependency.
- Simplified internal module structure (see TODO:CONTRIBUTING.md for current module structure).
- Renamed DBMS to GraphService and rebuilt URI handling and service addressing in `py2neo.addressing` module.
- Collapsed transactional Cypher all through official driver interface (including HTTP scheme handler)
- Pull and push can now be combined with other operations inside a transaction.

### Removed
- Previously deprecated attributes:
  - Graph.find()
  - Graph.find_one()
  - Graph.neo4j_version
  - Node.degree()
  - Node.exists()
  - Node.match()
  - Node.match_incoming()
  - Node.match_outgoing()
  - Node.properties
  - Node.pull()
  - Node.push()
  - Relationship.exists()
  - Relationship.properties
  - Relationship.pull()
  - Relationship.push()
  - Transaction.append()
- "Batman" extension (HTTP batch interface and manual indexing support)

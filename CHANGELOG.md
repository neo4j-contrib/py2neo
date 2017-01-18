# Change Log
Notable changes to this project are documented herein.

## [Unreleased]

### Added
- Support for Neo4j 3.1.
- Support for Python 3.6.
- New Cypher console
- `Transaction.pull()`
- `Transaction.push()`

### Changed
- Introduced project requirements:
  - `neo4j-driver`
  - `urllib3`
- Simplified internal module structure (see TODO:CONTRIBUTING.md for current module structure).
- Renamed `DBMS` to `GraphService`.
- Replaced URI handling code by introducing new `py2neo.addressing` module.
- Transactional Cypher over HTTP now goes via a plugin for the official driver
- `Subgraph.__db_pull__` now takes a `Transaction` instead of a `Graph`
- `Subgraph.__db_push__` now takes a `Transaction` instead of a `Graph`

### Removed
- Previously deprecated attributes:
  - `Graph.find()`
  - `Graph.find_one()`
  - `Graph.neo4j_version`
  - `Node.degree()`
  - `Node.exists()`
  - `Node.match()`
  - `Node.match_incoming()`
  - `Node.match_outgoing()`
  - `Node.properties`
  - `Node.pull()`
  - `Node.push()`
  - `Relationship.exists()`
  - `Relationship.properties`
  - `Relationship.pull()`
  - `Relationship.push()`
  - `Transaction.append()`
- "Batman" extension (HTTP batch interface and manual indexing support)

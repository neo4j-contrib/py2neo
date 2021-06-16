:orphan:

*************
Release notes
*************


Version 2021.2
==============

Highlights
----------
- Dropped support for Python 3.4


Version 2021.1
==============

Highlights
----------
- Full support for routing
- Full support for Neo4j 4.3
- Big stability improvements for multithreaded usage.
- Retries built into :meth:`.Graph.update` and :meth:`.Graph.query` methods
- New PEP249 (DB API 2.0) compatibility

Core API
--------
- The :class:`.ConnectionProfile` class has been moved to the root ``py2neo`` package and a new :class:`.ServiceProfile` subclass has also been introduced, providing access to ``neo4j://...``, ``neo4j+s://...`` and ``neo4j+ssc://...`` URIs.
- The ``py2neo.database.work`` package has been removed. This relocates :class:`Transaction` and :class:`TransactionSummary` into ``py2neo.database``, and :class:`Cursor` and :class:`Record` into ``py2neo.cypher``.
- The :class:`.Procedure` and :class:`.ProcedureLibrary` classes have been moved to ``py2neo.cypher.proc``.
- The ``py2neo.database`` package is now fully documented under the root ``py2neo`` package.
- The ``py2neo.data.operations`` module has been collapsed directly into the :class:`.Transaction` class. So instead of, for example, `create_subgraph(tx, subgraph)` you should now use `tx.create(subgraph)` directly.
- A new :meth:`.Graph.update` method has been added. This provides execution with retries for Cypher statements that carry out updates (writes), but which do not return results. This method also accepts transaction functions as well as individual queries.
- A new :meth:`.Graph.query` method has been added. This provides execution with retries for Cypher statements that carry out readonly queries, returning results.
- The :meth:`.Transaction.commit` and :meth:`.Transaction.rollback` methods have been deprecated. The :meth:`.Graph.commit` and :meth:`.Graph.rollback` methods should be used instead.
- The :class:`.Transaction` class can no longer be used in a ``with`` block. Use explicit begin/commit calls or a :meth:`.Graph.update` call instead.
- Big stability improvements for multithreaded usage.
- The :meth:`.Cursor.stats`, :meth:`.Cursor.plan` and :meth:`.Cursor.summary` methods now return simple dictionaries instead of custom classes.
- The :attr:`.Cursor.profile` attribute has been introduced to provide access to the connection profile under which the originating query was executed.

Error handling
--------------
- The py2neo error hierarchy has been given a major overhaul, with most surface-level errors now moved to the ``py2neo.errors`` module.
- :class:`.ClientError`, :class:`.ClientError` and :class:`.ClientError` are now subclasses of :class:`.Neo4jError`.
- :class:`.ServiceUnavailable` and :class:`.WriteServiceUnavailable` are now raised whenever the entire database service becomes unavailable, or when the service becomes limited to read-only, respectively.

New modules and packages
------------------------
- A new ``py2neo.export`` package has been introduced to house all bulk export functionality as well as exports to third party formats. The :class:`.Table` class has also been moved to this package.
- A new ``py2neo.pep249`` module has been added, which introduces an interface compatible with the Python DB API 2.0.

Command line tooling
--------------------
- Added multi-database support to command line console.

Neo4j support
-------------
- Basic support for Bolt 4.3 has been added, although not all optimisations are yet in place.
- Updated Cypher lexer to add support for Cypher 4.2.
- Routing support has now matured from experimental to full, allowing Aura and other Neo4j cluster deployments to be used from py2neo.

Requirements updates
--------------------
- The project requirements have been adjusted to allow `Prompt Toolkit 3.x` to be used when using Python 3.5 or above. Previously, this was limited to `Prompt Toolkit 2.x` for all Python versions, which caused knock-on dependency issues.

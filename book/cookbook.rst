========
Cookbook
========


Better Performance
==================


Monitoring Client-Server Interaction
====================================

.. autofunction:: py2neo.watch


Escaping Values in Cypher
=========================

```python
>>> from py2neo.cypher import cypher_escape
>>> rel_type = "KNOWS WELL"
>>> statement = "CREATE (a)-[ab:%s]->(b) RETURN ab" % cypher_escape(rel_type)
>>> statement
'CREATE (a)-[ab:`KNOWS WELL`]->(b) RETURN ab'
```


Authentication
==============

.. autofunction:: py2neo.authenticate


URI Rewriting
=============

.. autofunction:: py2neo.rewrite

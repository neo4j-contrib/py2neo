Deprecated Features
===================

All features deprecated in py2neo 1.5 and earlier versions have now been
removed. The list below shows all features deprecated in version 1.6.

Cypher
------

.. autofunction:: py2neo.cypher.execute

Geoff
-----

The entire geoff.py module has been deprecated in py2neo 1.6.2 and will be
removed completely in 1.7.0.

Geoff should now be used via the `load2neo <http://nigelsmall.com/load2neo>`_
extension that provides more efficient and more consistent behaviour. This
facility can be accessed client-side by using the `load_geoff
<graphs_nodes_relationships/#py2neo.neo4j.GraphDatabaseService.load_geoff>`_
method when the extension has been installed on the server.

WriteBatch
----------

.. automethod:: py2neo.neo4j.WriteBatch.get_or_create

.. automethod:: py2neo.neo4j.WriteBatch.get_or_add_indexed_node

.. automethod:: py2neo.neo4j.WriteBatch.get_or_add_indexed_relationship

.. automethod:: py2neo.neo4j.WriteBatch.remove_indexed_node

.. automethod:: py2neo.neo4j.WriteBatch.remove_indexed_relationship

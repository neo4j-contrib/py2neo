Deprecated Features
===================

In order to simplify the py2neo API, a significant number of functions have
been deprecated in version 1.5. These functions are listed below and, for each,
an alternative is presented.

GraphDatabaseService
--------------------

.. automethod:: py2neo.neo4j.GraphDatabaseService.get_reference_node

.. automethod:: py2neo.neo4j.GraphDatabaseService.get_or_create_relationships

Node
----

.. automethod:: py2neo.neo4j.Node.create_relationship_from

.. automethod:: py2neo.neo4j.Node.create_relationship_to

.. automethod:: py2neo.neo4j.Node.get_related_nodes

.. automethod:: py2neo.neo4j.Node.get_relationships

.. automethod:: py2neo.neo4j.Node.get_relationships_with

.. automethod:: py2neo.neo4j.Node.get_single_related_node

.. automethod:: py2neo.neo4j.Node.get_single_relationship

.. automethod:: py2neo.neo4j.Node.has_relationship

.. automethod:: py2neo.neo4j.Node.has_relationship_with

.. automethod:: py2neo.neo4j.Node.is_related_to

Relationship
------------

.. automethod:: py2neo.neo4j.Relationship.is_type

.. autoattribute:: py2neo.neo4j.Relationship.nodes

WriteBatch
----------

.. automethod:: py2neo.neo4j.WriteBatch.create_node

.. automethod:: py2neo.neo4j.WriteBatch.create_relationship

.. automethod:: py2neo.neo4j.WriteBatch.get_or_create_relationship

.. automethod:: py2neo.neo4j.WriteBatch.delete_node

.. automethod:: py2neo.neo4j.WriteBatch.delete_relationship

.. automethod:: py2neo.neo4j.WriteBatch.set_node_property

.. automethod:: py2neo.neo4j.WriteBatch.set_node_properties

.. automethod:: py2neo.neo4j.WriteBatch.delete_node_property

.. automethod:: py2neo.neo4j.WriteBatch.delete_node_properties

.. automethod:: py2neo.neo4j.WriteBatch.set_relationship_property

.. automethod:: py2neo.neo4j.WriteBatch.set_relationship_properties

.. automethod:: py2neo.neo4j.WriteBatch.delete_relationship_property

.. automethod:: py2neo.neo4j.WriteBatch.delete_relationship_properties

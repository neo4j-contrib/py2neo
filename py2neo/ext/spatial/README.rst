==================
Extension: Spatial
==================

Maintained by: Simon Harrison <noisyboiler@googlemail.com>

This is an API to the contrib Neo4j Spatial Extension for creating, destroying and querying Well
Known Text (WKT) geometries over GIS map Layers.

.. note::
   Shapely and libgeos both need to be installed separately to use this extension.


Main API
========

Each Layer you create will build a sub-graph modelling geographically aware nodes as an R-tree -
which is your magical spatial index!

A geographically-aware Node is one with a 'wkt' property. When you add such a Node to your
application you require a Layer for the Node and a unique name for this geometry. Internally, the
Node will be created in your application's graph, an additional node is added to the Rtree index
graph (the map Layer) and a relationship is created binding them together.

.. automodule:: py2neo.ext.spatial.plugin
    :members:


Geoserver Integration
=====================

.. _Geoserver: http://geoserver.org/
.. _Spatial: https://github.com/neo4j-contrib/spatial

The Neo4j Spatial utilities can be integrated with Geoserver_ to create a neo4j *type*
**datasource**.

When you configure your datasource to point at a neo4j data store that contains Open Street map
(OSM) layers, you can visualise your maps using Geoserver!

Unfortunately the Spatial_ server extension does not expose enough of the core Java api for
py2neoSpatial to implement an `OSMLayer` api, only that for an `EditableLayer`, and py2neoSpatial
implements this with WKT geometry. Even more unfortunately, at the time of writing, the Geoserver
integration does not recognise WKT type layers, only those of OSM type.

But watch this space.

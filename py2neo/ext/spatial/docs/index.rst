.. SpatialPlugin documentation master file, created by
   sphinx-quickstart on Tue Aug 26 15:53:18 2014.

Welcome to the documentation for py2neo's Spatial Plugin - py2neoSpatial!
=========================================================================

This is an API to the contrib Neo4j Spatial Extension for creating, destroying and querying Well Known Text (WKT) geometries over GIS map Layers.

Each Layer you create will build a sub-graph modelling geographically aware nodes as an R-tree - which is your magical spatial index!

A geographically-aware Node is one with a 'wkt' property. When you add such a Node to your application you require a Layer for the Node and a unique name for this geometry. Internally, the Node will be created in your application's graph, an additional node is added to the Rtree index graph (the map Layer) and a relationship is created binding them together. 

* :ref:`search`

.. toctree::
   :maxdepth: 2

   geoserver

API Documentation
=================

.. automodule:: plugin
.. autoclass:: Spatial
    :members:

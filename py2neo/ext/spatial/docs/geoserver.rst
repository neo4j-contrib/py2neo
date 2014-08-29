.. _Geoserver: http://geoserver.org/
.. _Spatial: https://github.com/neo4j-contrib/spatial


#####################
Geoserver Integration
#####################

The Neo4j Spatial utilities can be integrated with Geoserver_ to create a neo4j *type* **datasource**.

When you configure your datasource to point at a neo4j data store that contains Open Street map (OSM) layers, you can visualise your maps using Geoserver!

Unfortunately the Spatial_ server extension does not expose enough of the core Java api for py2neoSpatial to implement an `OSMLayer` api, only that for an `EditableLayer`, and py2neoSpatial implements this with WKT geometry. Even more unfortunately, at the time of writing, the Geoserver integration does not recognise WKT type layers, only those of OSM type.

But watch this space.

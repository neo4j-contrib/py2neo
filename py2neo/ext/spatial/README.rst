.. _Neo4j Spatial Extension: https://github.com/neo4j-contrib/spatial
.. _Shapely: https://pypi.python.org/pypi/Shapely
.. _libgeos: https://github.com/libgeos/libgeos
.. _Well Known Text: http://en.wikipedia.org/wiki/Well-known_text
.. _GIS: http://en.wikipedia.org/wiki/Geographic_information_system
.. _Geoserver: http://geoserver.org/
.. _Spatial: https://github.com/neo4j-contrib/spatial
.. _Rtree: http://en.wikipedia.org/wiki/R—tree
.. _geotools: http://www.geotools.org/
.. _WGS84: http://spatialreference.org/ref/epsg/wgs-84/

.. role:: bash(code)
   :language: bash

==================
Extension: Spatial
==================

Maintained by: Simon Harrison <noisyboiler@googlemail.com>

An API to the `Neo4j Spatial Extension`_ for creating, destroying and querying `Well Known Text`_ (WKT) geometries over GIS_ map Layers.

Requires Neo4j >= 2.0, the `Neo4j Spatial Extension`_, libgeos_ and the Python package Shapely_.

Neo4j Setup
-----------

Clone the Neo4j Spatial project and checkout the branch that corresponds to the Neo4j version you are using, e.g. remotes/origin/0.14-neo4j-2.2 for Neo4j Community 2.2.3 makes sense, then build the Neo4j spatial extension.

	:bash:`mvn clean package install -Dmaven.test.skip=true`

Stop your Neo4j server and install the plugin.

	:bash:`cp $NEO4J_SPATIAL_HOME/target/neo4j-spatial-XXXX-server-plugin.zip $NEO4J_HOME/plugins`

	:bash:`unzip $NEO4J_HOME/plugins/neo4j-spatial-XXXX-server-plugin.zip -d $NEO4J_HOME/plugins`

To test your install, from a shell you can use `curl` to ask for the configuration of the Neo4j server.

	:bash:`curl http://username:password@localhost:7474/db/data/`

In the "extensions" section of response you should see "SpatialPlugin".

Run Tests
=========

The py2neo Spatial APIs rely on the upstream project, the `Neo4j Spatial Extension`_, to be kept up to date with Neo4j. Because of this, it's advised to run the tests for this extension to check for compatibility problems.

	:bash: pip install -r ./test/requirements.txt
	:bash: py.test ./test/ext/spatial/

These tests have been run successfully against Neo4j 2.0 - 2.2.3.

API Overview
============

Simple CRUD APIs for GIS Layers, Points Of Interest (POI) and other geometries with endpoints for some spatial type queries over your data.

Each GIS Layer you create is essentially an "index" and is modelled as an Rtree_ within your graph which Neo4j will use when executing "spatial" type queries. This should not be confused with the Neo4j schema indexes, nor the "legacy" type of index, and is used internally by the spatial extension to optimise expensive spatial queries.

py2neo Spatial creates layers of type ``org.neo4j.gis.spatial.EditableLayerImpl`` and encodes spatial data with the ``org.neo4j.gis.spatial.WKTGeometryEncoder``. See the `Neo4j Spatial Extension`_ for alternatives.

Each vector geometry you create must be in the standard, unprojected `WGS84`_ (EPSG:4326) geographic coordinate system. For a vector geometry this will be marked-up in Well Known Text (WKT) and for a POI this will be as a longitude/latitude (x/y) pair. There is nothing stopping you adding your POI as WKT - the long/lat API exists for the convenience of the many clients that deal in point coordinates.

.. note::
	py2neo Spatial works with WGS84 (EPSG 4326) WKT strings. When py2neo Spatial handles Point Of Interest coordinates it use the  standard of POINT (x y) which translates to POINT (Lon Lat) - *not* lat/long.

See wiki `Well Known Text`_ for a list of possible geometries that you can create. Internally the data will be stored as Well Known Binary (WKB).

.. automodule:: py2neo.ext.spatial.plugin
    :members:


Neo4j Spatial & Geoserver
=========================

Geoserver is an open source server for sharing the geospatial data that you create using py2neo spatial and other GIS technologies.

In order to support a Neo4j type datastore we must first add the Neo4j and Neo4j Spatial source jars into your Geoservers ``WEB—INF/lib`` directory and restart it.

.. note::
	Documentation based on Neo4j 2.2.3, GeoServer 2.7 and Neo4j Spatial 0.14 on Ubunutu 14.04 VM

Clone the Neo4j Spatial project and checkout the branch that corresponds to the Neo4j version you are using, e.g. remotes/origin/0.14-neo4j-2.2. Build the Neo4j spatial extension.

	:bash:`mvn clean package install -Dmaven.test.skip=true`

On success, unpack the ``neo4j—spatial—X.XX—neo4j—X.X.X—server—plugin.zip`` archive that you've created in the projects `target` directory and copy or move the contained jars into the ``WEB—INF/lib`` directory found under $GEOSERVER_HOME. Then copy the jars from ``$NEO4J_HOME/lib`` into the same directory. Restart Geoserver.

If you've been successful you will now find a new Neo4j "Store" available from the Geoserver web UI:  **"Neo4j — A datasource backed by a Neo4j Spatial datasource"**.

Visit the web interface at http://localhost:8080/geoserver/web to find this out.

.. note::
	The default username and password is "admin" and "geoserver"

Getting spatial data into Neo4j is easy with py2neo's spatial extension but you may not have any yet and you may just want to try out Geoserver, so py2neo Spatial provides some example data and a script to load them.

For example, to put a MultiPolygon modelling Cornwall on to a GIS layer called "uk", from the root of your project, run:

	:bash:`python py2neo/ext/spatial/scripts/load_data.py --data cornwall --layer uk --username neo4j --password mysecret`

Whatever GIS data you have, stop your Neo4j server and place a copy of the database somewhere that Geoserver can access it. From the GeoServer web admin create a new Neo4j Store, name it, describe it, and add the path to your Neo4j database. Save it.

GeoServer should recognise the layers in the datastore, so whatever you named a Layer with py2neo, expect to see it listed now. Next step: pubish and preview the layers.

.. warning::
	Geoserver integration is flakey, and there are no integration tests between all the moving parts. Configuring Geoserver to not raise `java.lang.OutOfMemoryError` and patching Neo4j Spatial not to raise `org.neo4j.graphdb.NotInTransactionException` is only straight forward for JAVA developers. Getting your versions of Neo4j, Neo4j Spatial and Geoserver all to play well together is also a challenge. As exciting as the prospect of visualising Neo4j data sets in Geoserver is, it's currently incredibly hard work and requires someone to take this on seriously.

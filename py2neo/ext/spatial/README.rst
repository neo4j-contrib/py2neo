.. _Neo4j Spatial Extension: https://github.com/neo4j-contrib/spatial
.. _Shapely: https://pypi.python.org/pypi/Shapely
.. _libgeos: https://github.com/libgeos/libgeos
.. _Well Known Text: http://en.wikipedia.org/wiki/Well-known_text
.. _GIS: http://en.wikipedia.org/wiki/Geographic_information_system
.. _Geoserver: http://geoserver.org/
.. _Spatial: https://github.com/neo4j-contrib/spatial
.. _Rtree: http://en.wikipedia.org/wiki/R-tree
.. _Spatial: https://github.com/neo4j-contrib/spatial
.. _geotools: http://www.geotools.org/
.. role:: bash(code)
   :language: bash

==================
Extension: Spatial
==================

Maintained by: Simon Harrison <noisyboiler@googlemail.com>

This is an API to the contrib `Neo4j Spatial Extension`_ for creating, destroying and querying `Well
Known Text`_ (WKT) geometries over GIS_ map Layers.

.. note::

	| The `Neo4j Spatial Extension`_ must first be installed - check this from a shell: ``curl -v http://localhost:7474/db/data/``.
	| You'll need libgeos_.
	| And the Python package Shapely_.


Main API
========

Each Layer you create will build a sub-graph modelling geographically aware nodes as an Rtree_ -
which is your magical spatial index!

A geographically-aware Node is one with a 'wkt' property. When you add such a Node to your
application you require a Layer for the Node and a unique name for this geometry. Internally, the
Node will be created in your application's graph, an additional node is added to the Rtree_ index
graph (the map Layer) and a relationship is created binding them together.

.. automodule:: py2neo.ext.spatial.plugin
    :members:


#####################
Geoserver Integration
#####################

The Neo4j Server Spatial library can be integrated with Geoserver_ to allow the creation of a neo4j *type* **datasource** which you can use to visualise what you've created with Py2neo-Spatial.

.. warning::

	Neo4j server integration to Geoserver is work in progress. Because of this, it's recommened to work from this branch: ``https://github.com/noisyboiler/spatial``

	And it's almost certain that you can help!


Prerequisites
=============

Oracle JDK 7 or OpenJDK 7

Geoserver >= 2.5.2

Neo4j >= 2.1.5

Source of Neo4j Spatial Server Extension
- this documentation is based on the branch ``0.14-neo4j-2.2`` (https://github.com/neo4j-contrib/spatial/tree/0.14-neo4j-2.2)

py2neo latest


Install Geoserver
=================

If you do not have a preferred method for your OS, find out how to do it for you from Geoserver_.


Stop Geoserver
##############

You may have already started your server in a frenzy of excitement: :bash:`geoserver stop`


Build The Spatial Extension
===========================

Pre-compliled archives do exist, but until an integration test suite between these and Geoserver, it's almost certain you'll need to build from the "supporting" branch.

:bash:`mvn clean package -Dmaven.test.skip=true install`


####################################
Locate Your Geoserver Webapps Folder
####################################

Locate your Geoserver webapps lib folder, ``$GEOSERVER_HOME/webapps/geoserver/WEB-INF/lib`` - you'll be copying files from Neo4j *and* it's server extension into here.


##############################
Locate Your Neo4j Spatial Jars
##############################

Locate the Neo4j Spatial Extension archive: ``neo4j-spatial-X.XX-neo4j-X.X.X-server-plugin.zip``. Unzip this.


######################
Locate Your Neo4j Jars
######################

Locate your Neo4j source lib folder ``$NEO4J_HOME/lib``.


Integrate With Geoserver
========================

From your Neo4j Spatial Jars, copy everyhing but the geotools_ files (these are ones prefixed by 'gt') into ``$GEOSERVER_HOME/webapps/geoserver/WEB-INF/lib`` - Geoserver tends to prefer it's own versions of these. Do this now.

From your Neo4j Jar folder copy *everything* into ``$GEOSERVER_HOME/webapps/geoserver/WEB-INF/lib``.

Do notice the versioning of all files you copy over. If any of these libraries already exists in your Geoserver webapps lib folder and they are older... use your discretion. In my experience, Geoserver tends to prefer it's own versions of geotools_. At the very end of this exercise, if you fail to see any data, come back to these steps with suspicion.


Run Geoserver
=============

The default configuration for Geoserver **will not** allocate enough memeory for the tasks ahead, so do **not** start Geoserver  with :bash:`geoserver start` - you need to allocate extra room.

The following has been sufficient for small datasets, where ``start.jar`` is that from your Geoserver home directory.

:bash:`java -server -Xmx256M -Xms48m -XX:MaxPermSize=128m -DGEOSERVER_DATA_DIR=/path/to/data/ -jar /path/to/geoserver/start.jar`

For example, if you installed using homebrew you can expect to find yout ``start.jar`` somewhere like:

:bash:`java -server -Xmx256M -Xms48m -XX:MaxPermSize=128m -jar /usr/local/Cellar/geoserver/2.5.2/libexec/start.jar`

Check that all is okay: http://localhost:8080/geoserver/web/

Then check you can login.

.. note::

	username/password is likely to be admin/geoserver

Configure a workspace from the left sidebar "Workspaces" option. My imagination came up with "neo".

.. note::

	Geoserver will run it's own Neo4j server and it will want to do this on the default port, 7474. Your preferred server will have to be stopped before visualising your data with Geoserver. It's most likely that you will have to *copy* the data directory from your preferred server (whist it is *not* running) and place this somewhere convienient in you file system - i chose the same folder Geoserver requires to know about for other data.

.. warning::

	the version of Neo that your data was created in **must** match that Geoserver is using. It's very easy to forget and if you grab data from a different version it's likely Geoserver will not spot the spatial data, leaving you with no raised warnings or errors and no fun.


###########################
Try This Out With Some Data
###########################

Getting spatial data into Neo is easy with py2neo's spatial extension. But you may not have any, and you may just want to know whether your setup is working. So py2neo provides example data and a script to load the one of your choice.

For example, to put a MultiPolygon modelling Cornwall on to a GIS layer called "uk", from the root of your project, run:

:bash:`python py2neo/ext/spatial/scripts/load_data.py cornwall --layer uk`

You now have a GIS layer and a tiny piece of data to visualise!

Stop Neo4j (Geoserver will need the port) and go back to the Geoserver web admin: http://localhost:8080/geoserver/web/

Select "Stores", "Add new store", "Neo4j - A datasource backed by a Neo4j Spatial datasource".

Give the source a name, say, "cornwall". The directory path of the Neo4j database should be absolute, e.g. ``/usr/local/Cellar/neo4j/2.1.4/libexec/data/``.

.. note::

	this does not include the name of the datastore ``graph.db``, just the path. including the name of the datastore will lead to long stack traces.



###############
Troubleshooting
###############

Mac OS

BUILD FAILURE: Fatal error compiling: invalid target release: 1.7

Even though ``java -version`` clearly reports 1.7, the maven build finds the OS default, which can be lower.

Solution: override in your terminal session, e.g.

:bash:`export JAVA_HOME=`/usr/libexec/java_home -v 1.7``



ERROR org.mortbay.log - Nested in org.springframework.web.util.NestedServletException: Handler processing failed; nested exception is java.lang.NoClassDefFoundError: org/neo4j/graphdb/factory/GraphDatabaseFactory:
java.lang.NoClassDefFoundError: org/neo4j/graphdb/factory/GraphDatabaseFactory

Solution: Geoserver is not recognising Neo4j. All of Neo4j's source needs to be copied into the webapps lib directory as Geoserver will run it's own neo server. Try this step again.


87057 [btpool0-0] WARN  org.geoserver.web.data.store  - Error obtaining new data store
java.io.IOException....
Caused by: org.neo4j.graphdb.NotInTransactionException

	Write **and** reads from Neo are required to be in transactions.

# Grolt

Grolt is an interactive Docker-based tool for running Neo4j servers and clusters in development.

**Please ensure that you have an appropriate license for running Neo4j**


## Usage

To install Grolt, simply run:
```
pip install grolt
```

The primary interface is through the CLI, so to see the options available, use:
```
grolt --help
```

To start a single standalong instance with the latest Neo4j release, use:

```
grolt
```

To start a 3-core cluster with, use the command:

```
grolt -c 3
```

To start a 3-core cluster with additional configuration options you use:

```
grolt -c 3 -C dbms.default_database=mygraph
```

## Interacting with containers via docker

To view the running docker containers:
```
docker ps
```

To tail the main neo4j logs of an instance:
```
docker logs -f <container id>
```

To tail the debug logs of an instance:
```
docker exec <container id> tail -f /logs/debug.log
```

To pause and unpause an instance:
```
docker pause <container id>
docker unpause <container id>
```

## Running your latest code using grolt

First, you have to build neo4j tarballs:
```
cd <your neo4j repo>
mvn package -DskipTests -Dcheckstyle.skip -Dlicense.skip -Dlicensing.skip -Denforcer.skip -T2C
```

Then you can run grolt with them
```
cd <your neo4j repo>
grolt -c 3 --neo4j-source-dir "$(pwd)" --user "$(whoami)"
```

#/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright 2011-2014, Nigel Small
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


""" Deprecated features for core module.
"""


from py2neo.core import *
from py2neo.error import BindError, GraphError
from py2neo.packages.httpstream.numbers import BAD_REQUEST
from py2neo.util import deprecated, flatten, ustr


__all__ = []


class _Graph(Graph):

    @deprecated("Use `pull` instead")
    def get_properties(self, *entities):
        """ Fetch properties for multiple nodes and/or relationships as part
        of a single batch; returns a list of dictionaries in the same order
        as the supplied entities.
        """
        self.pull(*entities)
        return [entity.properties for entity in entities]


Graph.get_properties = _Graph.get_properties


class _PropertyContainer(PropertyContainer):

    @deprecated("Use `properties` attribute instead")
    def get_cached_properties(self):
        """ Fetch last known properties without calling the server.

        :return: dictionary of properties
        """
        return self.properties

    @deprecated("Use `pull` method on `properties` attribute instead")
    def get_properties(self):
        """ Fetch all properties.

        :return: dictionary of properties
        """
        if self.bound:
            self.properties.pull()
        return self.properties

    @deprecated("Use `push` method on `properties` attribute instead")
    def set_properties(self, properties):
        """ Replace all properties with those supplied.

        :param properties: dictionary of new properties
        """
        self.properties.replace(properties)
        if self.bound:
            self.properties.push()

    @deprecated("Use `push` method on `properties` attribute instead")
    def delete_properties(self):
        """ Delete all properties.
        """
        self.properties.clear()
        try:
            self.properties.push()
        except BindError:
            pass


PropertyContainer.get_cached_properties = _PropertyContainer.get_cached_properties
PropertyContainer.get_properties = _PropertyContainer.get_properties
PropertyContainer.set_properties = _PropertyContainer.set_properties
PropertyContainer.delete_properties = _PropertyContainer.delete_properties


class _Node(Node):

    @deprecated("Use `add` or `update` method of `labels` property instead")
    def add_labels(self, *labels):
        """ Add one or more labels to this node.

        :param labels: one or more text labels
        """
        labels = [ustr(label) for label in set(flatten(labels))]
        self.labels.update(labels)
        try:
            self.labels.push()
        except GraphError as err:
            if err.response.status_code == BAD_REQUEST and err.cause.exception == 'ConstraintViolationException':
                raise ValueError(err.cause.message)
            else:
                raise

    @deprecated("Use graph.create(Path(node, ...)) instead")
    def create_path(self, *items):
        """ Create a new path, starting at this node and chaining together the
        alternating relationships and nodes provided::

            (self)-[rel_0]->(node_0)-[rel_1]->(node_1) ...
                   |-----|  |------| |-----|  |------|
             item:    0        1        2        3

        Each relationship may be specified as one of the following:

        - an existing Relationship instance
        - a string holding the relationship type, e.g. "KNOWS"
        - a (`str`, `dict`) tuple holding both the relationship type and
          its properties, e.g. ("KNOWS", {"since": 1999})

        Nodes can be any of the following:

        - an existing Node instance
        - an integer containing the ID of an existing node
        - a `dict` holding a set of properties for a new node
        - :py:const:`None`, representing an unspecified node that will be
          created as required

        :param items: alternating relationships and nodes
        :return: `Path` object representing the newly-created path
        """
        path = Path(self, *items)
        return path.create(self.graph)

    @deprecated("Use Graph.delete instead")
    def delete(self):
        """ Delete this entity from the database.
        """
        self.graph.delete(self)

    @deprecated("Use Cypher query instead")
    def delete_related(self):
        """ Delete this node along with all related nodes and relationships.
        """
        if self.graph.supports_foreach_pipe:
            query = ("START a=node({a}) "
                     "MATCH (a)-[rels*0..]-(z) "
                     "FOREACH(r IN rels| DELETE r) "
                     "DELETE a, z")
        else:
            query = ("START a=node({a}) "
                     "MATCH (a)-[rels*0..]-(z) "
                     "FOREACH(r IN rels: DELETE r) "
                     "DELETE a, z")
        self.graph.cypher.post(query, {"a": self._id})

    @deprecated("Use `labels` property instead")
    def get_labels(self):
        """ Fetch all labels associated with this node.

        :return: :py:class:`set` of text labels
        """
        self.labels.pull()
        return self.labels

    @deprecated("Use graph.merge(Path(node, ...)) instead")
    def get_or_create_path(self, *items):
        """ Identical to `create_path` except will reuse parts of the path
        which already exist.

        Some examples::

            # add dates to calendar, starting at calendar_root
            christmas_day = calendar_root.get_or_create_path(
                "YEAR",  {"number": 2000},
                "MONTH", {"number": 12},
                "DAY",   {"number": 25},
            )
            # `christmas_day` will now contain a `Path` object
            # containing the nodes and relationships used:
            # (CAL)-[:YEAR]->(2000)-[:MONTH]->(12)-[:DAY]->(25)

            # adding a second, overlapping path will reuse
            # nodes and relationships wherever possible
            christmas_eve = calendar_root.get_or_create_path(
                "YEAR",  {"number": 2000},
                "MONTH", {"number": 12},
                "DAY",   {"number": 24},
            )
            # `christmas_eve` will contain the same year and month nodes
            # as `christmas_day` but a different (new) day node:
            # (CAL)-[:YEAR]->(2000)-[:MONTH]->(12)-[:DAY]->(25)
            #                                  |
            #                                [:DAY]
            #                                  |
            #                                  v
            #                                 (24)

        """
        path = Path(self, *items)
        return path.get_or_create(self.graph)

    @deprecated("Use Cypher query instead")
    def isolate(self):
        """ Delete all relationships connected to this node, both incoming and
        outgoing.
        """
        query = "START a=node({a}) MATCH a-[r]-b DELETE r"
        self.graph.cypher.post(query, {"a": self._id})

    @deprecated("Use `remove` method of `labels` property instead")
    def remove_labels(self, *labels):
        """ Remove one or more labels from this node.

        :param labels: one or more text labels
        """
        from py2neo.batch import WriteBatch
        labels = [ustr(label) for label in set(flatten(labels))]
        batch = WriteBatch(self.graph)
        for label in labels:
            batch.remove_label(self, label)
        batch.run()

    @deprecated("Use `clear` and `update` methods of `labels` property instead")
    def set_labels(self, *labels):
        """ Replace all labels on this node.

        :param labels: one or more text labels
        """
        labels = [ustr(label) for label in set(flatten(labels))]
        self.labels.clear()
        self.labels.add(*labels)


Node.add_labels = _Node.add_labels
Node.create_path = _Node.create_path
Node.delete = _Node.delete
Node.delete_related = _Node.delete_related
Node.get_labels = _Node.get_labels
Node.get_or_create_path = _Node.get_or_create_path
Node.isolate = _Node.isolate
Node.remove_labels = _Node.remove_labels
Node.set_labels = _Node.set_labels


class _Rel(Rel):

    @deprecated("Use graph.delete instead")
    def delete(self):
        """ Delete this Rel from the database.
        """
        self.resource.delete()


Rel.delete = _Rel.delete


class _Path(Path):

    def _create_query(self, unique):
        nodes, path, values, params = [], [], [], {}

        def append_node(i, node):
            if node is None:
                path.append("(n{0})".format(i))
                values.append("n{0}".format(i))
            elif node.bound:
                path.append("(n{0})".format(i))
                nodes.append("n{0}=node({{i{0}}})".format(i))
                params["i{0}".format(i)] = node._id
                values.append("n{0}".format(i))
            else:
                path.append("(n{0} {{p{0}}})".format(i))
                params["p{0}".format(i)] = node.properties
                values.append("n{0}".format(i))

        def append_rel(i, rel):
            if rel.properties:
                path.append("-[r{0}:`{1}` {{q{0}}}]->".format(i, rel.type))
                params["q{0}".format(i)] = rel.properties
                values.append("r{0}".format(i))
            else:
                path.append("-[r{0}:`{1}`]->".format(i, rel.type))
                values.append("r{0}".format(i))

        append_node(0, self.__nodes[0])
        for i, rel in enumerate(self.__rels):
            append_rel(i, rel)
            append_node(i + 1, self.__nodes[i + 1])
        clauses = []
        if nodes:
            clauses.append("START {0}".format(",".join(nodes)))
        if unique:
            clauses.append("CREATE UNIQUE p={0}".format("".join(path)))
        else:
            clauses.append("CREATE p={0}".format("".join(path)))
        #clauses.append("RETURN {0}".format(",".join(values)))
        clauses.append("RETURN p")
        query = " ".join(clauses)
        return query, params

    def _create(self, graph, unique):
        query, params = _Path._create_query(self, unique=unique)
        try:
            results = graph.cypher.execute(query, params)
        except GraphError:
            raise NotImplementedError(
                "The Neo4j server at <{0}> does not support "
                "Cypher CREATE UNIQUE clauses or the query contains "
                "an unsupported property type".format(graph.uri)
            )
        else:
            for row in results:
                return row[0]

    @deprecated("Use Graph.create(Path(...)) instead")
    def create(self, graph):
        """ Construct a path within the specified `graph` from the nodes
        and relationships within this :py:class:`Path` instance. This makes
        use of Cypher's ``CREATE`` clause.
        """
        return _Path._create(self, graph, unique=False)

    @deprecated("Use Graph.merge(Path(...)) instead")
    def get_or_create(self, graph):
        """ Construct a unique path within the specified `graph` from the
        nodes and relationships within this :py:class:`Path` instance. This
        makes use of Cypher's ``CREATE UNIQUE`` clause.
        """
        return _Path._create(self, graph, unique=True)


Path.create = _Path.create
Path.get_or_create = _Path.get_or_create


class _Relationship(Relationship):

    @deprecated("Use Graph.delete instead")
    def delete(self):
        """ Delete this relationship from the database.
        """
        self.graph.delete(self)

    @deprecated("Use `push` method on `properties` attribute instead")
    def delete_properties(self):
        """ Delete all properties.
        """
        self.properties.clear()
        try:
            self.properties.push()
        except BindError:
            pass

    @deprecated("Use `properties` attribute instead")
    def get_cached_properties(self):
        """ Fetch last known properties without calling the server.

        :return: dictionary of properties
        """
        return self.properties

    @deprecated("Use `pull` method on `properties` attribute instead")
    def get_properties(self):
        """ Fetch all properties.

        :return: dictionary of properties
        """
        if self.bound:
            self.properties.pull()
        return self.properties

    @deprecated("Use `push` method on `properties` attribute instead")
    def set_properties(self, properties):
        """ Replace all properties with those supplied.

        :param properties: dictionary of new properties
        """
        self.properties.replace(properties)
        if self.bound:
            self.properties.push()

    @deprecated("Use properties.update and push instead")
    def update_properties(self, properties):
        """ Update the properties for this relationship with the values
        supplied.
        """
        if self.bound:
            query, params = ["START a=rel({A})"], {"A": self._id}
            for i, (key, value) in enumerate(properties.items()):
                value_tag = "V" + str(i)
                query.append("SET a.`" + key + "`={" + value_tag + "}")
                params[value_tag] = value
            query.append("RETURN a")
            rel = self.graph.cypher.execute_one(" ".join(query), params)
            self._properties = rel.__metadata__["data"]
        else:
            self._properties.update(properties)


Relationship.delete = _Relationship.delete
Relationship.delete_properties = _Relationship.delete_properties
Relationship.get_cached_properties = _Relationship.get_cached_properties
Relationship.get_properties = _Relationship.get_properties
Relationship.set_properties = _Relationship.set_properties
Relationship.update_properties = _Relationship.update_properties

#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright 2011-2018, Nigel Small
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


from collections import Sequence, Set

from py2neo.cypher.writing import cypher_escape
from py2neo.data import Node, Relationship
from py2neo.internal.collections import is_collection


def _property_equality_conditions(properties, offset=1):
    for i, (key, value) in enumerate(properties.items(), start=offset):
        if key == "__id__":
            condition = "id(_)"
        else:
            condition = "_.%s" % cypher_escape(key)
        if isinstance(value, (tuple, set, frozenset)):
            condition += " IN {%d}" % i
            parameters = {"%d" % i: list(value)}
        else:
            condition += " = {%d}" % i
            parameters = {"%d" % i: value}
        yield condition, parameters


class NodeMatch(object):
    """ An immutable set of node match criteria.
    """

    def __init__(self, graph, labels=frozenset(), conditions=tuple(), order_by=tuple(), skip=None, limit=None):
        self.graph = graph
        self._labels = frozenset(labels)
        self._conditions = tuple(conditions)
        self._order_by = tuple(order_by)
        self._skip = skip
        self._limit = limit

    def __iter__(self):
        for record in self.graph.run(*self._query_and_parameters):
            yield record[0]

    def first(self):
        """ Evaluate the match and return the first :class:`.Node`
        matched or :const:`None` if no matching nodes are found.

        :return: a single matching :class:`.Node` or :const:`None`
        """
        return self.graph.evaluate(*self._query_and_parameters)

    @property
    def _query_and_parameters(self):
        """ A tuple of the Cypher query and parameters used to select
        the nodes that match the criteria for this selection.

        :return: Cypher query string
        """
        clauses = ["MATCH (_%s)" % "".join(":%s" % cypher_escape(label) for label in self._labels)]
        parameters = {}
        if self._conditions:
            conditions = []
            for condition in self._conditions:
                if isinstance(condition, tuple):
                    condition, param = condition
                    parameters.update(param)
                conditions.append(condition)
            clauses.append("WHERE %s" % " AND ".join(conditions))
        clauses.append("RETURN _")
        if self._order_by:
            clauses.append("ORDER BY %s" % (", ".join(self._order_by)))
        if self._skip:
            clauses.append("SKIP %d" % self._skip)
        if self._limit is not None:
            clauses.append("LIMIT %d" % self._limit)
        return " ".join(clauses), parameters

    def where(self, *conditions, **properties):
        """ Refine this match to create a new match. The criteria specified
        for refining the match consist of conditions and properties.
        Conditions are individual Cypher expressions that would be found
        in a `WHERE` clause; properties are used as exact matches for
        property values.

        To refer to the current node within a condition expression, use
        the underscore character ``_``. For example::

            match.where("_.name =~ 'J.*")

        Simple property equalities can also be specified::

            match.where(born=1976)

        :param conditions: Cypher expressions to add to the `WHERE` clause
        :param properties: exact property match keys and values
        :return: refined :class:`.NodeMatch` object
        """
        return self.__class__(self.graph, self._labels,
                              self._conditions + conditions + tuple(_property_equality_conditions(properties)),
                              self._order_by, self._skip, self._limit)

    def order_by(self, *fields):
        """ Order by the fields or field expressions specified.

        To refer to the current node within a field or field expression,
        use the underscore character ``_``. For example::

            match.order_by("_.name", "max(_.a, _.b)")

        :param fields: fields or field expressions to order by
        :return: refined :class:`.NodeMatch` object
        """
        return self.__class__(self.graph, self._labels, self._conditions,
                              fields, self._skip, self._limit)

    def skip(self, amount):
        """ Skip the first `amount` nodes in the result.

        :param amount: number of nodes to skip
        :return: refined :class:`.NodeMatch` object
        """
        return self.__class__(self.graph, self._labels, self._conditions,
                              self._order_by, amount, self._limit)

    def limit(self, amount):
        """ Limit to at most `amount` nodes.

        :param amount: maximum number of nodes to return
        :return: refined :class:`.NodeMatch` object
        """
        return self.__class__(self.graph, self._labels, self._conditions,
                              self._order_by, self._skip, amount)


class NodeMatcher(object):
    """ A :class:`.NodeMatcher` can be used to locate nodes that
    fulfil a specific set of criteria. Typically, a single node can be
    identified passing a specific label and property key-value pair.
    However, any number of labels and any condition supported by the
    Cypher `WHERE` clause is allowed.

    For a simple match by label and property::

        >>> from py2neo import Graph, NodeMatcher
        >>> graph = Graph()
        >>> matcher = NodeMatcher(graph)
        >>> matched = matcher.match("Person", name="Keanu Reeves")
        >>> list(matched)
        [(f9726ea:Person {born:1964,name:"Keanu Reeves"})]

    For a more comprehensive match using Cypher expressions, the
    :meth:`.NodeMatch.where` method can be used for further
    refinement. Here, the underscore character can be used to refer to
    the node being filtered::

        >>> matched = matcher.match("Person").where("_.name =~ 'J.*'", "1960 <= _.born < 1970")
        >>> list(matched)
        [(a03f6eb:Person {born:1967,name:"James Marshall"}),
         (e59993d:Person {born:1966,name:"John Cusack"}),
         (c44901e:Person {born:1960,name:"John Goodman"}),
         (b141775:Person {born:1965,name:"John C. Reilly"}),
         (e40244b:Person {born:1967,name:"Julia Roberts"})]

    The underlying query is only evaluated when the selection undergoes
    iteration or when a specific evaluation method is called (such as
    :meth:`.NodeMatch.first`). This means that a :class:`.NodeMatch`
    instance may be reused before and after a data changes for different
    results.
    """

    _match_class = NodeMatch

    def __init__(self, graph):
        self.graph = graph

    def __getitem__(self, identity):
        entity = self.get(identity)
        if entity is None:
            raise KeyError("Node %d not found" % identity)
        return entity

    def get(self, identity):
        """ Create a new :class:`.NodeMatch` that filters by identity and
        returns the first matched :class:`.Node`. This can essentially be
        used to match and return a :class:`.Node` by ID.

            matcher.get(1234)

        If no such :class:`.Node` is found, py:const:`None` is returned
        instead. Contrast with `matcher[1234]` which raises a `KeyError`
        if no entity is found.
        """
        try:
            return self.graph.node_cache[identity]
        except KeyError:
            return self.match().where("id(_) = %d" % identity).first()

    def match(self, *labels, **properties):
        """ Describe a basic node match using labels and property equality.

        :param labels: node labels to match
        :param properties: set of property keys and values to match
        :return: :class:`.NodeMatch` instance
        """
        criteria = {}
        if labels:
            criteria["labels"] = frozenset(labels)
        if properties:
            criteria["conditions"] = tuple(_property_equality_conditions(properties))
        return self._match_class(self.graph, **criteria)


class RelationshipMatch(object):

    def __init__(self, graph, nodes=None, r_type=None,
                 conditions=tuple(), order_by=tuple(), skip=None, limit=None):
        if nodes is not None and not isinstance(nodes, (Sequence, Set)):
            raise ValueError("Nodes must be supplied as a Sequence or a Set")
        self.graph = graph
        self._nodes = nodes
        self._r_type = r_type
        self._conditions = tuple(conditions)
        self._order_by = tuple(order_by)
        self._skip = skip
        self._limit = limit

    def __iter__(self):
        for record in self.graph.run(*self._query_and_parameters):
            yield record[0]

    def first(self):
        """ Evaluate the selection and return the first
        :class:`.Relationship` selected or :const:`None` if no matching
        relationships are found.

        :return: a single matching :class:`.Relationship` or :const:`None`
        """
        return self.graph.evaluate(*self._query_and_parameters)

    @property
    def _query_and_parameters(self):
        """ A tuple of the Cypher query and parameters used to select
        the nodes that match the criteria for this selection.

        :return: Cypher query string
        """

        def verify_node(n):
            if n.graph != self.graph:
                raise ValueError("Node %r does not belong to this graph" % n)
            if n.identity is None:
                raise ValueError("Node %r is not bound to a graph" % n)

        def r_type_name(r):
            try:
                return r.__name__
            except AttributeError:
                return r

        clauses = []
        parameters = {}
        if self._r_type is None:
            relationship_detail = ""
        elif is_collection(self._r_type):
            relationship_detail = ":" + "|:".join(cypher_escape(r_type_name(t)) for t in self._r_type)
        else:
            relationship_detail = ":%s" % cypher_escape(r_type_name(self._r_type))
        if isinstance(self._nodes, Sequence):
            if len(self._nodes) >= 1 and self._nodes[0] is not None:
                start_node = Node.cast(self._nodes[0])
                verify_node(start_node)
                clauses.append("MATCH (a) WHERE id(a) = {x}")
                parameters["x"] = start_node.identity
            if len(self._nodes) >= 2 and self._nodes[1] is not None:
                end_node = Node.cast(self._nodes[1])
                verify_node(end_node)
                clauses.append("MATCH (b) WHERE id(b) = {y}")
                parameters["y"] = end_node.identity
            if len(self._nodes) >= 3:
                raise ValueError("Node sequence cannot be longer than two")
            clauses.append("MATCH (a)-[_" + relationship_detail + "]->(b)")
        elif isinstance(self._nodes, Set):
            self._nodes = {node for node in self._nodes if node is not None}
            if len(self._nodes) >= 1:
                start_node = Node.cast(self._nodes.pop())
                verify_node(start_node)
                clauses.append("MATCH (a) WHERE id(a) = {x}")
                parameters["x"] = start_node.identity
            if len(self._nodes) >= 1:
                end_node = Node.cast(self._nodes.pop())
                verify_node(end_node)
                clauses.append("MATCH (b) WHERE id(b) = {y}")
                parameters["y"] = end_node.identity
            if len(self._nodes) >= 1:
                raise ValueError("Node set cannot be larger than two")
            clauses.append("MATCH (a)-[_" + relationship_detail + "]-(b)")
        else:
            clauses.append("MATCH (a)-[_" + relationship_detail + "]->(b)")
        if self._conditions:
            conditions = []
            for condition in self._conditions:
                if isinstance(condition, tuple):
                    condition, param = condition
                    parameters.update(param)
                conditions.append(condition)
            clauses.append("WHERE %s" % " AND ".join(conditions))
        clauses.append("RETURN _")
        if self._order_by:
            clauses.append("ORDER BY %s" % (", ".join(self._order_by)))
        if self._skip:
            clauses.append("SKIP %d" % self._skip)
        if self._limit is not None:
            clauses.append("LIMIT %d" % self._limit)
        return " ".join(clauses), parameters

    def where(self, *conditions, **properties):
        """ Refine this match to create a new match. The criteria specified
        for refining the match consist of conditions and properties.
        Conditions are individual Cypher expressions that would be found
        in a `WHERE` clause; properties are used as exact matches for
        property values.

        To refer to the current node within a condition expression, use
        the underscore character ``_``. For example::

            match.where("_.name =~ 'J.*")

        Simple property equalities can also be specified::

            match.where(born=1976)

        :param conditions: Cypher expressions to add to the `WHERE` clause
        :param properties: exact property match keys and values
        :return: refined :class:`.NodeMatch` object
        """
        return self.__class__(self.graph,
                              nodes=self._nodes,
                              r_type=self._r_type,
                              conditions=self._conditions + conditions + tuple(_property_equality_conditions(properties)),
                              order_by=self._order_by,
                              skip=self._skip,
                              limit=self._limit)

    def order_by(self, *fields):
        """ Order by the fields or field expressions specified.

        To refer to the current node within a field or field expression,
        use the underscore character ``_``. For example::

            match.order_by("_.name", "max(_.a, _.b)")

        :param fields: fields or field expressions to order by
        :return: refined :class:`.NodeMatch` object
        """
        return self.__class__(self.graph,
                              nodes=self._nodes,
                              r_type=self._r_type,
                              conditions=self._conditions,
                              order_by=fields,
                              skip=self._skip,
                              limit=self._limit)

    def skip(self, amount):
        """ Skip the first `amount` nodes in the result.

        :param amount: number of nodes to skip
        :return: refined :class:`.NodeMatch` object
        """
        return self.__class__(self.graph,
                              nodes=self._nodes,
                              r_type=self._r_type,
                              conditions=self._conditions,
                              order_by=self._order_by,
                              skip=amount,
                              limit=self._limit)

    def limit(self, amount):
        """ Limit to at most `amount` nodes.

        :param amount: maximum number of nodes to return
        :return: refined :class:`.NodeMatch` object
        """
        return self.__class__(self.graph,
                              nodes=self._nodes,
                              r_type=self._r_type,
                              conditions=self._conditions,
                              order_by=self._order_by,
                              skip=self._skip,
                              limit=amount)


class RelationshipMatcher(object):
    """ A :py:class:`.RelationshipMatcher` can be used to locate
    relationships that fulfil a specific set of criteria.
    """

    _match_class = RelationshipMatch

    def __init__(self, graph):
        self.graph = graph
        self._all = self._match_class(self.graph)

    def __getitem__(self, identity):
        entity = self.get(identity)
        if entity is None:
            raise KeyError("Relationship %d not found" % identity)
        return entity

    def get(self, identity):
        """ Create a new :class:`.RelationshipMatch` that filters by identity and
        returns the first matched :class:`.Relationship`. This can essentially be
        used to match and return a :class:`.Relationship` by ID.

            matcher.get(1234)

        If no such :class:`.Relationship` is found, py:const:`None` is returned
        instead. Contrast with `matcher[1234]` which raises a `KeyError`
        if no entity is found.
        """
        try:
            return self.graph.relationship_cache[identity]
        except KeyError:
            return self.match().where("id(_) = %d" % identity).first()

    def match(self, nodes=None, r_type=None, **properties):
        """ Describe a basic relationship match...

        :param nodes: Sequence or Set of start and end nodes (:const:`None` means any node);
                a Set implies a match in any direction
        :param r_type:
        :param properties: set of property keys and values to match
        :return: :class:`.RelationshipMatch` instance
        """
        criteria = {}
        if nodes is not None:
            criteria["nodes"] = nodes
        if r_type is not None:
            criteria["r_type"] = r_type
        if properties:
            criteria["conditions"] = tuple(_property_equality_conditions(properties))
        return self._match_class(self.graph, **criteria)

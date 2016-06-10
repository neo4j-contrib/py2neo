#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright 2011-2016, Nigel Small
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


from py2neo.database.cypher import cypher_escape, cypher_repr


def property_equality_conditions(**properties):
    for key, value in properties.items():
        if isinstance(value, (tuple, set, frozenset)):
            yield "_.%s IN %s" % (cypher_escape(key), cypher_repr(list(value)))
        else:
            yield "_.%s = %s" % (cypher_escape(key), cypher_repr(value))


class NodeSelection(object):
    """ A set of criteria representing a selection of nodes from a
    graph.
    """

    def __init__(self, graph, labels=frozenset(), conditions=tuple(), order_by=tuple(), skip=None, limit=None):
        self.graph = graph
        self._labels = frozenset(labels)
        self._conditions = tuple(conditions)
        self._order_by_fields = tuple(order_by)
        self._skip_amount = skip
        self._limit_amount = limit

    def __iter__(self):
        for node, in self.graph.run(self.query):
            yield node

    def one(self):
        """ Evaluate the selection and return the first
        :py:class:`.Node` selected or :py:const:`None` if no matching
        nodes are found.

        :return: a single matching :py:class:`.Node` or :py:const:`None`
        """
        return self.graph.evaluate(self.query)

    @property
    def query(self):
        """ The Cypher query used to select the nodes that match the
        criteria for this selection.

        :return: Cypher query string
        """
        clauses = ["MATCH (_%s)" % "".join(":%s" % cypher_escape(label) for label in self._labels)]
        if self._conditions:
            clauses.append("WHERE %s" % " AND ".join(self._conditions))
        clauses.append("RETURN _")
        if self._order_by_fields:
            clauses.append("ORDER BY %s" % (", ".join(self._order_by_fields)))
        if self._skip_amount:
            clauses.append("SKIP %d" % self._skip_amount)
        if self._limit_amount is not None:
            clauses.append("LIMIT %d" % self._limit_amount)
        return " ".join(clauses)

    def where(self, *conditions, **properties):
        """ Create a new selection based on this selection. The
        criteria specified for refining the selection consist of
        conditions and properties. Conditions are individual Cypher
        expressions that would be found in a `WHERE` clause; properties
        are used as exact matches for property values.

        To refer to the current node within a condition expression, use
        the underscore character ``_``. For example::

            selection.where("_.name =~ 'J.*")

        :param conditions: Cypher expressions to add to the selection
                           `WHERE` clause
        :param properties: exact property match keys and values
        :return: refined selection object
        """
        return self.__class__(self.graph, self._labels,
                              self._conditions + conditions + tuple(property_equality_conditions(**properties)),
                              self._order_by_fields, self._skip_amount, self._limit_amount)

    def order_by(self, *fields):
        """ Order by the fields or field expressions specified.

        To refer to the current node within a field or field expression,
        use the underscore character ``_``. For example::

            selection.order_by("_.name", "max(_.a, _.b)")

        :param fields: fields or field expressions to order by
        :return: refined selection object
        """
        return self.__class__(self.graph, self._labels, self._conditions,
                              fields, self._skip_amount, self._limit_amount)

    def skip(self, amount):
        """ Skip the first `amount` nodes in the result.

        :param amount: number of nodes to skip
        :return: refined selection object
        """
        return self.__class__(self.graph, self._labels, self._conditions,
                              self._order_by_fields, amount, self._limit_amount)

    def limit(self, amount):
        """ Limit the selection to at most `amount` nodes.

        :param amount: maximum number of nodes to select
        :return: refined selection object
        """
        return self.__class__(self.graph, self._labels, self._conditions,
                              self._order_by_fields, self._skip_amount, amount)


class NodeSelector(object):
    """ A :py:class:`.NodeSelector` can be used to locate nodes within
    a graph that fulfil a specified set of conditions. Typically, a
    single node can be identified passing a specific label and property
    key-value pair. However, any number of labels and any condition
    supported by the Cypher `WHERE` clause is allowed.

    For a simple selection by label and property::

        >>> from py2neo import Graph
        >>> graph = Graph()
        >>> selector = NodeSelector(graph)
        >>> selected = selector.select("Person", name="Keanu Reeves")
        >>> list(selected)
        [(f9726ea:Person {born:1964,name:"Keanu Reeves"})]

    For a more comprehensive selection using Cypher expressions::

        >>> selected = selector.select("Person").where("_.name =~ 'J.*'", "1960 <= _.born < 1970")
        >>> list(selected)
        [(a03f6eb:Person {born:1967,name:"James Marshall"}),
         (e59993d:Person {born:1966,name:"John Cusack"}),
         (c44901e:Person {born:1960,name:"John Goodman"}),
         (b141775:Person {born:1965,name:"John C. Reilly"}),
         (e40244b:Person {born:1967,name:"Julia Roberts"})]

    Note that the underlying query is only evaluated when the selection
    undergoes iteration. This means that a :py:class:`NodeSelection`
    instance may be reused to query the graph data multiple times.
    """

    selection_class = NodeSelection

    def __init__(self, graph):
        self.graph = graph
        self._all = self.selection_class(self.graph)

    def select(self, *labels, **properties):
        """ Describe a basic node selection using labels and property equality.

        :param labels: node labels to match
        :param properties: set of property keys and values to match
        :return: :py:class:`.NodeSelection` instance
        """
        if labels or properties:
            return self.selection_class(self.graph, frozenset(labels),
                                        tuple(property_equality_conditions(**properties)))
        else:
            return self._all

#!/usr/bin/env python
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


from io import StringIO

from py2neo.core import Node, LabelSet, PropertySet, Relationship, NodePointer
from py2neo.cypher.lang import CypherParameter, CypherWriter
from py2neo.util import ustr, xstr


__all__ = ["Cypher", "Create", "CypherTask", "CreateNode", "CreateRelationship", "MergeNode"]


class Cypher(object):

    prefix = None
    separator = "\n"

    def __init__(self, *parts, **parameters):
        self.__parts = list(map(ustr, parts))
        self.__parameters = dict(parameters)

    def __repr__(self):
        return "<Cypher statement=%r parameters=%r>" % (self.statement, self.parameters)

    def __str__(self):
        return xstr(self.statement)

    def __unicode__(self):
        return ustr(self.statement)

    @property
    def parts(self):
        return self.__parts

    @property
    def statement(self):
        if self.prefix:
            return "%s %s" % (self.prefix, self.separator.join(self.__parts))
        else:
            return self.separator.join(self.__parts)

    @property
    def parameters(self):
        return self.__parameters


class Create(Cypher):

    prefix = "CREATE"
    separator = ", "

    def __init__(self, *parts, **parameters):
        super(Create, self).__init__(*parts, **parameters)


class CypherTask(object):
    """ The `CypherTask` class can either be used directly or as
    a base class for more specific statement implementations.
    """

    def __init__(self, statement="", parameters=None, **kwparameters):
        self.__statement = statement
        self.__parameters = dict(parameters or {}, **kwparameters)

    def __repr__(self):
        return "<CypherTask statement=%r parameters=%r>" % (self.statement, self.parameters)

    def __str__(self):
        return xstr(self.statement)

    def __unicode__(self):
        return ustr(self.statement)

    @property
    def statement(self):
        """ The Cypher statement.
        """
        return self.__statement

    @property
    def parameters(self):
        """ Dictionary of parameters.
        """
        return self.__parameters


class CreateNode(CypherTask):
    """ :class:`.CypherTask` for creating nodes.
    """

    identifier = "a"
    properties_parameter_key = "P"

    def __init__(self, *labels, **properties):
        CypherTask.__init__(self)
        self.__node = Node(*labels, **properties)
        self.__return = False

    @property
    def labels(self):
        """ The full set of labels to apply to the created node.

        :rtype: :class:`py2neo.LabelSet`
        """
        return self.__node.labels

    @property
    def properties(self):
        """ The full set of properties to apply to the created node.

        :rtype: :class:`py2neo.PropertySet`
        """
        return self.__node.properties

    def set(self, *labels, **properties):
        """ Extra labels and properties to apply to the node.
        """
        self.__node.labels.update(labels)
        self.__node.properties.update(properties)
        return self

    def with_return(self):
        """ Include a RETURN clause in the statement.
        """
        self.__return = True
        return self

    @property
    def statement(self):
        """ The full Cypher statement.
        """
        string = StringIO()
        writer = CypherWriter(string)
        writer.write_literal("CREATE ")
        writer.write_node(self.__node, self.identifier if self.__return else None,
                          CypherParameter(self.properties_parameter_key) if self.__node.properties
                          else None)
        if self.__return:
            writer.write_literal(" RETURN %s" % self.identifier)
        return string.getvalue()

    @property
    def parameters(self):
        """ Dictionary of parameters.
        """
        if self.__node.properties:
            return {self.properties_parameter_key: self.properties}
        else:
            return {}


class CreateRelationship(CypherTask):
    """ :class:`.CypherTask` for creating relationships.
    """

    identifier = "a"
    properties_parameter_key = "P"

    def __init__(self, *triple, **properties):
        CypherTask.__init__(self)
        rel = Relationship(*triple, **properties)
        if isinstance(rel.start_node, NodePointer) or isinstance(rel.end_node, NodePointer):
            raise TypeError("NodePointers cannot be used with CypherTasks")
        self.__relationship = rel
        self.__return = False

    @property
    def start_node(self):
        """ The start node of the created relationship.

        :rtype: :class:`py2neo.Node`
        """
        return self.__relationship.start_node

    @property
    def end_node(self):
        """ The end node of the created relationship.

        :rtype: :class:`py2neo.Node`
        """
        return self.__relationship.end_node

    @property
    def type(self):
        """ The type of the created relationship.`
        """
        return self.__relationship.type

    @property
    def properties(self):
        """ The full set of properties to apply to the created relationship.

        :rtype: :class:`py2neo.PropertySet`
        """
        return self.__relationship.properties

    def set(self, **properties):
        """ Extra properties to apply to the relationship.
        """
        self.__relationship.properties.update(properties)
        return self

    def with_return(self):
        """ Include a RETURN clause in the statement.
        """
        self.__return = True
        return self

    @property
    def statement(self):
        """ The full Cypher statement.
        """
        # TODO: MATCH bound nodes, CREATE unbound nodes, CREATE rel, RETURN rel
        # TODO: chain together CreateNode or MatchNode tasks
        # StatementChain(MatchOrCreateNode(start_node).named("a"), MatchOrCreate(self.end_node).named("b"), )
        string = StringIO()
        writer = CypherWriter(string)
        writer.write_literal("CREATE ")
        writer.write_node(self.__relationship, "a" if self.__return else None,
                          CypherParameter("P") if self.__relationship.properties else None)
        if self.__return:
            writer.write_literal(" RETURN a")
        return string.getvalue()

    @property
    def parameters(self):
        """ Dictionary of parameters.
        """
        if self.__relationship.properties:
            return {"P": self.properties}
        else:
            return {}


class MergeNode(CypherTask):
    """ :class:`.CypherTask` for `merging <http://neo4j.com/docs/stable/query-merge.html>`_
    nodes.

    ::

        >>> from py2neo import Graph
        >>> graph = Graph()
        >>> tx = graph.cypher.begin()
        >>> tx.append(MergeNode("Person", "name", "Alice"))
        >>> tx.commit()
           | a
        ---+-----------------------
         1 | (n170 {name:"Alice"})


    """

    def __init__(self, primary_label, primary_key=None, primary_value=None):
        CypherTask.__init__(self)
        self.__node = Node(primary_label)
        if primary_key is not None:
            self.__node.properties[primary_key] = CypherParameter("V", primary_value)
        self.__labels = LabelSet()
        self.__properties = PropertySet()
        self.__return = False

    @property
    def labels(self):
        """ The full set of labels to apply to the merged node.

        :rtype: :class:`py2neo.LabelSet`
        """
        l = LabelSet(self.__labels)
        l.update(self.__node.labels)
        return l

    @property
    def properties(self):
        """ The full set of properties to apply to the merged node.

        :rtype: :class:`py2neo.PropertySet`
        """
        p = PropertySet(self.__properties)
        if self.primary_key:
            p[self.primary_key] = self.primary_value
        return p

    @property
    def primary_label(self):
        """ The label on which to merge.
        """
        return list(self.__node.labels)[0]

    @property
    def primary_key(self):
        """ The property key on which to merge.
        """
        try:
            return list(self.__node.properties.keys())[0]
        except IndexError:
            return None

    @property
    def primary_value(self):
        """ The property value on which to merge.
        """
        try:
            return list(self.__node.properties.values())[0].value
        except IndexError:
            return None

    def set(self, *labels, **properties):
        """ Extra labels and properties to apply to the node.

            >>> merge = MergeNode("Person", "name", "Bob").set("Employee", employee_id=1234)

        """
        self.__labels.update(labels)
        self.__properties.update(properties)
        return self

    def with_return(self):
        """ Include a RETURN clause in the statement.
        """
        self.__return = True
        return self

    @property
    def statement(self):
        """ The full Cypher statement.
        """
        string = StringIO()
        writer = CypherWriter(string)
        writer.write_literal("MERGE ")
        if self.__labels or self.__properties or self.__return:
            node_name = "a"
        else:
            node_name = None
        writer.write_node(self.__node, node_name)
        if self.__labels:
            writer.write_literal(" SET a")
            for label in self.__labels:
                writer.write_label(label)
        if self.__properties:
            writer.write_literal(" SET a={P}")
        if self.__return:
            writer.write_literal(" RETURN a")
        return string.getvalue()

    @property
    def parameters(self):
        """ Dictionary of parameters.
        """
        parameters = {}
        if self.__node.properties:
            parameters["V"] = self.primary_value
        if self.__properties:
            parameters["P"] = self.properties
        return parameters

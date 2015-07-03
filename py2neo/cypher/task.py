#!/usr/bin/env python
# -*- encoding: utf-8 -*-

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

from py2neo.core import Node, LabelSet, PropertySet
from py2neo.cypher.lang import CypherParameter, CypherWriter
from py2neo.util import ustr, xstr


__all__ = ["CypherTask", "CreateNode", "MergeNode", "CreateUniqueConstraint"]


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
        writer.write_node(self.__node, "a" if self.__return else None,
                          CypherParameter("P") if self.__node.properties else None)
        if self.__return:
            writer.write_literal(" RETURN a")
        return string.getvalue()

    @property
    def parameters(self):
        """ Dictionary of parameters.
        """
        if self.__node.properties:
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


class CreateUniqueConstraint(CypherTask):

    def __init__(self, label, property_key):
        CypherTask.__init__(self)
        self.label = label
        self.property_key = property_key

    @property
    def statement(self):
        """ The full Cypher statement.
        """
        string = StringIO()
        writer = CypherWriter(string)
        writer.write_literal("CREATE CONSTRAINT ON ")
        writer.write_node(Node(self.label), "a")
        writer.write_literal(" ASSERT a.")
        writer.write_identifier(self.property_key)
        writer.write_literal(" IS UNIQUE")
        return string.getvalue()

    @property
    def parameters(self):
        """ Dictionary of parameters.
        """
        return {}

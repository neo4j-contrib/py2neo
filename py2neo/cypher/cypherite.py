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


from py2neo.core import LabelSet, PropertySet
from py2neo.cypher.lang import cypher_escape
from py2neo.util import ustr, xstr


class Cypherite(object):
    """ The Cypherite class can either be used directly or as
    a base class for more specific statement implementations.
    """

    def __init__(self, statement="", parameters=None, **kwparameters):
        self.__statement = statement
        self.__parameters = dict(parameters or {}, **kwparameters)

    def __repr__(self):
        return "<Cypherite statement=%r parameters=%r>" % (self.statement, self.parameters)

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


class MergeNode(Cypherite):
    """ :class:`.Cypherite` for `merging <http://neo4j.com/docs/stable/query-merge.html>`_
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
        Cypherite.__init__(self)
        self.__labels = LabelSet([primary_label])
        self.__properties = PropertySet()
        if primary_key is not None:
            self.__properties[primary_key] = primary_value
        self.__primary_label = primary_label
        self.__primary_key = primary_key
        self.__return_node = False

    @property
    def labels(self):
        """ The full set of labels to apply to the merged node.

        :rtype: :class:`py2neo.LabelSet`
        """
        return self.__labels

    @property
    def properties(self):
        """ The full set of properties to apply to the merged node.

        :rtype: :class:`py2neo.PropertySet`
        """
        return self.__properties

    @property
    def primary_label(self):
        """ The label on which to merge.
        """
        return self.__primary_label

    @property
    def primary_key(self):
        """ The property key on which to merge.
        """
        return self.__primary_key

    @property
    def primary_value(self):
        """ The property value on which to merge.
        """
        primary_key = self.primary_key
        if primary_key is None:
            return None
        else:
            return self.properties.get(primary_key)

    def set(self, *labels, **properties):
        """ Extra labels and properties to apply to the merged node.

            >>> merge = MergeNode("Person", "name", "Bob").set("Employee", employee_id=1234)

        """
        self.__labels.update(labels)
        self.__properties.update(properties)
        return self

    def with_return(self):
        self.__return_node = True
        return self

    @property
    def statement(self):
        """ The Cypher MERGE statement.
        """
        lines = []
        if self.primary_key is None:
            lines.append("MERGE (a:%s)" % cypher_escape(self.primary_label))
        else:
            lines.append("MERGE (a:%s {%s:{V}})" % (cypher_escape(self.primary_label),
                                                    cypher_escape(self.primary_key)))
        if len(self.labels) > 1:
            lines.append("SET a:" + ":".join(cypher_escape(l)
                                             for l in self.labels
                                             if l != self.primary_label))
        if len(self.properties) > 1:
            lines.append("SET a={P}")
        if self.__return_node:
            lines.append("RETURN a")
        return "\n".join(lines)

    @property
    def parameters(self):
        """ Dictionary of parameters.
        """
        parameters = {}
        if self.primary_key is not None:
            parameters["V"] = self.primary_value
        if len(self.properties) > 1:
            parameters["P"] = self.properties
        return parameters

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


class CypherBuilder(object):

    def __init__(self, statement="", parameters=None):
        self.__statement = statement
        self.__parameters = dict(parameters or {})

    def __repr__(self):
        return "<CypherBuilder statement=%r parameters=%r>" % (self.statement, self.parameters)

    def __str__(self):
        return xstr(self.statement)

    def __unicode__(self):
        return ustr(self.statement)

    @property
    def statement(self):
        return self.__statement

    @property
    def parameters(self):
        return self.__parameters


class MergeNode(CypherBuilder):

    def __init__(self, primary_label, primary_key=None, primary_value=None):
        CypherBuilder.__init__(self)
        self.__labels = LabelSet([primary_label])
        self.__properties = PropertySet({}, **{primary_key: primary_value})
        self.__primary_label = primary_label
        self.__primary_key = primary_key

    @property
    def labels(self):
        return self.__labels

    @property
    def properties(self):
        return self.__properties

    @property
    def primary_label(self):
        return self.__primary_label

    @property
    def primary_key(self):
        return self.__primary_key

    @property
    def primary_value(self):
        primary_key = self.primary_key
        if primary_key is None:
            return None
        else:
            return self.properties.get(primary_key)

    def set(self, *labels, **properties):
        self.__labels.update(labels)
        self.__properties.update(properties)
        return self

    @property
    def statement(self):
        lines = []
        if self.primary_key is None:
            lines.append("MERGE (n:%s)" % cypher_escape(self.primary_label))
        else:
            lines.append("MERGE (n:%s {%s:{V}})" % (cypher_escape(self.primary_label),
                                                    cypher_escape(self.primary_key)))
        if len(self.labels) > 1:
            lines.append("SET n:" + ":".join(cypher_escape(l)
                                             for l in self.labels
                                             if l != self.primary_label))
        lines.append("SET n={P}")
        lines.append("RETURN n")
        return "\n".join(lines)

    @property
    def parameters(self):
        parameters = {"P": self.properties}
        if self.primary_key is not None:
            parameters["V"] = self.primary_value
        return parameters

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


from xml.dom import minidom
from xml.etree.ElementTree import Element, SubElement, tostring


class GexfDocument(object):

    def __init__(self):
        self.root = Element("gexf")
        self.meta = SubElement(self.root, "meta")
        self.graph = SubElement(self.root, "graph")
        self.attributes = SubElement(self.graph, "attributes")
        self.nodes = SubElement(self.graph, "nodes")
        self.edges = SubElement(self.graph, "edges")
        self.attribute_index = {}

    def __repr__(self):
        rough_string = tostring(self.root, 'utf-8')
        reparsed = minidom.parseString(rough_string)
        return reparsed.toprettyxml(indent="  ")

    def get_or_add_attribute(self, title, type_):
        key = (title, type_)
        if key in self.attribute_index:
            return self.attribute_index[key]
        else:
            id_ = len(self.attribute_index)
            self.attribute_index[key] = id_
            SubElement(self.attributes, "attribute", {"id": str(id_), "title": title, "type": type_})
            return id_

    def add_node(self, id_, attributes):
        node = SubElement(self.nodes, "node", {"id": str(id_)})
        if attributes:
            att_values = SubElement(node, "attvalues")
            for key in attributes:
                value = attributes[key]
                attribute_id = self.get_or_add_attribute(key, value.__class__.__name__.lower())
                SubElement(att_values, "attvalue", {"for": str(attribute_id), "value": str(value)})

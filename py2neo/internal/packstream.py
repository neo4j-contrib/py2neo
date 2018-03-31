#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright 2011-2017, Nigel Small
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


from __future__ import absolute_import

from collections import namedtuple

from neo4j.v1 import Hydrator, Structure

from py2neo.internal.hydration import hydrate_node
from py2neo.types import Node, Relationship, Path


_unbound_relationship = namedtuple("UnboundRelationship", ["id", "type", "properties"])


class PackStreamHydrator(Hydrator):

    def __init__(self, graph, keys, entities=None):
        self.graph = graph
        self.keys = keys
        self.entities = entities or {}

    def hydrate(self, values):
        """ Hydrate values from raw PackStream representations into client objects.
        """
        graph = self.graph
        entities = self.entities
        keys = self.keys

        def hydrate_(obj, inst=None):
            # TODO: hydrate directly instead of via HTTP hydration
            if isinstance(obj, Structure):
                tag = obj.tag
                fields = obj.fields
                if tag == b"N":
                    return Node.hydrate(graph, fields[0], inst=inst,
                                        metadata={"labels": list(fields[1])}, data=hydrate_(fields[2]))
                elif tag == b"R":
                    return Relationship.hydrate(graph, fields[0], inst=inst,
                                                start=fields[1], end=fields[2],
                                                type=fields[3], data=hydrate_(fields[4]))
                elif tag == b"P":
                    nodes = [hydrate_(node) for node in fields[0]]
                    u_rels = [_unbound_relationship(*map(hydrate_, r)) for r in fields[1]]
                    sequence = fields[2]
                    last_node = nodes[0]
                    steps = [last_node]
                    for i, rel_index in enumerate(sequence[::2]):
                        next_node = nodes[sequence[2 * i + 1]]
                        if rel_index > 0:
                            u_rel = u_rels[rel_index - 1]
                            rel = Relationship.hydrate(graph, u_rel.id,
                                                       start=last_node.identity, end=next_node.identity,
                                                       type=u_rel.type, data=u_rel.properties)
                        else:
                            u_rel = u_rels[-rel_index - 1]
                            rel = Relationship.hydrate(graph, u_rel.id,
                                                       start=next_node.identity, end=last_node.identity,
                                                       type=u_rel.type, data=u_rel.properties)
                        steps.append(rel)
                        steps.append(next_node)
                        last_node = next_node
                    return Path(*steps)
                else:
                    # If we don't recognise the structure type, just return it as-is
                    # TODO: add warning for unsupported structure types
                    return obj
            elif isinstance(obj, list):
                return list(map(hydrate_, obj))
            elif isinstance(obj, dict):
                return {key: hydrate_(value) for key, value in obj.items()}
            else:
                return obj

        return tuple(hydrate_(value, entities.get(keys[i])) for i, value in enumerate(values))

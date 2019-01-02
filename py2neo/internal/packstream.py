#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright 2011-2019, Nigel Small
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

from py2neo.data import Record
from py2neo.internal.hydration import hydrate_node, hydrate_relationship, Unknown


_unbound_relationship = namedtuple("UnboundRelationship", ["id", "type", "properties"])


class PackStreamHydrator(object):

    def __init__(self, graph, keys, entities=None):
        # TODO: protocol version
        # super(PackStreamHydrator, self).__init__(2)  # maximum known protocol version
        self.graph = graph
        self.keys = keys
        self.entities = entities or {}

    def hydrate_records(self, keys, record_values):
        for values in record_values:
            yield Record(zip(keys, self.hydrate(values)))

    def hydrate(self, values):
        """ Convert PackStream values into native values.
        """
        from neobolt.packstream import Structure

        graph = self.graph
        entities = self.entities
        keys = self.keys

        def hydrate_(obj, inst=None):
            if isinstance(obj, Structure):
                tag = obj.tag
                fields = obj.fields
                if tag == b"N":
                    return hydrate_node(graph, fields[0], inst=inst,
                                        metadata={"labels": fields[1]}, data=hydrate_(fields[2]))
                elif tag == b"R":
                    return hydrate_relationship(graph, fields[0], inst=inst,
                                                start=fields[1], end=fields[2],
                                                type=fields[3], data=hydrate_(fields[4]))
                elif tag == b"P":
                    from py2neo.data import Path
                    nodes = [hydrate_(node) for node in fields[0]]
                    u_rels = [_unbound_relationship(*map(hydrate_, r)) for r in fields[1]]
                    sequence = fields[2]
                    last_node = nodes[0]
                    steps = [last_node]
                    for i, rel_index in enumerate(sequence[::2]):
                        next_node = nodes[sequence[2 * i + 1]]
                        if rel_index > 0:
                            u_rel = u_rels[rel_index - 1]
                            rel = hydrate_relationship(graph, u_rel.id,
                                                       start=last_node.identity, end=next_node.identity,
                                                       type=u_rel.type, data=u_rel.properties)
                        else:
                            u_rel = u_rels[-rel_index - 1]
                            rel = hydrate_relationship(graph, u_rel.id,
                                                       start=next_node.identity, end=last_node.identity,
                                                       type=u_rel.type, data=u_rel.properties)
                        steps.append(rel)
                        steps.append(next_node)
                        last_node = next_node
                    return Path(*steps)
                else:
                    try:
                        f = self.hydration_functions[obj.tag]
                    except KeyError:
                        # If we don't recognise the structure type, just return it as-is
                        return obj
                    else:
                        return f(*map(hydrate_, obj.fields))
            elif isinstance(obj, list):
                return list(map(hydrate_, obj))
            elif isinstance(obj, dict):
                return {key: hydrate_(value) for key, value in obj.items()}
            else:
                return obj

        return tuple(hydrate_(value, entities.get(keys[i])) for i, value in enumerate(values))

    # def hydrate(self, values):
    #     """ Hydrate values from raw PackStream representations into client objects.
    #     """
    #     from neo4j.packstream import Structure
    #
    #     graph = self.graph
    #     entities = self.entities
    #     keys = self.keys
    #
    #     def hydrate_(obj, inst=None):
    #         if isinstance(obj, Structure):
    #             tag = obj.tag
    #             fields = obj.fields
    #             if tag == b"N":
    #                 return hydrate_node(graph, fields[0], inst=inst,
    #                                     metadata={"labels": list(fields[1])}, data=hydrate_(fields[2]))
    #             elif tag == b"R":
    #                 return hydrate_relationship(graph, fields[0], inst=inst,
    #                                             start=fields[1], end=fields[2],
    #                                             type=fields[3], data=hydrate_(fields[4]))
    #             elif tag == b"P":
    #                 from py2neo.data import Path
    #                 nodes = [hydrate_(node) for node in fields[0]]
    #                 u_rels = [_unbound_relationship(*map(hydrate_, r)) for r in fields[1]]
    #                 sequence = fields[2]
    #                 last_node = nodes[0]
    #                 steps = [last_node]
    #                 for i, rel_index in enumerate(sequence[::2]):
    #                     next_node = nodes[sequence[2 * i + 1]]
    #                     if rel_index > 0:
    #                         u_rel = u_rels[rel_index - 1]
    #                         rel = hydrate_relationship(graph, u_rel.id,
    #                                                    start=last_node.identity, end=next_node.identity,
    #                                                    type=u_rel.type, data=u_rel.properties)
    #                     else:
    #                         u_rel = u_rels[-rel_index - 1]
    #                         rel = hydrate_relationship(graph, u_rel.id,
    #                                                    start=next_node.identity, end=last_node.identity,
    #                                                    type=u_rel.type, data=u_rel.properties)
    #                     steps.append(rel)
    #                     steps.append(next_node)
    #                     last_node = next_node
    #                 return Path(*steps)
    #             else:
    #                 # Defer everything else to the official driver
    #                 return super(PackStreamHydrator, self).hydrate([obj])[0]
    #         elif isinstance(obj, list):
    #             return list(map(hydrate_, obj))
    #         elif isinstance(obj, dict):
    #             return {key: hydrate_(value) for key, value in obj.items()}
    #         else:
    #             return obj
    #
    #     return tuple(hydrate_(value, entities.get(keys[i])) for i, value in enumerate(values))

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


from neo4j.v1 import ValueSystem, Structure, UnboundRelationship

from py2neo.types import Node, Relationship, Path, remote


class Py2neoPackStreamValueSystem(ValueSystem):

    def __init__(self, graph, entities, keys):
        self.graph = graph
        self.keys = keys
        self.entities = entities

    def hydrate(self, values):
        """ Hydrate values from raw representations into client objects.
        """
        graph_uri = remote(self.graph).uri.string
        entities = self.entities
        keys = self.keys

        def hydrate_(obj, inst=None):
            # TODO: hydrate directly instead of via HTTP hydration
            if isinstance(obj, Structure):
                signature, args = obj
                if signature == b"N":
                    uri = "%snode/%s" % (graph_uri, args[0])
                    return Node.hydrate(uri, inst=inst, metadata={"labels": list(args[1])}, data=hydrate_(args[2]))
                elif signature == b"R":
                    return Relationship.hydrate({
                        "self": "%srelationship/%s" % (graph_uri, args[0]),
                        "start": "%snode/%s" % (graph_uri, args[1]),
                        "end": "%snode/%s" % (graph_uri, args[2]),
                        "type": args[3],
                        "data": hydrate_(args[4]),
                    }, inst)
                elif signature == b"P":
                    nodes = [hydrate_(node) for node in args[0]]
                    u_rels = [UnboundRelationship.hydrate(*map(hydrate_, r)) for _, r in args[1]]
                    sequence = args[2]
                    last_node = nodes[0]
                    steps = [last_node]
                    for i, rel_index in enumerate(sequence[::2]):
                        next_node = nodes[sequence[2 * i + 1]]
                        if rel_index > 0:
                            u_rel = u_rels[rel_index - 1]
                            data = {"self": "%srelationship/%s" % (graph_uri, u_rel.id),
                                    "start": "%snode/%s" % (graph_uri, remote(last_node)._id),
                                    "end": "%snode/%s" % (graph_uri, remote(next_node)._id),
                                    "type": u_rel.type, "data": u_rel.properties}
                            rel = Relationship.hydrate(data)
                        else:
                            u_rel = u_rels[-rel_index - 1]
                            data = {"self": "%srelationship/%s" % (graph_uri, u_rel.id),
                                    "start": "%snode/%s" % (graph_uri, remote(next_node)._id),
                                    "end": "%snode/%s" % (graph_uri, remote(last_node)._id),
                                    "type": u_rel.type, "data": u_rel.properties}
                            rel = Relationship.hydrate(data)
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

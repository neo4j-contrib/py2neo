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


from collections import namedtuple

from neobolt.packstream import Structure

from py2neo.data import Node, Relationship, Path
from py2neo.matching import RelationshipMatcher


class Hydrator(object):

    def hydrate(self, values):
        raise NotImplementedError()


class PackStreamHydrator(Hydrator):

    unbound_relationship = namedtuple("UnboundRelationship", ["id", "type", "properties"])

    def __init__(self, version, graph, keys, entities=None):
        self.version = version
        self.graph = graph
        self.keys = keys
        self.entities = entities or {}
        self.hydration_functions = {}

    def hydrate(self, values):
        """ Convert PackStream values into native values.
        """

        graph = self.graph
        entities = self.entities
        keys = self.keys

        def hydrate_object(obj, inst=None):
            if isinstance(obj, Structure):
                tag = obj.tag
                fields = obj.fields
                if tag == b"N":
                    return hydrate_node(fields[0], inst=inst,
                                        metadata={"labels": fields[1]}, data=hydrate_object(fields[2]))
                elif tag == b"R":
                    return hydrate_relationship(fields[0], inst=inst,
                                                start=fields[1], end=fields[2],
                                                type=fields[3], data=hydrate_object(fields[4]))
                elif tag == b"P":
                    # Herein lies a dirty hack to retrieve missing relationship
                    # detail for paths received over HTTP.
                    nodes = [hydrate_object(node) for node in fields[0]]
                    u_rels = []
                    typeless_u_rel_ids = []
                    for r in fields[1]:
                        u_rel = self.unbound_relationship(*map(hydrate_object, r))
                        u_rels.append(u_rel)
                        if u_rel.type is None:
                            typeless_u_rel_ids.append(u_rel.id)
                    if typeless_u_rel_ids:
                        r_dict = {r.identity: r for r in RelationshipMatcher(graph).get(typeless_u_rel_ids)}
                        for i, u_rel in enumerate(u_rels):
                            if u_rel.type is None:
                                u_rels[i] = self.unbound_relationship(
                                    u_rel.id,
                                    type(r_dict[u_rel.id]).__name__,
                                    u_rel.properties
                                )
                    sequence = fields[2]
                    last_node = nodes[0]
                    steps = [last_node]
                    for i, rel_index in enumerate(sequence[::2]):
                        next_node = nodes[sequence[2 * i + 1]]
                        if rel_index > 0:
                            u_rel = u_rels[rel_index - 1]
                            rel = hydrate_relationship(u_rel.id,
                                                       start=last_node.identity, end=next_node.identity,
                                                       type=u_rel.type, data=u_rel.properties)
                        else:
                            u_rel = u_rels[-rel_index - 1]
                            rel = hydrate_relationship(u_rel.id,
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
                        return f(*map(hydrate_object, obj.fields))
            elif isinstance(obj, list):
                return list(map(hydrate_object, obj))
            elif isinstance(obj, dict):
                return {key: hydrate_object(value) for key, value in obj.items()}
            else:
                return obj

        def hydrate_node(identity, inst=None, **rest):
            if inst is None:

                def inst_constructor():
                    new_inst = Node()
                    new_inst.graph = graph
                    new_inst.identity = identity
                    new_inst._stale.update({"labels", "properties"})
                    return new_inst

                inst = graph.node_cache.update(identity, inst_constructor)
            else:
                inst.graph = graph
                inst.identity = identity
                graph.node_cache.update(identity, inst)

            properties = rest.get("data")
            if properties is not None:
                inst._stale.discard("properties")
                inst.clear()
                inst.update(properties)

            labels = rest.get("metadata", {}).get("labels")
            if labels is not None:
                inst._stale.discard("labels")
                inst._remote_labels = frozenset(labels)
                inst.clear_labels()
                inst.update_labels(labels)

            return inst

        def hydrate_relationship(identity, inst=None, **rest):
            start = rest["start"]
            end = rest["end"]

            if inst is None:

                def inst_constructor():
                    properties = rest.get("data")
                    if properties is None:
                        new_inst = Relationship(hydrate_node(start), rest.get("type"),
                                                hydrate_node(end))
                        new_inst._stale.add("properties")
                    else:
                        new_inst = Relationship(hydrate_node(start), rest.get("type"),
                                                hydrate_node(end), **properties)
                    new_inst.graph = graph
                    new_inst.identity = identity
                    return new_inst

                inst = graph.relationship_cache.update(identity, inst_constructor)
            else:
                inst.graph = graph
                inst.identity = identity
                hydrate_node(start, inst=inst.start_node)
                hydrate_node(end, inst=inst.end_node)
                inst._type = rest.get("type")
                if "data" in rest:
                    inst.clear()
                    inst.update(rest["data"])
                else:
                    inst._stale.add("properties")
                graph.relationship_cache.update(identity, inst)
            return inst

        return tuple(hydrate_object(value, entities.get(keys[i])) for i, value in enumerate(values))


class JSONHydrator(Hydrator):

    def __init__(self, version):
        self.version = version
        if self.version != "rest":
            raise ValueError("Unsupported JSON version %r" % self.version)

    def hydrate(self, values):
        raise NotImplementedError()

#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright 2011-2018, Nigel Small
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


from py2neo.internal.collections import round_robin


def hydrate_node(graph, identity, inst=None, **rest):
    if inst is None:

        def inst_constructor():
            from py2neo.types import Node
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

    if "data" in rest:
        inst._stale.discard("properties")
        inst.clear()
        inst.update(rest["data"])
    if "metadata" in rest:
        inst._stale.discard("labels")
        metadata = rest["metadata"]
        inst._remote_labels = frozenset(metadata["labels"])
        inst.clear_labels()
        inst.update_labels(metadata["labels"])
    return inst


def hydrate_relationship(graph, identity, inst=None, **rest):
    start = rest["start"]
    end = rest["end"]

    if inst is None:

        def inst_constructor():
            from py2neo.types import Relationship
            new_inst = Relationship(hydrate_node(graph, start), rest.get("type"),
                                    hydrate_node(graph, end), **rest.get("data", {}))
            new_inst.graph = graph
            new_inst.identity = identity
            return new_inst

        inst = graph.relationship_cache.update(identity, inst_constructor)
    else:
        inst.graph = graph
        inst.identity = identity
        hydrate_node(graph, start, inst=inst.start_node)
        hydrate_node(graph, end, inst=inst.end_node)
        inst._type = rest.get("type")
        if "data" in rest:
            inst.clear()
            inst.update(rest["data"])
        else:
            inst._stale.add("properties")
        graph.relationship_cache.update(identity, inst)
    return inst


def hydrate_path(graph, data):
    from py2neo.types import Path
    node_ids = data["nodes"]
    relationship_ids = data["relationships"]
    offsets = [(0, 1) if direction == "->" else (1, 0) for direction in data["directions"]]
    nodes = [hydrate_node(graph, identity) for identity in node_ids]
    relationships = [hydrate_relationship(graph, identity,
                                          start=node_ids[i + offsets[i][0]],
                                          end=node_ids[i + offsets[i][1]])
                     for i, identity in enumerate(relationship_ids)]
    inst = Path(*round_robin(nodes, relationships))
    inst.__metadata = data
    return inst

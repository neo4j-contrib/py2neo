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
        inst.__remote_labels = frozenset(metadata["labels"])
        inst.clear_labels()
        inst.update_labels(metadata["labels"])
    return inst

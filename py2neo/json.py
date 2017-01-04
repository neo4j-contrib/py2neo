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


from neo4j.v1 import ValueSystem

from py2neo.remoting import remote
from py2neo.types import Node, Relationship, Path
from py2neo.util import is_collection


class JSONValueSystem(ValueSystem):

    def __init__(self, graph, keys, entities=None):
        self.graph = graph
        self.keys = keys
        self.entities = entities or {}

    def hydrate(self, values):
        """ Hydrate values from raw JSON representations into client objects.
        """
        graph = self.graph
        entities = self.entities
        keys = self.keys

        def hydrate_(data, inst=None):
            if isinstance(data, dict):
                if "self" in data:
                    if "type" in data:
                        return Relationship.hydrate(data["self"], inst=inst, **data)
                    else:
                        return Node.hydrate(data["self"], inst=inst, **data)
                elif "nodes" in data and "relationships" in data:
                    if "directions" not in data:
                        directions = []
                        relationships = graph.evaluate(
                            "MATCH ()-[r]->() WHERE id(r) IN {x} RETURN collect(r)",
                            x=[int(uri.rpartition("/")[-1]) for uri in data["relationships"]])
                        node_uris = data["nodes"]
                        for i, relationship in enumerate(relationships):
                            if remote(relationship.start_node()).uri == node_uris[i]:
                                directions.append("->")
                            else:
                                directions.append("<-")
                        data["directions"] = directions
                    return Path.hydrate(data)
                else:
                    from warnings import warn
                    warn("Map literals returned over the Neo4j REST interface are ambiguous "
                         "and may be hydrated as graph objects")
                    return data
            elif is_collection(data):
                return type(data)(map(hydrate_, data))
            else:
                return data

        return tuple(hydrate_(value, entities.get(keys[i])) for i, value in enumerate(values))

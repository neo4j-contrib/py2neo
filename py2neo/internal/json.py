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


from __future__ import absolute_import

from py2neo.internal.collections import is_collection
from py2neo.internal.compat import integer_types, string_types, ustr, bytes_types
from py2neo.internal.hydration import hydrate_node, hydrate_relationship, hydrate_path


INT64_MIN = -(2 ** 63)
INT64_MAX = (2 ** 63) - 1


class JSONHydrator(object):

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

        def uri_to_id(uri):
            _, _, identity = uri.rpartition("/")
            return int(identity)

        def hydrate_(data, inst=None):
            if isinstance(data, dict):
                if "self" in data:
                    if "type" in data:
                        data["start"] = uri_to_id(data["start"])
                        data["end"] = uri_to_id(data["end"])
                        return hydrate_relationship(graph, uri_to_id(data["self"]), inst=inst, **data)
                    else:
                        return hydrate_node(graph, uri_to_id(data["self"]), inst=inst, **data)
                elif "nodes" in data and "relationships" in data:
                    data["nodes"] = list(map(uri_to_id, data["nodes"]))
                    data["relationships"] = list(map(uri_to_id, data["relationships"]))
                    if "directions" not in data:
                        directions = []
                        relationships = graph.evaluate(
                            "MATCH ()-[r]->() WHERE id(r) IN {x} RETURN collect(r)",
                            x=data["relationships"])
                        for i, relationship in enumerate(relationships):
                            if relationship.start_node.identity == data["nodes"][i]:
                                directions.append("->")
                            else:
                                directions.append("<-")
                        data["directions"] = directions
                    return hydrate_path(graph, data)
                else:
                    # from warnings import warn
                    # warn("Map literals returned over the Neo4j REST interface are ambiguous "
                    #      "and may be hydrated as graph objects")
                    return data
            elif is_collection(data):
                return type(data)(map(hydrate_, data))
            else:
                return data

        return tuple(hydrate_(value, entities.get(keys[i])) for i, value in enumerate(values))


class JSONDehydrator(object):

    def __init__(self):
        self.dehydration_functions = {}

    def dehydrate(self, values):
        """ Convert native values into PackStream values.
        """

        def dehydrate_(obj):
            try:
                f = self.dehydration_functions[type(obj)]
            except KeyError:
                pass
            else:
                return f(obj)
            if obj is None:
                return None
            elif isinstance(obj, bool):
                return obj
            elif isinstance(obj, integer_types):
                if INT64_MIN <= obj <= INT64_MAX:
                    return obj
                raise ValueError("Integer out of bounds (64-bit signed integer values only)")
            elif isinstance(obj, float):
                return obj
            elif isinstance(obj, string_types):
                return ustr(obj)
            elif isinstance(obj, bytes_types):  # order is important here - bytes must be checked after string
                raise TypeError("Parameters passed over JSON do not support BYTES")
            elif isinstance(obj, list):
                return list(map(dehydrate_, obj))
            elif isinstance(obj, dict):
                return {key: dehydrate_(value) for key, value in obj.items()}
            else:
                raise TypeError(obj)

        return tuple(map(dehydrate_, values))

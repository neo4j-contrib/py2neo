#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright 2011-2020, Nigel Small
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

from py2neo.internal.compat import Sequence, Mapping, integer_types, string_types
from py2neo.internal.hydration.spatial import (
    hydration_functions as spatial_hydration_functions,
    dehydration_functions as spatial_dehydration_functions,
)
from py2neo.internal.hydration.temporal import (
    hydration_functions as temporal_hydration_functions,
    dehydration_functions as temporal_dehydration_functions,
)
from py2neo.matching import RelationshipMatcher
from py2neo.net.packstream import Structure


INT64_LO = -(2 ** 63)
INT64_HI = 2 ** 63 - 1


def uri_to_id(uri):
    """ Utility function to convert entity URIs into numeric identifiers.
    """
    _, _, identity = uri.rpartition("/")
    return int(identity)


class Hydrator(object):

    def __init__(self, graph):
        self.graph = graph

    def hydrate(self, keys, values):
        raise NotImplementedError()

    def dehydrate(self, values):
        raise NotImplementedError()

    def hydrate_node(self, instance, identity, labels=None, properties=None):
        if instance is None:

            def instance_constructor():
                from py2neo.data import Node
                new_instance = Node()
                new_instance.graph = self.graph
                new_instance.identity = identity
                new_instance._stale.update({"labels", "properties"})
                return new_instance

            instance = self.graph.node_cache.update(identity, instance_constructor)
        else:
            instance.graph = self.graph
            instance.identity = identity
            self.graph.node_cache.update(identity, instance)

        if properties is not None:
            instance._stale.discard("properties")
            instance.clear()
            instance.update(properties)

        if labels is not None:
            instance._stale.discard("labels")
            instance._remote_labels = frozenset(labels)
            instance.clear_labels()
            instance.update_labels(labels)

        return instance

    def hydrate_relationship(self, instance, identity, start, end, type=None, properties=None):

        if instance is None:

            def instance_constructor():
                from py2neo.data import Relationship
                if properties is None:
                    new_instance = Relationship(self.hydrate_node(None, start), type,
                                                self.hydrate_node(None, end))
                    new_instance._stale.add("properties")
                else:
                    new_instance = Relationship(self.hydrate_node(None, start), type,
                                                self.hydrate_node(None, end), **properties)
                new_instance.graph = self.graph
                new_instance.identity = identity
                return new_instance

            instance = self.graph.relationship_cache.update(identity, instance_constructor)
        else:
            instance.graph = self.graph
            instance.identity = identity
            self.hydrate_node(instance.start_node, start)
            self.hydrate_node(instance.end_node, end)
            instance._type = type
            if properties is None:
                instance._stale.add("properties")
            else:
                instance.clear()
                instance.update(properties)
            self.graph.relationship_cache.update(identity, instance)
        return instance


class PackStreamHydrator(Hydrator):

    unbound_relationship = namedtuple("UnboundRelationship", ["id", "type", "properties"])

    def __init__(self, version, graph, entities=None):
        super(PackStreamHydrator, self).__init__(graph)
        self.version = version if isinstance(version, tuple) else (version, 0)
        self.entities = entities or {}
        self.hydration_functions = {}
        self.dehydration_functions = {}
        if self.version >= (2, 0):
            self.hydration_functions.update(temporal_hydration_functions())
            self.hydration_functions.update(spatial_hydration_functions())
            self.dehydration_functions.update(temporal_dehydration_functions())
            self.dehydration_functions.update(spatial_dehydration_functions())

    def hydrate(self, keys, values):
        """ Convert PackStream values into native values.
        """
        return tuple(self.hydrate_object(value, self.entities.get(keys[i]))
                     for i, value in enumerate(values))

    def hydrate_object(self, obj, inst=None):
        if isinstance(obj, Structure):
            tag = obj.tag if isinstance(obj.tag, bytes) else bytes(bytearray([obj.tag]))
            fields = obj.fields
            if tag == b"N":
                return self.hydrate_node(inst, fields[0], fields[1], self.hydrate_object(fields[2]))
            elif tag == b"R":
                return self.hydrate_relationship(inst, fields[0], fields[1], fields[2], fields[3],
                                                 self.hydrate_object(fields[4]))
            elif tag == b"P":
                return self.hydrate_path(*fields)
            else:
                try:
                    f = self.hydration_functions[tag]
                except KeyError:
                    # If we don't recognise the structure type, just return it as-is
                    return obj
                else:
                    return f(*map(self.hydrate_object, obj.fields))
        elif isinstance(obj, list):
            return list(map(self.hydrate_object, obj))
        elif isinstance(obj, dict):
            return {key: self.hydrate_object(value) for key, value in obj.items()}
        else:
            return obj

    def hydrate_path(self, nodes, relationships, sequence):
        from py2neo.data import Path
        nodes = [self.hydrate_node(None, n_id, n_label, self.hydrate_object(n_properties))
                 for n_id, n_label, n_properties in nodes]
        u_rels = []
        for r_id, r_type, r_properties in relationships:
            u_rel = self.unbound_relationship(r_id, r_type, self.hydrate_object(r_properties))
            u_rels.append(u_rel)
        last_node = nodes[0]
        steps = [last_node]
        for i, rel_index in enumerate(sequence[::2]):
            next_node = nodes[sequence[2 * i + 1]]
            if rel_index > 0:
                u_rel = u_rels[rel_index - 1]
                rel = self.hydrate_relationship(None, u_rel.id,
                                                last_node.identity, next_node.identity,
                                                u_rel.type, u_rel.properties)
            else:
                u_rel = u_rels[-rel_index - 1]
                rel = self.hydrate_relationship(None, u_rel.id,
                                                next_node.identity, last_node.identity,
                                                u_rel.type, u_rel.properties)
            steps.append(rel)
            # steps.append(next_node)
            last_node = next_node
        return Path(*steps)

    def dehydrate(self, data):
        """ Dehydrate to PackStream.
        """
        t = type(data)
        if t in self.dehydration_functions:
            f = self.dehydration_functions[t]
            return f(data)
        elif data is None or data is True or data is False or isinstance(data, float) or isinstance(data, string_types):
            return data
        elif isinstance(data, integer_types):
            if data < INT64_LO or data > INT64_HI:
                raise ValueError("Integers must be within the signed 64-bit range")
            return data
        elif isinstance(data, bytearray):
            return data
        elif isinstance(data, Mapping):
            d = {}
            for key in data:
                if not isinstance(key, string_types):
                    raise TypeError("Dictionary keys must be strings")
                d[key] = self.dehydrate(data[key])
            return d
        elif isinstance(data, Sequence):
            return list(map(self.dehydrate, data))
        else:
            raise TypeError("Neo4j does not support PackStream parameters of type %s" % type(data).__name__)


class JSONHydrator(Hydrator):

    unbound_relationship = namedtuple("UnboundRelationship", ["id", "type", "properties"])

    def __init__(self, version, graph, entities=None):
        super(JSONHydrator, self).__init__(graph)
        self.version = version
        assert self.version == "rest"
        self.entities = entities or {}
        self.hydration_functions = {}

    @classmethod
    def json_to_packstream(cls, data):
        """ This converts from JSON format into PackStream prior to
        proper hydration. This code needs to die horribly in a freak
        yachting accident.
        """
        # TODO: other partial hydration
        if "self" in data:
            if "type" in data:
                return Structure(ord(b"R"),
                                 uri_to_id(data["self"]),
                                 uri_to_id(data["start"]),
                                 uri_to_id(data["end"]),
                                 data["type"],
                                 data["data"])
            else:
                return Structure(ord(b"N"),
                                 uri_to_id(data["self"]),
                                 data["metadata"]["labels"],
                                 data["data"])
        elif "nodes" in data and "relationships" in data:
            nodes = [Structure(ord(b"N"), i, None, None) for i in map(uri_to_id, data["nodes"])]
            relps = [Structure(ord(b"r"), i, None, None) for i in map(uri_to_id, data["relationships"])]
            seq = [i // 2 + 1 for i in range(2 * len(data["relationships"]))]
            for i, direction in enumerate(data["directions"]):
                if direction == "<-":
                    seq[2 * i] *= -1
            return Structure(ord(b"P"), nodes, relps, seq)
        else:
            # from warnings import warn
            # warn("Map literals returned over the Neo4j HTTP interface are ambiguous "
            #      "and may be unintentionally hydrated as graph objects")
            return data

    def hydrate(self, keys, values):
        """ Convert JSON values into native values. This is the other half
        of the HTTP hydration process, and is basically a copy of the
        Bolt/PackStream hydration code. It needs to be combined with the
        code in `json_to_packstream` so that hydration is done in a single
        pass.
        """

        graph = self.graph
        entities = self.entities

        def hydrate_object(obj, inst=None):
            from py2neo.data import Path
            if isinstance(obj, Structure):
                tag = obj.tag
                fields = obj.fields
                if tag == ord(b"N"):
                    return self.hydrate_node(inst, fields[0], fields[1], hydrate_object(fields[2]))
                elif tag == ord(b"R"):
                    return self.hydrate_relationship(inst, fields[0],
                                                     fields[1], fields[2],
                                                     fields[3], hydrate_object(fields[4]))
                elif tag == ord(b"P"):
                    # Herein lies a dirty hack to retrieve missing relationship
                    # detail for paths received over HTTP.
                    nodes = [hydrate_object(node) for node in fields[0]]
                    u_rels = []
                    typeless_u_rel_ids = []
                    for r in fields[1]:
                        u_rel = self.unbound_relationship(*map(hydrate_object, r))
                        assert u_rel.type is None
                        typeless_u_rel_ids.append(u_rel.id)
                        u_rels.append(u_rel)
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
                            rel = self.hydrate_relationship(None, u_rel.id,
                                                            last_node.identity, next_node.identity,
                                                            u_rel.type, u_rel.properties)
                        else:
                            u_rel = u_rels[-rel_index - 1]
                            rel = self.hydrate_relationship(None, u_rel.id,
                                                            next_node.identity, last_node.identity,
                                                            u_rel.type, u_rel.properties)
                        steps.append(rel)
                        # steps.append(next_node)
                        last_node = next_node
                    return Path(*steps)
                else:
                    try:
                        f = self.hydration_functions[tag]
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

        return tuple(hydrate_object(value, entities.get(keys[i])) for i, value in enumerate(values))

    def dehydrate(self, data):
        """ Dehydrate to JSON.
        """
        if data is None or data is True or data is False or isinstance(data, float) or isinstance(data, string_types):
            return data
        elif isinstance(data, integer_types):
            if data < INT64_LO or data > INT64_HI:
                raise ValueError("Integers must be within the signed 64-bit range")
            return data
        elif isinstance(data, bytearray):
            return list(data)
        elif isinstance(data, Mapping):
            d = {}
            for key in data:
                if not isinstance(key, string_types):
                    raise TypeError("Dictionary keys must be strings")
                d[key] = self.dehydrate(data[key])
            return d
        elif isinstance(data, Sequence):
            return list(map(self.dehydrate, data))
        else:
            raise TypeError("Neo4j does not support JSON parameters of type %s" % type(data).__name__)

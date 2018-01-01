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


from itertools import chain
from uuid import uuid4

from cypy.graph import size
from cypy.graph.store import PropertyDict
from cypy.encoding import LabelSetView, cypher_escape, cypher_repr

from py2neo.caching import ThreadLocalEntityCache
from py2neo.compat import integer, string, ustr
from py2neo.util import is_collection, round_robin, snake_case, relationship_case


def walk(*walkables):
    """ Traverse over the arguments supplied, yielding the entities
    from each in turn.

    :arg walkables: sequence of walkable objects
    """
    if not walkables:
        return
    walkable = walkables[0]
    try:
        entities = walkable.__walk__()
    except AttributeError:
        raise TypeError("Object %r is not walkable" % walkable)
    for entity in entities:
        yield entity
    end_node = walkable.end_node()
    for walkable in walkables[1:]:
        try:
            if end_node == walkable.start_node():
                entities = walkable.__walk__()
                end_node = walkable.end_node()
            elif end_node == walkable.end_node():
                entities = reversed(list(walkable.__walk__()))
                end_node = walkable.start_node()
            else:
                raise ValueError("Cannot append walkable %r "
                                 "to node %r" % (walkable, end_node))
        except AttributeError:
            raise TypeError("Object %r is not walkable" % walkable)
        for i, entity in enumerate(entities):
            if i > 0:
                yield entity


def cast(obj, entities=None):
    if obj is None:
        return None
    elif isinstance(obj, (Node, Relationship, Path)):
        return obj
    elif isinstance(obj, dict):
        return cast_node(obj)
    elif isinstance(obj, tuple):
        return cast_relationship(obj, entities)
    else:
        raise TypeError(obj)


def cast_node(obj):
    if obj is None or isinstance(obj, Node):
        return obj

    def apply(x):
        if isinstance(x, dict):
            inst.update(x)
        elif is_collection(x):
            for item in x:
                apply(item)
        elif isinstance(x, string):
            inst.add_label(ustr(x))
        else:
            raise TypeError("Cannot cast %s to Node" % obj.__class__.__name__)

    inst = Node()
    apply(obj)
    return inst


def cast_relationship(obj, entities=None):

    def get_type(r):
        if isinstance(r, string):
            return r
        elif hasattr(r, "type"):
            return r.type
        elif isinstance(r, tuple) and len(r) == 2 and isinstance(r[0], string):
            return r[0]
        else:
            raise ValueError("Cannot determine relationship type from %r" % r)

    def get_properties(r):
        if isinstance(r, string):
            return {}
        elif hasattr(r, "type"):
            return dict(r)
        elif hasattr(r, "properties"):
            return r.properties
        elif isinstance(r, tuple) and len(r) == 2 and isinstance(r[0], string):
            return dict(r[1])
        else:
            raise ValueError("Cannot determine properties from %r" % r)

    if isinstance(obj, Relationship):
        return obj
    elif isinstance(obj, tuple):
        if len(obj) == 3:
            start_node, t, end_node = obj
            properties = get_properties(t)
        elif len(obj) == 4:
            start_node, t, end_node, properties = obj
            properties = dict(get_properties(t), **properties)
        else:
            raise TypeError("Cannot cast relationship from %r" % obj)
    else:
        raise TypeError("Cannot cast relationship from %r" % obj)

    if entities:
        if isinstance(start_node, integer):
            start_node = entities[start_node]
        if isinstance(end_node, integer):
            end_node = entities[end_node]
    return Relationship(start_node, get_type(t), end_node, **properties)


def remote(obj):
    """ Return the remote counterpart of a local object.

    :param obj: the local object
    :return: the corresponding remote entity
    """
    try:
        return obj.__remote__
    except AttributeError:
        return None


class Subgraph(object):
    """ Arbitrary, unordered collection of nodes and relationships.
    """
    def __init__(self, nodes=None, relationships=None):
        self.__nodes = frozenset(nodes or [])
        self.__relationships = frozenset(relationships or [])
        self.__nodes |= frozenset(chain(*(r.nodes for r in self.__relationships)))
        if not self.__nodes:
            raise ValueError("Subgraphs must contain at least one node")

    # def __repr__(self):
    #     return "Subgraph({" + ", ".join(map(repr, self.nodes)) + "}, {" + ", ".join(map(repr, self.relationships)) + "})"

    def __eq__(self, other):
        try:
            return self.nodes == other.nodes and self.relationships == other.relationships
        except AttributeError:
            return False

    def __ne__(self, other):
        return not self.__eq__(other)

    def __hash__(self):
        value = 0
        for entity in self.__nodes:
            value ^= hash(entity)
        for entity in self.__relationships:
            value ^= hash(entity)
        return value

    def __graph_order__(self):
        """ Total number of unique nodes.
        """
        return len(self.__nodes)

    def __graph_size__(self):
        """ Total number of unique relationships.
        """
        return len(self.__relationships)

    def __len__(self):
        return len(self.__relationships)

    def __iter__(self):
        return iter(self.__relationships)

    def __bool__(self):
        return bool(self.__relationships)

    def __nonzero__(self):
        return bool(self.__relationships)

    def __or__(self, other):
        return Subgraph(set(self.nodes) | set(other.nodes), set(self.relationships) | set(other.relationships))

    def __and__(self, other):
        return Subgraph(set(self.nodes) & set(other.nodes), set(self.relationships) & set(other.relationships))

    def __sub__(self, other):
        r = set(self.relationships) - set(other.relationships)
        n = (set(self.nodes) - set(other.nodes)) | set().union(*(set(rel.nodes) for rel in r))
        return Subgraph(n, r)

    def __xor__(self, other):
        r = set(self.relationships) ^ set(other.relationships)
        n = (set(self.nodes) ^ set(other.nodes)) | set().union(*(set(rel.nodes) for rel in r))
        return Subgraph(n, r)

    def __db_create__(self, tx):
        nodes = list(self.nodes)
        reads = []
        writes = []
        parameters = {}
        returns = {}
        for i, node in enumerate(nodes):
            node_id = "a%d" % i
            param_id = "x%d" % i
            remote_node = remote(node)
            if remote_node:
                reads.append("MATCH (%s) WHERE id(%s)={%s}" % (node_id, node_id, param_id))
                parameters[param_id] = remote_node._id
            else:
                label_string = "".join(":" + cypher_escape(label)
                                       for label in sorted(node.labels))
                writes.append("CREATE (%s%s {%s})" % (node_id, label_string, param_id))
                parameters[param_id] = dict(node)
                node._set_remote_pending(tx)
            returns[node_id] = node
        for i, relationship in enumerate(self.relationships):
            if not remote(relationship):
                rel_id = "r%d" % i
                start_node_id = "a%d" % nodes.index(relationship.start_node())
                end_node_id = "a%d" % nodes.index(relationship.end_node())
                type_string = cypher_escape(relationship.type)
                param_id = "y%d" % i
                writes.append("CREATE UNIQUE (%s)-[%s:%s]->(%s) SET %s={%s}" %
                              (start_node_id, rel_id, type_string, end_node_id, rel_id, param_id))
                parameters[param_id] = dict(relationship)
                returns[rel_id] = relationship
                relationship._set_remote_pending(tx)
        statement = "\n".join(reads + writes + ["RETURN %s LIMIT 1" % ", ".join(returns)])
        tx.entities.append(returns)
        list(tx.run(statement, parameters))

    def __db_degree__(self, tx):
        node_ids = []
        for i, node in enumerate(self.nodes):
            remote_node = remote(node)
            if remote_node:
                node_ids.append(remote_node._id)
        statement = "OPTIONAL MATCH (a)-[r]-() WHERE id(a) IN {x} RETURN count(DISTINCT r)"
        parameters = {"x": node_ids}
        return tx.evaluate(statement, parameters)

    def __db_delete__(self, tx):
        node_ids = set()
        relationship_ids = set()
        for i, node in enumerate(self.nodes):
            remote_node = remote(node)
            if remote_node:
                node_ids.add(remote_node._id)
            else:
                return False
        for i, relationship in enumerate(self.relationships):
            remote_relationship = remote(relationship)
            if remote_relationship:
                relationship_ids.add(remote_relationship._id)
            else:
                return False
        statement = ("OPTIONAL MATCH (a) WHERE id(a) IN {x} "
                     "OPTIONAL MATCH ()-[r]->() WHERE id(r) IN {y} "
                     "DELETE r, a")
        parameters = {"x": list(node_ids), "y": list(relationship_ids)}
        list(tx.run(statement, parameters))

    def __db_exists__(self, tx):
        node_ids = set()
        relationship_ids = set()
        for i, node in enumerate(self.nodes):
            remote_node = remote(node)
            if remote_node:
                node_ids.add(remote_node._id)
            else:
                return False
        for i, relationship in enumerate(self.relationships):
            remote_relationship = remote(relationship)
            if remote_relationship:
                relationship_ids.add(remote_relationship._id)
            else:
                return False
        statement = ("OPTIONAL MATCH (a) WHERE id(a) IN {x} "
                     "OPTIONAL MATCH ()-[r]->() WHERE id(r) IN {y} "
                     "RETURN count(DISTINCT a) + count(DISTINCT r)")
        parameters = {"x": list(node_ids), "y": list(relationship_ids)}
        return tx.evaluate(statement, parameters) == len(node_ids) + len(relationship_ids)

    def __db_merge__(self, tx, primary_label=None, primary_key=None):
        nodes = list(self.nodes)
        match_clauses = []
        merge_clauses = []
        parameters = {}
        returns = {}
        for i, node in enumerate(nodes):
            node_id = "a%d" % i
            param_id = "x%d" % i
            remote_node = remote(node)
            if remote_node:
                match_clauses.append("MATCH (%s) WHERE id(%s)={%s}" % (node_id, node_id, param_id))
                parameters[param_id] = remote_node._id
            else:
                merge_label = getattr(node, "__primarylabel__", None) or primary_label
                if merge_label is None:
                    label_string = "".join(":" + cypher_escape(label)
                                           for label in sorted(node.labels))
                elif node.labels:
                    label_string = ":" + cypher_escape(merge_label)
                else:
                    label_string = ""
                merge_keys = getattr(node, "__primarykey__", None) or primary_key
                if merge_keys is None:
                    merge_keys = ()
                elif is_collection(merge_keys):
                    merge_keys = tuple(merge_keys)
                else:
                    merge_keys = (merge_keys,)
                if merge_keys:
                    property_map_string = cypher_repr({k: v for k, v in dict(node).items()
                                                       if k in merge_keys})
                else:
                    property_map_string = cypher_repr(dict(node))
                merge_clauses.append("MERGE (%s%s %s)" % (node_id, label_string, property_map_string))
                if node.labels:
                    merge_clauses.append("SET %s%s" % (
                        node_id, "".join(":" + cypher_escape(label)
                                         for label in sorted(node.labels))))
                if merge_keys:
                    merge_clauses.append("SET %s={%s}" % (node_id, param_id))
                    parameters[param_id] = dict(node)
                node._set_remote_pending(tx)
            returns[node_id] = node
        clauses = match_clauses + merge_clauses
        for i, relationship in enumerate(self.relationships):
            if not remote(relationship):
                rel_id = "r%d" % i
                start_node_id = "a%d" % nodes.index(relationship.start_node())
                end_node_id = "a%d" % nodes.index(relationship.end_node())
                type_string = cypher_escape(relationship.type)
                param_id = "y%d" % i
                clauses.append("MERGE (%s)-[%s:%s]->(%s) SET %s={%s}" %
                               (start_node_id, rel_id, type_string, end_node_id, rel_id, param_id))
                parameters[param_id] = dict(relationship)
                returns[rel_id] = relationship
                relationship._set_remote_pending(tx)
        statement = "\n".join(clauses + ["RETURN %s LIMIT 1" % ", ".join(returns)])
        tx.entities.append(returns)
        list(tx.run(statement, parameters))

    def __db_pull__(self, tx):
        nodes = {node: None for node in self.nodes}
        relationships = list(self.relationships)
        for node in nodes:
            tx.entities.append({"_": node})
            cursor = tx.run("MATCH (_) WHERE id(_) = {x} RETURN _, labels(_)", x=remote(node)._id)
            nodes[node] = cursor
        for relationship in relationships:
            tx.entities.append({"_": relationship})
            list(tx.run("MATCH ()-[_]->() WHERE id(_) = {x} RETURN _", x=remote(relationship)._id))
        for node, cursor in nodes.items():
            new_labels = cursor.evaluate(1)
            if new_labels:
                node._Node__remote_labels = frozenset(new_labels)
                labels = node._Node__labels
                labels.clear()
                labels.update(new_labels)

    def __db_push__(self, tx):
        # TODO: reimplement this when REMOVE a:* is available in Cypher
        for node in self.nodes:
            remote_node = remote(node)
            if remote_node:
                clauses = ["MATCH (_) WHERE id(_) = {x}", "SET _ = {y}"]
                parameters = {"x": remote_node._id, "y": dict(node)}
                old_labels = node._Node__remote_labels - node._Node__labels
                if old_labels:
                    clauses.append("REMOVE _:%s" % ":".join(map(cypher_escape, old_labels)))
                new_labels = node._Node__labels - node._Node__remote_labels
                if new_labels:
                    clauses.append("SET _:%s" % ":".join(map(cypher_escape, new_labels)))
                tx.run("\n".join(clauses), parameters)
        for relationship in self.relationships:
            remote_relationship = remote(relationship)
            if remote_relationship:
                clauses = ["MATCH ()-[_]->() WHERE id(_) = {x}", "SET _ = {y}"]
                parameters = {"x": remote_relationship._id, "y": dict(relationship)}
                tx.run("\n".join(clauses), parameters)

    def __db_separate__(self, tx):
        matches = []
        deletes = []
        parameters = {}
        for i, relationship in enumerate(self.relationships):
            remote_relationship = remote(relationship)
            if remote_relationship:
                rel_id = "r%d" % i
                param_id = "y%d" % i
                matches.append("MATCH ()-[%s]->() "
                               "WHERE id(%s)={%s}" % (rel_id, rel_id, param_id))
                deletes.append("DELETE %s" % rel_id)
                parameters[param_id] = remote_relationship._id
                del relationship.__remote__
        statement = "\n".join(matches + deletes)
        list(tx.run(statement, parameters))

    @property
    def nodes(self):
        """ Set of all nodes.
        """
        return self.__nodes

    @property
    def relationships(self):
        """ Set of all relationships.
        """
        return self.__relationships

    @property
    def labels(self):
        """ Set of all node labels.
        """
        return frozenset(chain(*(node.labels for node in self.__nodes)))

    def types(self):
        """ Set of all relationship types.
        """
        return frozenset(rel.type for rel in self.__relationships)

    def keys(self):
        """ Set of all property keys.
        """
        return (frozenset(chain(*(node.keys() for node in self.__nodes))) |
                frozenset(chain(*(rel.keys() for rel in self.__relationships))))


class Walkable(Subgraph):
    """ A subgraph with added traversal information.
    """

    def __init__(self, iterable):
        self.__sequence = tuple(iterable)
        Subgraph.__init__(self, self.__sequence[0::2], self.__sequence[1::2])

    # def __repr__(self):
    #     from py2neo.compat import unicode_repr
    #     return unicode_repr(cypher_repr(self))

    def __eq__(self, other):
        try:
            other_walk = tuple(walk(other))
        except TypeError:
            return False
        else:
            return tuple(walk(self)) == other_walk

    def __ne__(self, other):
        return not self.__eq__(other)

    def __hash__(self):
        value = 0
        for item in self.__sequence:
            value ^= hash(item)
        return value

    def __len__(self):
        return (len(self.__sequence) - 1) // 2

    def __getitem__(self, index):
        if isinstance(index, slice):
            start, stop = index.start, index.stop
            if start is not None:
                if start < 0:
                    start += len(self)
                start *= 2
            if stop is not None:
                if stop < 0:
                    stop += len(self)
                stop = 2 * stop + 1
            return Walkable(self.__sequence[start:stop])
        elif index < 0:
            return self.__sequence[2 * index]
        else:
            return self.__sequence[2 * index + 1]

    def __iter__(self):
        for relationship in self.__sequence[1::2]:
            yield relationship

    def __add__(self, other):
        if other is None:
            return self
        return Walkable(walk(self, other))

    def __walk__(self):
        """ Traverse and yield all nodes and relationships in this
        object in order.
        """
        return iter(self.__sequence)

    def start_node(self):
        """ The first node encountered on a :func:`.walk` of this object.
        """
        return self.__sequence[0]

    def end_node(self):
        """ The last node encountered on a :func:`.walk` of this object.
        """
        return self.__sequence[-1]

    @property
    def nodes(self):
        """ The sequence of nodes over which a :func:`.walk` of this
        object will traverse.
        """
        return self.__sequence[0::2]

    @property
    def relationships(self):
        """ The sequence of relationships over which a :func:`.walk`
        of this object will traverse.
        """
        return self.__sequence[1::2]


class Entity(PropertyDict, Walkable):
    """ Base class for objects that can be optionally bound to a remote resource. This
    class is essentially a container for a :class:`.Resource` instance.
    """

    __remote = None
    __remote_pending_tx = None

    def __init__(self, iterable, properties):
        Walkable.__init__(self, iterable)
        PropertyDict.__init__(self, properties)
        uuid = str(uuid4())
        while "0" <= uuid[-7] <= "9":
            uuid = str(uuid4())
        self.__uuid__ = uuid
        if "__name__" in properties:
            self.__name__ = properties["__name__"]
        elif "name" in properties:
            self.__name__ = snake_case(properties["name"])
        else:
            self.__name__ = self.__uuid__[-7:]

    # def __repr__(self):
    #     return Walkable.__repr__(self)

    def __bool__(self):
        return len(self) > 0

    def __nonzero__(self):
        return len(self) > 0

    def _set_remote_pending(self, tx):
        self.__remote_pending_tx = tx

    @property
    def __remote__(self):
        """ Remote resource with which this entity is associated.
        """
        if self.__remote_pending_tx:
            self.__remote_pending_tx.process()
            self.__remote_pending_tx = None
        return self.__remote

    @__remote__.setter
    def __remote__(self, value):
        self.__remote = value
        self.__remote_pending_tx = None

    @__remote__.deleter
    def __remote__(self):
        cache = getattr(self, "cache", None)
        if cache:
            uri = remote(self).uri
            if uri in cache:
                cache.update(remote(self).uri, None)
        self.__remote = None
        self.__remote_pending_tx = None


class Node(Entity):
    """ A node is a fundamental unit of data storage within a property
    graph that may optionally be connected, via relationships, to
    other nodes.

    All positional arguments passed to the constructor are interpreted
    as labels and all keyword arguments as properties::

        >>> from py2neo import Node
        >>> a = Node("Person", name="Alice")

    """

    cache = ThreadLocalEntityCache()

    @classmethod
    def instance(cls, uri, inst=None):
        if inst is None:

            def inst_constructor():
                new_inst = cls()
                new_inst.__stale.update({"labels", "properties"})
                return new_inst

            inst = cls.cache.update(uri, inst_constructor)
        else:
            cls.cache.update(uri, inst)
        return inst

    @classmethod
    def hydrate(cls, uri, inst=None, **rest):
        from py2neo.http import Remote
        inst = cls.instance(uri, inst)
        inst.__remote__ = Remote(uri)
        if "data" in rest:
            inst.__stale.discard("properties")
            inst.clear()
            inst.update(rest["data"])
        if "metadata" in rest:
            inst.__stale.discard("labels")
            metadata = rest["metadata"]
            inst.__remote_labels = frozenset(metadata["labels"])
            inst.clear_labels()
            inst.update_labels(metadata["labels"])
        return inst

    def __init__(self, *labels, **properties):
        self.__remote_labels = frozenset()
        self.__labels = set(labels)
        Entity.__init__(self, (self,), properties)
        self.__stale = set()

    def __eq__(self, other):
        if self.__class__ is not other.__class__:
            return False
        remote_self = remote(self)
        remote_other = remote(other)
        if remote_self and remote_other:
            return remote_self == remote_other
        else:
            return self is other

    def __ne__(self, other):
        return not self.__eq__(other)

    def __hash__(self):
        remote_self = remote(self)
        if remote_self:
            return hash(remote_self.uri)
        else:
            return hash(id(self))

    def __getitem__(self, item):
        remote_self = remote(self)
        if remote_self and "properties" in self.__stale:
            remote_self.graph.pull(self)
        return Entity.__getitem__(self, item)

    def __ensure_labels(self):
        remote_self = remote(self)
        if remote_self and "labels" in self.__stale:
            remote_self.graph.pull(self)

    @property
    def labels(self):
        """ Set of all node labels.
        """
        self.__ensure_labels()
        return LabelSetView(self.__labels)

    def has_label(self, label):
        self.__ensure_labels()
        return label in self.__labels

    def add_label(self, label):
        self.__ensure_labels()
        self.__labels.add(label)

    def remove_label(self, label):
        self.__ensure_labels()
        self.__labels.discard(label)

    def clear_labels(self):
        self.__ensure_labels()
        self.__labels.clear()

    def update_labels(self, labels):
        self.__ensure_labels()
        self.__labels.update(labels)


class Relationship(Entity):
    """ A relationship represents a typed connection between a pair of nodes.

    The positional arguments passed to the constructor identify the nodes to
    relate and the type of the relationship. Keyword arguments describe the
    properties of the relationship::

        >>> from py2neo import Node, Relationship
        >>> a = Node("Person", name="Alice")
        >>> b = Node("Person", name="Bob")
        >>> a_knows_b = Relationship(a, "KNOWS", b, since=1999)

    This class may be extended to allow relationship types names to be
    derived from the class name. For example::

        >>> class WorksWith(Relationship): pass
        >>> a_works_with_b = WorksWith(a, b)
        >>> a_works_with_b.type
        'WORKS_WITH'

    """

    cache = ThreadLocalEntityCache()

    @classmethod
    def default_type(cls):
        if cls is Relationship:
            return "TO"
        assert issubclass(cls, Relationship)
        return ustr(relationship_case(cls.__name__))

    @classmethod
    def hydrate(cls, uri, inst=None, **rest):
        from py2neo.http import Remote
        start = rest["start"]
        end = rest["end"]

        if inst is None:

            def inst_constructor():
                return cls(Node.hydrate(start), rest.get("type"),
                           Node.hydrate(end), **rest.get("data", {}))

            inst = cls.cache.update(uri, inst_constructor)
        else:
            Node.hydrate(start, inst=inst.start_node())
            Node.hydrate(end, inst=inst.end_node())
            inst.__type = rest.get("type")
            if "data" in rest:
                inst.clear()
                inst.update(rest["data"])
            else:
                inst.__stale.add("properties")
            cls.cache.update(uri, inst)
        inst.__remote__ = Remote(uri)
        return inst

    def __init__(self, *nodes, **properties):
        n = []
        p = {}
        for value in nodes:
            if isinstance(value, string):
                n.append(value)
            elif isinstance(value, tuple) and len(value) == 2 and isinstance(value[0], string):
                t, props = value
                n.append(t)
                p.update(props)
            else:
                n.append(cast_node(value))
        p.update(properties)

        num_args = len(n)
        if num_args == 0:
            raise TypeError("Relationships must specify at least one endpoint")
        elif num_args == 1:
            # Relationship(a)
            self.__type = self.default_type()
            n = (n[0], n[0])
        elif num_args == 2:
            if n[1] is None or isinstance(n[1], string):
                # Relationship(a, "TO")
                self.__type = n[1]
                n = (n[0], n[0])
            else:
                # Relationship(a, b)
                self.__type = self.default_type()
                n = (n[0], n[1])
        elif num_args == 3:
            # Relationship(a, "TO", b)
            self.__type = n[1]
            n = (n[0], n[2])
        else:
            raise TypeError("Hyperedges not supported")
        Entity.__init__(self, (n[0], self, n[1]), p)

        self.__stale = set()

    def __eq__(self, other):
        if other is None:
            return False
        try:
            other = cast_relationship(other)
        except TypeError:
            return False
        else:
            remote_self = remote(self)
            remote_other = remote(other)
            if remote_self and remote_other:
                return remote_self == remote_other
            else:
                return (self.nodes == other.nodes and size(other) == 1 and
                        self.type == other.type and dict(self) == dict(other))

    def __ne__(self, other):
        return not self.__eq__(other)

    def __hash__(self):
        return hash(self.nodes) ^ hash(self.type)

    @property
    def type(self):
        """ The type of this relationship.
        """
        remote_self = remote(self)
        if remote_self and self.__type is None:
            remote_self.graph.pull(self)
        return self.__type


class Path(Walkable):
    """ A sequence of nodes connected by relationships that may
    optionally be bound to remote counterparts in a Neo4j database.

        >>> from py2neo import Node, Path
        >>> alice, bob, carol = Node(name="Alice"), Node(name="Bob"), Node(name="Carol")
        >>> abc = Path(alice, "KNOWS", bob, Relationship(carol, "KNOWS", bob), carol)
        >>> abc
        <Path order=3 size=2>
        >>> abc.nodes
        (<Node labels=set() properties={'name': 'Alice'}>,
         <Node labels=set() properties={'name': 'Bob'}>,
         <Node labels=set() properties={'name': 'Carol'}>)
        >>> abc.relationships
        (<Relationship type='KNOWS' properties={}>,
         <Relationship type='KNOWS' properties={}>)
        >>> dave, eve = Node(name="Dave"), Node(name="Eve")
        >>> de = Path(dave, "KNOWS", eve)
        >>> de
        <Path order=2 size=1>
        >>> abcde = Path(abc, "KNOWS", de)
        >>> abcde
        <Path order=5 size=4>
        >>> for relationship in abcde.relationships:
        ...     print(relationship)
        ({name:"Alice"})-[:KNOWS]->({name:"Bob"})
        ({name:"Carol"})-[:KNOWS]->({name:"Bob"})
        ({name:"Carol"})-[:KNOWS]->({name:"Dave"})
        ({name:"Dave"})-[:KNOWS]->({name:"Eve"})

    """
    @classmethod
    def hydrate(cls, data):
        node_uris = data["nodes"]
        relationship_uris = data["relationships"]
        offsets = [(0, 1) if direction == "->" else (1, 0) for direction in data["directions"]]
        nodes = [Node.hydrate(uri) for uri in node_uris]
        relationships = [Relationship.hydrate(uri,
                                              start=node_uris[i + offsets[i][0]],
                                              end=node_uris[i + offsets[i][1]])
                         for i, uri in enumerate(relationship_uris)]
        inst = Path(*round_robin(nodes, relationships))
        inst.__metadata = data
        return inst

    def __init__(self, *entities):
        entities = list(entities)
        for i, entity in enumerate(entities):
            if isinstance(entity, Entity):
                continue
            elif entity is None:
                entities[i] = Node()
            elif isinstance(entity, dict):
                entities[i] = Node(**entity)
        for i, entity in enumerate(entities):
            try:
                start_node = entities[i - 1].end_node()
                end_node = entities[i + 1].start_node()
            except (IndexError, AttributeError):
                pass
            else:
                if isinstance(entity, string):
                    entities[i] = Relationship(start_node, entity, end_node)
                elif isinstance(entity, tuple) and len(entity) == 2:
                    t, properties = entity
                    entities[i] = Relationship(start_node, t, end_node, **properties)
        Walkable.__init__(self, walk(*entities))

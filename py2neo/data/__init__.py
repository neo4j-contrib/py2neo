#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright 2011-2021, Nigel Small
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

from collections import OrderedDict
from itertools import chain
from uuid import uuid4

from py2neo.collections import is_collection, SetView, PropertyDict
from py2neo.compat import integer_types, string_types, ustr, xstr
from py2neo.cypher import cypher_repr
from py2neo.cypher.encoding import CypherEncoder, LabelSetView
from py2neo.data.operations import (
    create_subgraph,
    merge_subgraph,
    delete_subgraph,
    separate_subgraph,
    pull_subgraph,
    push_subgraph,
    subgraph_exists,
)


class Subgraph(object):
    """ A :class:`.Subgraph` is an arbitrary collection of nodes and
    relationships. It is also the base class for :class:`.Node`,
    :class:`.Relationship` and :class:`.Path`.

    By definition, a subgraph must contain at least one node;
    `null subgraphs <http://mathworld.wolfram.com/NullGraph.html>`_
    should be represented by :const:`None`. To test for
    `emptiness <http://mathworld.wolfram.com/EmptyGraph.html>`_ the
    built-in :func:`bool` function can be used.

    The simplest way to construct a subgraph is by combining nodes and
    relationships using standard set operations. For example::

        >>> s = ab | ac
        >>> s
        {(alice:Person {name:"Alice"}),
         (bob:Person {name:"Bob"}),
         (carol:Person {name:"Carol"}),
         (Alice)-[:KNOWS]->(Bob),
         (Alice)-[:WORKS_WITH]->(Carol)}
        >>> s.nodes()
        frozenset({(alice:Person {name:"Alice"}),
                   (bob:Person {name:"Bob"}),
                   (carol:Person {name:"Carol"})})
        >>> s.relationships()
        frozenset({(Alice)-[:KNOWS]->(Bob),
                   (Alice)-[:WORKS_WITH]->(Carol)})

    .. describe:: subgraph | other | ...

        Union.
        Return a new subgraph containing all nodes and relationships from *subgraph* as well as all those from *other*.
        Any entities common to both will only be included once.

    .. describe:: subgraph & other & ...

        Intersection.
        Return a new subgraph containing all nodes and relationships common to both *subgraph* and *other*.

    .. describe:: subgraph - other - ...

        Difference.
        Return a new subgraph containing all nodes and relationships that exist in *subgraph* but do not exist in *other*,
        as well as all nodes that are connected by the relationships in *subgraph* regardless of whether or not they exist in *other*.

    .. describe:: subgraph ^ other ^ ...

        Symmetric difference.
        Return a new subgraph containing all nodes and relationships that exist in *subgraph* or *other*, but not in both,
        as well as all nodes that are connected by those relationships regardless of whether or not they are common to *subgraph* and *other*.

    """

    def __init__(self, nodes=None, relationships=None):
        self.__nodes = frozenset(nodes or [])
        self.__relationships = frozenset(relationships or [])
        self.__nodes |= frozenset(chain(*(r.nodes for r in self.__relationships)))
        if not self.__nodes:
            raise ValueError("Subgraphs must contain at least one node")

    def __repr__(self):
        return "Subgraph({%s}, {%s})" % (", ".join(map(repr, self.nodes)),
                                         ", ".join(map(repr, self.relationships)))

    def __eq__(self, other):
        try:
            return self.nodes == other.nodes and self.relationships == other.relationships
        except (AttributeError, TypeError):
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

    def __ior__(self, other):
        if isinstance(self, Walkable):
            self = Subgraph(self.nodes, self.relationships)
        self.__nodes |= other.nodes
        self.__relationships |= other.relationships
        return self

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
        create_subgraph(tx, self)

    def __db_delete__(self, tx):
        delete_subgraph(tx, self)

    def __db_exists__(self, tx):
        return subgraph_exists(tx, self)

    def __db_merge__(self, tx, primary_label=None, primary_key=None):
        merge_subgraph(tx, self, primary_label, primary_key)

    def __db_pull__(self, tx):
        pull_subgraph(tx, self)

    def __db_push__(self, tx):
        push_subgraph(tx, self)

    def __db_separate__(self, tx):
        separate_subgraph(tx, self)

    @property
    def graph(self):
        assert self.__nodes     # assume there is at least one node
        return set(self.__nodes).pop().graph

    @property
    def nodes(self):
        """ The set of all nodes in this subgraph.
        """
        return SetView(self.__nodes)

    @property
    def relationships(self):
        """ The set of all relationships in this subgraph.
        """
        return SetView(self.__relationships)

    def labels(self):
        """ Return the set of all node labels in this subgraph.

        *Changed in version 2020.0: this is now a method rather than a
        property, as in previous versions.*
        """
        return frozenset(chain(*(node.labels for node in self.__nodes)))

    def types(self):
        """ Return the set of all relationship types in this subgraph.
        """
        return frozenset(type(rel).__name__ for rel in self.__relationships)

    def keys(self):
        """ Return the set of all property keys used by the nodes and
        relationships in this subgraph.
        """
        return (frozenset(chain(*(node.keys() for node in self.__nodes))) |
                frozenset(chain(*(rel.keys() for rel in self.__relationships))))


class Walkable(Subgraph):
    """ A subgraph with added traversal information.
    """

    def __init__(self, iterable):
        self.__sequence = tuple(iterable)
        nodes = self.__sequence[0::2]
        for node in nodes:
            _ = node.labels  # ensure not stale
        Subgraph.__init__(self, nodes, self.__sequence[1::2])

    def __repr__(self):
        return "%s(subgraph=%s, sequence=%r)" % (self.__class__.__name__,
                                                 Subgraph.__repr__(self),
                                                 self.__sequence)

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
            return Path(*self.__sequence[start:stop])
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
        return Path(*walk(self, other))

    def __walk__(self):
        """ Traverse and yield all nodes and relationships in this
        object in order.
        """
        return iter(self.__sequence)

    @property
    def start_node(self):
        """ The first node encountered on a :func:`.walk` of this object.
        """
        return self.__sequence[0]

    @property
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

    graph = None
    identity = None

    def __init__(self, iterable, properties):
        Walkable.__init__(self, iterable)
        PropertyDict.__init__(self, properties)
        uuid = str(uuid4())
        while "0" <= uuid[-7] <= "9":
            uuid = str(uuid4())
        self.__uuid__ = uuid

    def __bool__(self):
        return len(self) > 0

    def __nonzero__(self):
        return len(self) > 0

    @property
    def __name__(self):
        name = None
        if name is None and "__name__" in self:
            name = self["__name__"]
        if name is None and "name" in self:
            name = self["name"]
        if name is None and self.identity is not None:
            name = u"_" + ustr(self.identity)
        return name or u""

    def __or__(self, other):
        # Python 3.9 added the | and |= operators to the dict
        # class (PEP584). This broke Entity union operations by
        # picking up the __or__ handler in PropertyDict before
        # the one in Walkable. The hack below forces Entity to
        # use the Walkable implementation.
        return Walkable.__or__(self, other)


class Node(Entity):
    """ A node is a fundamental unit of data storage within a property
    graph that may optionally be connected, via relationships, to
    other nodes.

    Node objects can either be created implicitly, by returning nodes
    in a Cypher query such as ``CREATE (a) RETURN a``, or can be
    created explicitly through the constructor. In the former case, the
    local Node object is *bound* to the remote node in the database; in
    the latter case, the Node object remains unbound until
    :meth:`created <.Transaction.create>` or
    :meth:`merged <.Transaction.merge>` into a Neo4j database.

    It possible to combine nodes (along with relationships and other
    graph data objects) into :class:`.Subgraph` objects using set
    operations. For more details, look at the documentation for the
    :class:`.Subgraph` class.

    All positional arguments passed to the constructor are interpreted
    as labels and all keyword arguments as properties::

        >>> from py2neo import Node
        >>> a = Node("Person", name="Alice")

    """

    @classmethod
    def cast(cls, obj):
        """ Cast an arbitrary object to a :class:`Node`. This method
        takes its best guess on how to interpret the supplied object
        as a :class:`Node`.
        """
        if obj is None or isinstance(obj, Node):
            return obj

        def apply(x):
            if isinstance(x, dict):
                inst.update(x)
            elif is_collection(x):
                for item in x:
                    apply(item)
            elif isinstance(x, string_types):
                inst.add_label(ustr(x))
            else:
                raise TypeError("Cannot cast %s to Node" % obj.__class__.__name__)

        inst = Node()
        apply(obj)
        return inst

    @classmethod
    def hydrate(cls, graph, identity, labels=None, properties=None, into=None):
        """ Hydrate a new or existing Node object from the attributes provided.
        """
        if into is None:

            def instance_constructor():
                new_instance = cls()
                new_instance.graph = graph
                new_instance.identity = identity
                new_instance._stale.update({"labels", "properties"})
                return new_instance

            into = instance_constructor()
        else:
            assert isinstance(into, cls)
            into.graph = graph
            into.identity = identity

        if properties is not None:
            into._stale.discard("properties")
            into.clear()
            into.update(properties)

        if labels is not None:
            into._stale.discard("labels")
            into._remote_labels = frozenset(labels)
            into.clear_labels()
            into.update_labels(labels)

        return into

    def __init__(self, *labels, **properties):
        self._remote_labels = frozenset()
        self._labels = set(labels)
        Entity.__init__(self, (self,), properties)
        self._stale = set()

    def __repr__(self):
        args = list(map(repr, sorted(self.labels)))
        kwargs = OrderedDict()
        d = dict(self)
        for key in sorted(d):
            if CypherEncoder.is_safe_key(key):
                args.append("%s=%r" % (key, d[key]))
            else:
                kwargs[key] = d[key]
        if kwargs:
            args.append("**{%s}" % ", ".join("%r: %r" % (k, kwargs[k]) for k in kwargs))
        return "Node(%s)" % ", ".join(args)

    def __str__(self):
        return xstr(cypher_repr(self))

    def __eq__(self, other):
        if self is other:
            return True
        try:
            if any(x is None for x in [self.graph, other.graph, self.identity, other.identity]):
                return False
            return issubclass(type(self), Node) and issubclass(type(other), Node) and self.graph == other.graph and self.identity == other.identity
        except (AttributeError, TypeError):
            return False

    def __ne__(self, other):
        return not self.__eq__(other)

    def __hash__(self):
        if self.graph and self.identity:
            return hash(self.graph.service) ^ hash(self.graph.name) ^ hash(self.identity)
        else:
            return hash(id(self))

    def __getitem__(self, item):
        if self.graph is not None and self.identity is not None and "properties" in self._stale:
            self.graph.pull(self)
        return Entity.__getitem__(self, item)

    def __ensure_labels(self):
        if self.graph is not None and self.identity is not None and "labels" in self._stale:
            self.graph.pull(self)

    def keys(self):
        if self.graph is not None and self.identity is not None and "properties" in self._stale:
            self.graph.pull(self)
        return Entity.keys(self)

    @property
    def labels(self):
        """ The full set of labels associated with with this *node*.

        This set is immutable and cannot be used to add or remove
        labels. Use methods such as :meth:`.add_label` and
        :meth:`.remove_label` for that instead.
        """
        self.__ensure_labels()
        return LabelSetView(self._labels)

    def has_label(self, label):
        """ Return :const:`True` if this node has the label `label`,
        :const:`False` otherwise.
        """
        self.__ensure_labels()
        return label in self._labels

    def add_label(self, label):
        """ Add the label `label` to this node.
        """
        self.__ensure_labels()
        self._labels.add(label)

    def remove_label(self, label):
        """ Remove the label `label` from this node, if it exists.
        """
        self.__ensure_labels()
        self._labels.discard(label)

    def clear_labels(self):
        """ Remove all labels from this node.
        """
        self.__ensure_labels()
        self._labels.clear()

    def update_labels(self, labels):
        """ Add multiple labels to this node from the iterable
        `labels`.
        """
        self.__ensure_labels()
        self._labels.update(labels)


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

        >>> WORKS_WITH = Relationship.type("WORKS_WITH")
        >>> a_works_with_b = WORKS_WITH(a, b)
        >>> a_works_with_b
        (Alice)-[:WORKS_WITH {}]->(Bob)

    """

    @staticmethod
    def type(name):
        """ Return the :class:`.Relationship` subclass corresponding to a
        given name.

        :param name: relationship type name
        :returns: `type` object

        Example::

            >>> KNOWS = Relationship.type("KNOWS")
            >>> KNOWS(a, b)
            KNOWS(Node('Person', name='Alice'), Node('Person', name='Bob')

        """
        for s in Relationship.__subclasses__():
            if s.__name__ == name:
                return s
        return type(xstr(name), (Relationship,), {})

    @classmethod
    def cast(cls, obj, entities=None):

        def get_type(r):
            if isinstance(r, string_types):
                return r
            elif isinstance(r, Relationship):
                return type(r).__name__
            elif isinstance(r, tuple) and len(r) == 2 and isinstance(r[0], string_types):
                return r[0]
            else:
                raise ValueError("Cannot determine relationship type from %r" % r)

        def get_properties(r):
            if isinstance(r, string_types):
                return {}
            elif isinstance(r, Relationship):
                return dict(r)
            elif hasattr(r, "properties"):
                return r.properties
            elif isinstance(r, tuple) and len(r) == 2 and isinstance(r[0], string_types):
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
            if isinstance(start_node, integer_types):
                start_node = entities[start_node]
            if isinstance(end_node, integer_types):
                end_node = entities[end_node]
        return Relationship(start_node, get_type(t), end_node, **properties)

    @classmethod
    def hydrate(cls, graph, identity, start, end, type=None, properties=None, into=None):
        if into is None:

            def instance_constructor():
                if properties is None:
                    new_instance = cls(Node.hydrate(graph, start), type,
                                       Node.hydrate(graph, end))
                    new_instance._stale.add("properties")
                else:
                    new_instance = cls(Node.hydrate(graph, start), type,
                                       Node.hydrate(graph, end), **properties)
                new_instance.graph = graph
                new_instance.identity = identity
                return new_instance

            into = instance_constructor()
        else:
            assert isinstance(into, Relationship)
            into.graph = graph
            into.identity = identity
            Node.hydrate(graph, start, into=into.start_node)
            Node.hydrate(graph, end, into=into.end_node)
            into._type = type
            if properties is None:
                into._stale.add("properties")
            else:
                into.clear()
                into.update(properties)
        return into

    def __init__(self, *nodes, **properties):
        n = []
        for value in nodes:
            if value is None:
                n.append(None)
            elif isinstance(value, string_types):
                n.append(value)
            else:
                n.append(Node.cast(value))

        num_args = len(n)
        if num_args == 0:
            raise TypeError("Relationships must specify at least one endpoint")
        elif num_args == 1:
            # Relationship(a)
            n = (n[0], n[0])
        elif num_args == 2:
            if n[1] is None or isinstance(n[1], string_types):
                # Relationship(a, "TO")
                self.__class__ = Relationship.type(n[1])
                n = (n[0], n[0])
            else:
                # Relationship(a, b)
                n = (n[0], n[1])
        elif num_args == 3:
            # Relationship(a, "TO", b)
            self.__class__ = Relationship.type(n[1])
            n = (n[0], n[2])
        else:
            raise TypeError("Hyperedges not supported")
        Entity.__init__(self, (n[0], self, n[1]), properties)

        self._stale = set()

    def __repr__(self):
        args = [repr(self.nodes[0]), repr(self.nodes[-1])]
        kwargs = OrderedDict()
        d = dict(self)
        for key in sorted(d):
            if CypherEncoder.is_safe_key(key):
                args.append("%s=%r" % (key, d[key]))
            else:
                kwargs[key] = d[key]
        if kwargs:
            args.append("**{%s}" % ", ".join("%r: %r" % (k, kwargs[k]) for k in kwargs))
        return "%s(%s)" % (self.__class__.__name__, ", ".join(args))

    def __str__(self):
        return xstr(cypher_repr(self))

    def __eq__(self, other):
        if self is other:
            return True
        try:
            if any(x is None for x in [self.graph, other.graph, self.identity, other.identity]):
                try:
                    return type(self) is type(other) and list(self.nodes) == list(other.nodes) and dict(self) == dict(other)
                except (AttributeError, TypeError):
                    return False
            return issubclass(type(self), Relationship) and issubclass(type(other), Relationship) and self.graph == other.graph and self.identity == other.identity
        except (AttributeError, TypeError):
            return False

    def __ne__(self, other):
        return not self.__eq__(other)

    def __hash__(self):
        return hash(self.nodes) ^ hash(type(self))


class Path(Walkable):
    """ A path represents a walk through a graph, starting on a node
    and visiting alternating relationships and nodes thereafter.
    Paths have a "overlaid" direction separate to that of the
    relationships they contain, and the nodes and relationships
    themselves may each be visited multiple times, in any order,
    within the same path.

    Paths can be returned from Cypher queries or can be constructed
    locally via the constructor or by using the addition operator.

    The `entities` provided to the constructor are walked in order to
    build up the new path. This is only possible if the end node of
    each entity is the same as either the start node or the end node
    of the next entity; in the latter case, the second entity will be
    walked in reverse. Nodes that overlap from one argument onto
    another are not duplicated.

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
    def hydrate(cls, graph, nodes, u_rels, sequence):
        last_node = nodes[0]
        steps = [last_node]
        for i, rel_index in enumerate(sequence[::2]):
            next_node = nodes[sequence[2 * i + 1]]
            if rel_index > 0:
                u_rel = u_rels[rel_index - 1]
                rel = Relationship.hydrate(graph, u_rel.id,
                                           last_node.identity, next_node.identity,
                                           u_rel.type, u_rel.properties)
            else:
                u_rel = u_rels[-rel_index - 1]
                rel = Relationship.hydrate(graph, u_rel.id,
                                           next_node.identity, last_node.identity,
                                           u_rel.type, u_rel.properties)
            steps.append(rel)
            last_node = next_node
        return cls(*steps)

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
                start_node = entities[i - 1].end_node
                end_node = entities[i + 1].start_node
            except (IndexError, AttributeError):
                pass
            else:
                if isinstance(entity, string_types):
                    entities[i] = Relationship(start_node, entity, end_node)
                elif isinstance(entity, tuple) and len(entity) == 2:
                    t, properties = entity
                    entities[i] = Relationship(start_node, t, end_node, **properties)
        Walkable.__init__(self, walk(*entities))

    def __str__(self):
        return xstr(cypher_repr(self))

    def __repr__(self):
        entities = [self.start_node] + list(self.relationships)
        return "Path(%s)" % ", ".join(map(repr, entities))

    @staticmethod
    def walk(*walkables):
        """ Traverse over the arguments supplied, in order, yielding
        alternating :class:`.Node` and :class:`.Relationship` objects.
        Any node or relationship may be traversed one or more times in
        any direction.

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
        end_node = walkable.end_node
        for walkable in walkables[1:]:
            try:
                if end_node == walkable.start_node:
                    entities = walkable.__walk__()
                    end_node = walkable.end_node
                elif end_node == walkable.end_node:
                    entities = reversed(list(walkable.__walk__()))
                    end_node = walkable.start_node
                else:
                    raise ValueError("Cannot append walkable %r "
                                     "to node %r" % (walkable, end_node))
            except AttributeError:
                raise TypeError("Object %r is not walkable" % walkable)
            for i, entity in enumerate(entities):
                if i > 0:
                    yield entity


walk = Path.walk



from py2neo import Node, Relationship


KNOWS = Relationship.type("KNOWS")


def test_single_node_creation(graph):
    a = Node("Person", name="Alice")
    assert a.labels == {"Person"}
    assert a["name"] == "Alice"
    graph.create(a)
    assert isinstance(a.identity, int)
    assert graph.exists(a)


def test_relationship_creation(graph):
    a = Node("Person", name="Alice")
    b = Node("Person", name="Bob")
    ab = KNOWS(a, b, since=1999)
    assert type(ab) is KNOWS
    assert ab["since"] == 1999
    assert ab.start_node is a
    assert ab.end_node is b
    graph.create(ab)
    assert isinstance(ab.identity, int)
    assert isinstance(a.identity, int)
    assert isinstance(b.identity, int)
    assert graph.exists(ab)
    assert graph.exists(a)
    assert graph.exists(b)

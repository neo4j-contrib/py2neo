**************************************
``py2neo.matching`` -- Entity matching
**************************************

The ``py2neo.matching`` module provides functionality to match nodes and relationships according to certain criteria.
For each entity type, a ``Matcher`` class and a ``Match`` class are provided.
The ``Matcher`` can be used to perform a basic selection, returning a ``Match`` that can be evaluated or further refined.

A simple node selection may match on label and property name::

        >>> from py2neo import Graph, NodeMatcher
        >>> graph = Graph()
        >>> matcher = NodeMatcher(graph)
        >>> matcher.match("Person", name="Keanu Reeves").first()
        [(_224:Person {born:1964,name:"Keanu Reeves"})]

For a more complex selection, the match can be refined with a _WHERE_ clause, using an underscore character to refer to the matched entity::

        >>> list(matcher.match("Person").where("_.name =~ 'K.*'"))
        [(_57:Person {born: 1957, name: 'Kelly McGillis'}),
         (_80:Person {born: 1958, name: 'Kevin Bacon'}),
         (_83:Person {born: 1962, name: 'Kelly Preston'}),
         (_224:Person {born: 1964, name: 'Keanu Reeves'}),
         (_226:Person {born: 1966, name: 'Kiefer Sutherland'}),
         (_243:Person {born: 1957, name: 'Kevin Pollak'})]

Orders and limits can also be applied::

        >>> list(matcher.match("Person").where("_.name =~ 'K.*'").order_by("_.name").limit(3))
        [(_224:Person {born: 1964, name: 'Keanu Reeves'}),
         (_57:Person {born: 1957, name: 'Kelly McGillis'}),
         (_83:Person {born: 1962, name: 'Kelly Preston'})]

If only a count of matched entities is required, the length of a match can be evaluated::

        >>> len(matcher.match("Person").where("_.name =~ 'K.*'"))
        6

.. autoclass:: py2neo.matching.NodeMatcher
   :members:
   :special-members: __getitem__

.. autoclass:: py2neo.matching.NodeMatch
   :members:
   :special-members: __iter__

.. autoclass:: py2neo.matching.RelationshipMatcher
   :members:
   :special-members: __getitem__

.. autoclass:: py2neo.matching.RelationshipMatch
   :members:
   :special-members: __iter__

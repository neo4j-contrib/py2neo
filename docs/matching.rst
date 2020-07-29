*****************************************************
``py2neo.matching`` -- Node and relationship matching
*****************************************************

.. automodule:: py2neo.matching


Node matching
=============

``NodeMatcher`` objects
-----------------------

.. autoclass:: NodeMatcher(graph)
    :members:

``NodeMatch`` objects
-----------------------

.. autoclass:: py2neo.matching.NodeMatch
   :members:


Relationship matching
=====================

``RelationshipMatcher`` objects
-------------------------------

.. autoclass:: RelationshipMatcher(graph)
    :members:

``RelationshipMatch`` objects
-----------------------------

.. autoclass:: py2neo.matching.RelationshipMatch
   :members:


Applying predicates
===================

Predicates other than basic equality can be applied to a match by using the built-in predicate functions.

For example, to match all nodes with a name that starts with "John", use the ``STARTS_WITH`` function, which corresponds to the similarly named Cypher operator::

    >>> nodes.match("Person", name=STARTS_WITH("John")).all()
    [Node('Person', born=1966, name='John Cusack'),
     Node('Person', born=1950, name='John Patrick Stanley'),
     Node('Person', born=1940, name='John Hurt'),
     Node('Person', born=1960, name='John Goodman'),
     Node('Person', born=1965, name='John C. Reilly')]

The ``ALL`` and ``ANY`` functions can combine several other functions with an AND or OR operation respectively.
The example below matches everyone born between 1964 and 1966 inclusive::

    >>> nodes.match("Person", born=ALL(GE(1964), LE(1966))).all()
    [Node('Person', born=1964, name='Keanu Reeves'),
     Node('Person', born=1965, name='Lana Wachowski'),
     Node('Person', born=1966, name='Kiefer Sutherland'),
     Node('Person', born=1966, name='John Cusack'),
     Node('Person', born=1966, name='Halle Berry'),
     Node('Person', born=1965, name='Tom Tykwer'),
     Node('Person', born=1966, name='Matthew Fox'),
     Node('Person', born=1965, name='John C. Reilly')]

*Changed in 2020.0: the predicate system has been overhauled to provide
a more idiomatic API.*


Null check predicates
---------------------
.. autofunction:: IS_NULL
.. autofunction:: IS_NOT_NULL

Equality predicates
-------------------
.. autofunction:: EQ
.. autofunction:: NE

Ordering predicates
-------------------
.. autofunction:: LT
.. autofunction:: LE
.. autofunction:: GT
.. autofunction:: GE

String predicates
-----------------
.. autofunction:: STARTS_WITH
.. autofunction:: ENDS_WITH
.. autofunction:: CONTAINS
.. autofunction:: LIKE

List predicates
---------------
.. autofunction:: IN

Connectives
-----------
.. autofunction:: AND
.. autofunction:: OR
.. autofunction:: XOR

Custom predicates
-----------------

For predicates that cannot be expressed using one of the built-in functions, raw Cypher expressions can also be inserted into :meth:`.NodeMatch.where` method to refine the selection.
Here, the underscore character can be used to refer to the node being filtered::

    >>> nodes.match("Person").where("_.born % 10 = 0").all()
    [Node('Person', born=1950, name='Ed Harris'),
     Node('Person', born=1960, name='Hugo Weaving'),
     Node('Person', born=1940, name='Al Pacino'),
     Node('Person', born=1970, name='Jay Mohr'),
     Node('Person', born=1970, name='River Phoenix'),
     Node('Person', born=1940, name='James L. Brooks'),
     Node('Person', born=1960, name='Annabella Sciorra'),
     Node('Person', born=1970, name='Ethan Hawke'),
     Node('Person', born=1940, name='James Cromwell'),
     Node('Person', born=1950, name='John Patrick Stanley'),
     Node('Person', born=1970, name='Brooke Langton'),
     Node('Person', born=1930, name='Gene Hackman'),
     Node('Person', born=1950, name='Howard Deutch'),
     Node('Person', born=1930, name='Richard Harris'),
     Node('Person', born=1930, name='Clint Eastwood'),
     Node('Person', born=1940, name='John Hurt'),
     Node('Person', born=1960, name='John Goodman'),
     Node('Person', born=1980, name='Christina Ricci'),
     Node('Person', born=1960, name='Oliver Platt')]


Ordering and limiting
=====================

As with raw Cypher queries, ordering and limiting can also be applied::

    >>> nodes.match("Person").where(name=LIKE("K.*")).order_by("_.name").limit(3).all()
    [Node('Person', born=1964, name='Keanu Reeves'),
     Node('Person', born=1957, name='Kelly McGillis'),
     Node('Person', born=1962, name='Kelly Preston')]

**************************************
``py2neo.matching`` -- Entity matching
**************************************

The ``py2neo.matching`` module provides functionality to match nodes and relationships according to certain criteria.
For each entity type, a ``Matcher`` class and a ``Match`` class are provided.
The ``Matcher`` can be used to perform a basic selection, returning a ``Match`` that can be evaluated or further refined.


Node Matching
=============

A :class:`.NodeMatcher` can be used to locate nodes that fulfil a specific set of criteria.
Typically, a single node can be identified passing a specific label and property key-value pair.
However, any number of labels and any condition supported by the Cypher `WHERE` clause is allowed.

For a simple match by label and property::

        >>> from py2neo import Graph, NodeMatcher
        >>> graph = Graph()
        >>> matcher = NodeMatcher(graph)
        >>> matcher.match("Person", name="Keanu Reeves").first()
        (_224:Person {born:1964,name:"Keanu Reeves"})

Many types of comparisons can be made with operator suffixes. By appending the opeartor suffix you change the operator used in Cypher expression.

        +--------------------------+--------------+-------------+------------------------------------------------------------+
        | Description              | Suffix       | Operator    | Example                                                    |
        +--------------------------+--------------+-------------+------------------------------------------------------------+
        | Explicit Equal           | __exact      | =           | >>>matcher.match("Person", name__exact="Kevin Bacon")         |
        |                          |              |             | MATCH (_:Person) WHERE name = "Kevin Bacon" RETURN _       |
        +--------------------------+--------------+-------------+------------------------------------------------------------+
        | Not Equal                | __not        | <>          | matcher.match("Person", name__not="Rick Astley")           |
        |                          |              |             | MATCH (_:Person) WHERE _.name <> "Rick Astley" RETURN _    |
        +--------------------------+--------------+-------------+------------------------------------------------------------+
        | Greater than             | __gt         | >           | matcher.match("Person", born__gt=1985)                     |
        |                          |              |             | MATCH (_:Person) WHERE _.born > 1985 RETURN _              |
        +--------------------------+--------------+-------------+------------------------------------------------------------+
        | Greater than or equal to | __gte        | >=          | matcher.match("Person", born__gte=1965)                    |
        |                          |              |             | MATCH (_:Person) WHERE _.born >= 1965 RETURN _             |
        +--------------------------+--------------+-------------+------------------------------------------------------------+
        | Less than                | __lt         | <           | matcher.match("Person", born__lt=1965)                     |
        |                          |              |             | MATCH (_:Person) WHERE _.born < 1965 RETURN _              |
        +--------------------------+--------------+-------------+------------------------------------------------------------+
        | Less than or equal to    | __lte        | <=          | matcher.match("Person", born__lte=1965)                    |
        |                          |              |             | MATCH (_:Person) WHERE _.born <= 1965 RETURN _             |
        +--------------------------+--------------+-------------+------------------------------------------------------------+
        | Starts with              | __startswith | STARTS WITH | matcher.match("Person", name__startswith="Kevin")          |
        |                          |              |             | MATCH (_:Person) WHERE _.name STARTS WITH "Kevin" RETURN _ |
        +--------------------------+--------------+-------------+------------------------------------------------------------+
        | Ends with                | __endswith   | ENDS WITH   | matcher.match("Person", name__endswith="Smith")            |
        |                          |              |             | MATCH (_:Person) WHERE _.name ENDS WITH "Smith" RETURN _   |
        +--------------------------+--------------+-------------+------------------------------------------------------------+
        | Contains                 | __contains   | CONTAINS    | matcher.match("Person", name__contains="James")            |
        |                          |              |             | MATCH (_:Person) HWERE _.name CONTAINS "James" RETURN _    |
        +--------------------------+--------------+-------------+------------------------------------------------------------+

For a more comprehensive match using Cypher expressions, the :meth:`.NodeMatch.where` method can be used to further refine the selection.
Here, the underscore character can be used to refer to the node being filtered::

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

The underlying query is only evaluated when the selection undergoes iteration or when a specific evaluation method is called (such as :meth:`.NodeMatch.first`).
This means that a :class:`.NodeMatch` instance may be reused before and after a data changes for different results.

.. autoclass:: py2neo.matching.NodeMatcher
   :members:
   :special-members: __len__, __getitem__

.. autoclass:: py2neo.matching.NodeMatch
   :members:
   :special-members: __len__, __iter__


Relationship Matching
=====================

.. autoclass:: py2neo.matching.RelationshipMatcher
   :members:
   :special-members: __len__, __getitem__

.. autoclass:: py2neo.matching.RelationshipMatch
   :members:
   :special-members: __len__, __iter__

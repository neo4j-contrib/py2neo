************************************
``py2neo.text.table`` -- Text tables
************************************

.. module:: py2neo.text.table


:class:`.Table` objects
=======================

.. class:: Table(records, keys=None)

    A :class:`.Table` holds a list of :class:`.Record` objects, typically received as the result of a Cypher query.
    It provides a convenient container for working with a result in its entirety and provides methods for conversion into various output formats.
    :class:`.Table` extends ``list``.

    .. describe:: repr(table)

        Return a string containing an ASCII art representation of this table.
        Internally, this method calls :meth:`.write` with `header=True`, writing the output into an ``io.StringIO`` instance.

    .. automethod:: _repr_html_()

    .. automethod:: keys

    .. automethod:: field

    .. automethod:: write

    .. automethod:: write_html

    .. automethod:: write_separated_values

    .. automethod:: write_csv

    .. automethod:: write_tsv

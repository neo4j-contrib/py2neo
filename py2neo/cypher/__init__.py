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


__all__ = [
    "CypherExpression",
    "cypher_escape",
    "cypher_join",
    "cypher_repr",
    "cypher_str",
]


from py2neo.cypher.encoding import CypherEncoder
from py2neo.compat import string_types, unicode_types


class CypherExpression(object):

    def __init__(self, value):
        self.__value = value

    @property
    def value(self):
        return self.__value


def cypher_escape(identifier):
    """ Return a Cypher identifier, with escaping if required.

    Simple Cypher identifiers, which just contain alphanumerics
    and underscores, can be represented as-is in expressions.
    Any which contain more esoteric characters, such as spaces
    or punctuation, must be escaped in backticks. Backticks
    themselves are escaped by doubling.

    ::

        >>> cypher_escape("simple_identifier")
        'simple_identifier'
        >>> cypher_escape("identifier with spaces")
        '`identifier with spaces`'
        >>> cypher_escape("identifier with `backticks`")
        '`identifier with ``backticks```'

    Identifiers are used in Cypher to denote named values, labels,
    relationship types and property keys. This function will typically
    be used to construct dynamic Cypher queries in places where
    parameters cannot be used.

        >>> "MATCH (a:{label}) RETURN id(a)".format(label=cypher_escape("Employee of the Month"))
        'MATCH (a:`Employee of the Month`) RETURN id(a)'

    :param identifier: any non-empty string
    """
    if not isinstance(identifier, string_types):
        raise TypeError(type(identifier).__name__)
    encoder = CypherEncoder()
    return encoder.encode_key(identifier)


def cypher_join(*clauses, **parameters):
    """ Join multiple Cypher clauses, returning a (query, parameters)
    tuple. Each clause may either be a simple string query or a
    (query, parameters) tuple. Additional `parameters` may also be
    supplied as keyword arguments.

    :param clauses:
    :param parameters:
    :return: (query, parameters) tuple
    """
    query = []
    params = {}
    for clause in clauses:
        if clause is None:
            continue
        if isinstance(clause, tuple):
            try:
                q, p = clause
            except ValueError:
                raise ValueError("Expected query or (query, parameters) tuple "
                                 "for clause %r" % clause)
        else:
            q = clause
            p = None
        query.append(q)
        if p:
            params.update(p)
    params.update(parameters)
    return "\n".join(query), params


def cypher_repr(value, **kwargs):
    """ Return the Cypher representation of a value.

    This function attempts to convert the supplied value into a Cypher
    literal form, as used in expressions.

    """
    encoder = CypherEncoder(**kwargs)
    return encoder.encode_value(value)


def cypher_str(value, **kwargs):
    """ Convert a Cypher value to a Python Unicode string.

    This function converts the supplied value into a string form, as
    used for human-readable output. This is generally identical to
    :meth:`.cypher_repr` except for with string values, which are
    returned as-is, instead of being enclosed in quotes with certain
    characters escaped.

    """
    if isinstance(value, unicode_types):
        return value
    elif isinstance(value, string_types):
        return value.decode(kwargs.get("encoding", "utf-8"))
    else:
        return cypher_repr(value, **kwargs)

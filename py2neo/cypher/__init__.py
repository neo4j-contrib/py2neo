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


from .plan import *
from .lang import *


def cypher_escape(identifier, **kwargs):
    """ Escape a Cypher identifier in backticks.

    ::

        >>> cypher_escape("simple_identifier")
        'simple_identifier'
        >>> cypher_escape("identifier with spaces")
        '`identifier with spaces`'
        >>> cypher_escape("identifier with `backticks`")
        '`identifier with ``backticks```'

    :arg identifier: any non-empty string
    """
    from py2neo.compat import string
    if not isinstance(identifier, string):
        raise TypeError(type(identifier).__name__)
    from .encoder import CypherEncoder
    encoder = CypherEncoder(**kwargs)
    return encoder.encode_key(identifier)


def cypher_repr(value, **kwargs):
    """ Return the Cypher representation of a value.
    """
    from .encoder import CypherEncoder
    encoder = CypherEncoder(**kwargs)
    return encoder.encode_value(value)


def cypher_str(value, **kwargs):
    """ Convert a Cypher value to a Python Unicode string.
    """
    from py2neo.compat import unicode
    if isinstance(value, unicode):
        return value
    elif isinstance(value, bytes):
        return value.decode(kwargs.get("encoding", "utf-8"))
    else:
        return cypher_repr(value, **kwargs)

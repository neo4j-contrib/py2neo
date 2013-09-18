#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright 2011-2013, Nigel Small
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

""" Cypher Query Language module.
"""


from __future__ import unicode_literals

import json

from .neo4j import CypherQuery, CypherError
from .util import deprecated


@deprecated("The cypher module is deprecated, use "
            "neo4j.CypherQuery instead")
class Metadata(object):
    """Metadata for query results.
    """

    #: List of column names
    columns = []

    def __init__(self, columns=None):
        self.columns = columns or []


@deprecated("The cypher module is deprecated, use "
            "neo4j.CypherQuery instead")
def execute(graph_db, query, params=None, row_handler=None,
            metadata_handler=None, error_handler=None):
    query = CypherQuery(graph_db, query)
    data, metadata = [], None
    try:
        results = query.execute(**params or {})
    except CypherError as err:
        if error_handler:
            error_handler(err.message, err.exception, err.stack_trace)
        else:
            raise
    else:
        metadata = Metadata(results.columns)
        if metadata_handler:
            metadata_handler(metadata)
        if row_handler:
            for record in results:
                row_handler(list(record))
            return data, metadata
        else:
            return [list(record) for record in results], metadata


def dumps(obj, sep=(", ", ": ")):
    """ Dumps an object as a Cypher expression string.

    :param obj:
    :param sep:
    :return:
    """
    if isinstance(obj, dict):
        buffer = ["{"]
        link = ""
        for key, value in obj.items():
            buffer.append(link)
            if " " in key:
                buffer.append("`")
                buffer.append(key)
                buffer.append("`")
            else:
                buffer.append(key)
            buffer.append(sep[1])
            buffer.append(dumps(value, sep=sep))
            link = sep[0]
        buffer.append("}")
        return "".join(buffer)
    elif isinstance(obj, (tuple, set, list)):
        buffer = ["["]
        link = ""
        for value in obj:
            buffer.append(link)
            buffer.append(dumps(value, sep=sep))
            link = sep[0]
        buffer.append("]")
        return "".join(buffer)
    else:
        return json.dumps(obj)

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


from .neo4j import CypherError
from .util import deprecated


@deprecated("The cypher module is deprecated, use "
            "neo4j.GraphDatabaseService.cypher instead")
class Metadata(object):
    """Metadata for query results.
    """

    #: List of column names
    columns = []

    def __init__(self, columns=None):
        self.columns = columns or []


@deprecated("The cypher module is deprecated, use "
            "neo4j.GraphDatabaseService.cypher instead")
def execute(graph_db, query, params=None, row_handler=None,
            metadata_handler=None, error_handler=None):
    query = graph_db.cypher.query(query)
    data, metadata = [], None
    try:
        record_set = query.execute(params)
    except CypherError as err:
        if error_handler:
            error_handler(err.message, err.exception, err.stack_trace)
        else:
            raise
    else:
        metadata = Metadata(record_set.columns)
        if metadata_handler:
            metadata_handler(metadata)
        for record in record_set:
            if row_handler:
                row_handler(list(record))
            else:
                data.append(list(record))
        return data, metadata

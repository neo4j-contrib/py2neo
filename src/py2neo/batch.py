#!/usr/bin/env python

# Copyright 2011 Nigel Small
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

"""
Batch utility module
"""

__author__    = "Nigel Small <py2neo@nigelsmall.org>"
__copyright__ = "Copyright 2011 Nigel Small"
__license__   = "Apache License, Version 2.0"


try:
    from . import rest, neo4j
except ImportError:
    import rest, neo4j
except ValueError:
    import rest, neo4j


class BatchError(SystemError):

    def __init__(self, *args):
        SystemError.__init__(self, *args)

def relative_uri(uri, start):
    return "".join(uri.rpartition(start)[1:3])

def creator(id, value):
    if isinstance(value, dict):
        return {
            "method": "POST",
            "to": "/node",
            "id": id,
            "body": value
        }
    try:
        if isinstance(value[0], neo4j.Node):
            start_node_uri = relative_uri(value[0]._lookup("self"), "/node")
        else:
            start_node_uri = "{" + str(value[0]) + "}"
        if isinstance(value[2], neo4j.Node):
            end_node_uri = relative_uri(value[2]._lookup("self"), "/node")
        else:
            end_node_uri = "{" + str(value[2]) + "}"
        return {
            "method": "POST",
            "to": start_node_uri + "/relationships",
            "id": id,
            "body": {
                "to": end_node_uri,
                "data": value[3] if len(value) > 3 else {},
                "type": value[1]
            }
        }
    except KeyError:
        raise TypeError(value)

def result(value, graph_db):
    try:
        metadata = value["body"]
        if "type" in metadata:
            entity = neo4j.Relationship(metadata["self"], graph_db=graph_db)
        else:
            entity = neo4j.Node(metadata["self"], graph_db=graph_db)
        entity._metadata = rest.PropertyCache(metadata)
        return entity
    except KeyError:
        raise ValueError(value)

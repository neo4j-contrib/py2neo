#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright 2011-2014, Nigel Small
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


from __future__ import unicode_literals

import json
import logging

from py2neo.cypher.lang import Representation
from py2neo.cypher.results import CypherResults, IterableCypherResults, Record, RecordProducer
from py2neo.cypher.simple import CypherResource, CypherQuery
from py2neo.cypher.tx import Session, Transaction, TransactionError, TransactionFinished
from py2neo.util import is_collection


log = logging.getLogger("cypher")


# TODO keep in __init__ as wrapper
# TODO: add support for Node, NodePointer, Path, Rel, Relationship and Rev
def dumps(obj, separators=(", ", ": "), ensure_ascii=True):
    """ Dumps an object as a Cypher expression string.

    :param obj:
    :param separators:
    :return:
    """

    def dump_mapping(obj):
        buffer = ["{"]
        link = ""
        for key, value in obj.items():
            buffer.append(link)
            if " " in key:
                buffer.append("`")
                buffer.append(key.replace("`", "``"))
                buffer.append("`")
            else:
                buffer.append(key)
            buffer.append(separators[1])
            buffer.append(dumps(value, separators=separators,
                                ensure_ascii=ensure_ascii))
            link = separators[0]
        buffer.append("}")
        return "".join(buffer)

    def dump_collection(obj):
        buffer = ["["]
        link = ""
        for value in obj:
            buffer.append(link)
            buffer.append(dumps(value, separators=separators,
                                ensure_ascii=ensure_ascii))
            link = separators[0]
        buffer.append("]")
        return "".join(buffer)

    if isinstance(obj, dict):
        return dump_mapping(obj)
    elif is_collection(obj):
        return dump_collection(obj)
    else:
        return json.dumps(obj, ensure_ascii=ensure_ascii)

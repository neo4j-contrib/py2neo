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

import logging

from py2neo.cypher.create import CreateStatement
from py2neo.cypher.error.core import *
from py2neo.cypher.lang import *
from py2neo.cypher.core import *


log = logging.getLogger("cypher")


# TODO keep in __init__ as wrapper
# TODO: add support for Node, NodePointer, Path, Rel, Relationship and Rev
def dumps(obj, separators=(", ", ": "), ensure_ascii=True):
    """ Dumps an object as a Cypher expression string.

    :param obj:
    :param separators:
    :return:
    """

    from py2neo.util import is_collection

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


class CypherPipeline(object):

    def __init__(self, graph):
        self.parameters = {}
        self.parameter_filename = None
        self.graph = graph
        self.tx = None

    def begin(self):
        self.tx = self.graph.cypher.begin()

    def set_parameter(self, key, value):
        try:
            self.parameters[key] = json.loads(value)
        except ValueError:
            self.parameters[key] = value

    def set_parameter_filename(self, filename):
        self.parameter_filename = filename

    def execute(self, statement):
        import codecs
        if self.parameter_filename:
            columns = None
            with codecs.open(self.parameter_filename, encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if columns is None:
                        columns = line.split(",")
                    elif line:
                        values = json.loads("[" + line + "]")
                        p = dict(self.parameters)
                        p.update(zip(columns, values))
                        self.tx.execute(statement, p)
        else:
            self.tx.execute(statement, self.parameters)
        return self.tx.flush()

    def commit(self):
        self.tx.commit()


def main():
    import os
    import sys
    from py2neo.core import ServiceRoot
    from py2neo.packages.httpstream.packages.urimagic import URI
    from py2neo.util import ustr
    script, args = sys.argv[0], sys.argv[1:]
    uri = URI(os.getenv("NEO4J_URI", ServiceRoot.DEFAULT_URI)).resolve("/")
    service_root = ServiceRoot(uri.string)
    human_readable = False
    pipeline = CypherPipeline(service_root.graph)
    pipeline.begin()
    while args:
        arg = args.pop(0)
        if arg.startswith("-"):
            if arg in ("-p", "--parameter"):
                key = args.pop(0)
                value = args.pop(0)
                pipeline.set_parameter(key, value)
            elif arg in ("-f",):
                pipeline.set_parameter_filename(args.pop(0))
            elif arg in ("-h", "--human-readable"):
                human_readable = True
            else:
                raise ValueError("Unrecognised option %s" % arg)
        else:
            try:
                results = pipeline.execute(arg)
            except CypherError as error:
                sys.stderr.write("%s: %s\n\n" % (error.__class__.__name__, error.args[0]))
            else:
                for result in results:
                    if human_readable:
                        sys.stdout.write(ustr(result))
                    else:
                        sys.stdout.write("\t".join(map(json.dumps, result.columns)))
                        sys.stdout.write("\n")
                        for record in result:
                            sys.stdout.write("\t".join(map(cypher_repr, record)))
                            sys.stdout.write("\n")
                    sys.stdout.write("\n")
    try:
        pipeline.commit()
    except CypherTransactionError as error:
        sys.stderr.write(error.args[0])
        sys.stderr.write("\n")


if __name__ == "__main__":
    main()

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


import re

from pygments.lexer import RegexLexer, words
from pygments.token import *


KEYWORDS = [
    "AS",
    "ASSERT",
    "BY",
    "CALL",
    "CASE",
    "CONSTRAINT",
    "CSV",
    "DELETE",
    "DETACH",
    "DISTINCT",
    "DROP",
    "ELSE",
    "END",
    "ENDS",
    "EXPLAIN",
    "FIELDTERMINATOR",
    "FOREACH",
    "FROM",
    "HEADERS",
    "IN",
    "INDEX",
    "IS",
    "LIMIT",
    "LOAD",
    "MATCH",
    "MERGE",
    "ON",
    "OPTIONAL",
    "ORDER",
    "PROFILE",
    "REMOVE",
    "RETURN",
    "SET",
    "SKIP",
    "START",
    "STARTS",
    "THEN",
    "UNION",
    "UNIQUE",
    "UNWIND",
    "USING",
    "WHEN",
    "WHERE",
    "WITH",
    "YIELD",
]
CONSTANTS = [
    "NULL",
    "TRUE",
    "FALSE",
]
OPERATORS = [
    "AND",
    "OR",
    "XOR",
    "IS NULL",
    "STARTS WITH",
    "ENDS WITH",
]


class CypherLexer(RegexLexer):
    name = "Cypher"
    aliases = ["cypher"]
    filenames = ["*.cypher", "*.cyp"]

    flags = re.IGNORECASE
    tokens = {
        "root": [
            (r"\s+", Whitespace),
            (r"(" + "|".join(op.replace(" ", "\s+") for op in OPERATORS) + "\b)", Operator),
            (words(KEYWORDS, suffix=r"\b"), Keyword),
            (words(CONSTANTS, suffix=r"\b"), Keyword.Constant),
            (r"(\d*\.\d+)(e[+-]?\d+)?", Number.Float),
            (r"0[0-7]+", Number.Oct),
            (r"0x[0-9A-Fa-f]+", Number.Hex),
            (r"0", Number.Integer),
            (r"[1-9]\d*", Number.Integer),
            (r'[+*/<>=-]+', Operator),
        ],
    }

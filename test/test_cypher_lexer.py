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


from __future__ import absolute_import

from pygments import lex
from pygments.token import *
from unittest import TestCase

from py2neo.cypher.lexer import CypherLexer


class CypherLexerTestCase(TestCase):

    expression_sources = {
        u"0": Number.Integer,
        u"1": Number.Integer,
        u"123": Number.Integer,
        u"01": Number.Oct,
        u"0123": Number.Oct,
        u"0x1": Number.Hex,
        u"0x123": Number.Hex,
        u"0.0": Number.Float,
        u".0": Number.Float,
        u"1.0": Number.Float,
        u"0.1": Number.Float,
        u".1": Number.Float,
        u"123.0": Number.Float,
        u"0.123": Number.Float,
        u".123": Number.Float,
        u"123.456": Number.Float,
    }

    def test_should_correctly_parse_expressions(self):
        for expr, t in self.expression_sources.items():
            source = u"RETURN {}".format(expr)
            expected_tokens = [
                (Keyword, u'RETURN'),
                (Whitespace, u' '),
                (t, expr),
                (Whitespace, u'\n'),
            ]
            actual_tokens = list(lex(source, CypherLexer()))
            self.assertEqual(actual_tokens, expected_tokens)

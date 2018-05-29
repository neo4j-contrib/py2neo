#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright 2011-2018, Nigel Small
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


from unittest import TestCase

from pygments.token import Token

from py2neo.cypher.lexer import CypherLexer


class CypherLexerTestCase(TestCase):

    single_statements = {
        "RETURN 1": [(Token.Keyword, 'RETURN'), (Token.Literal.Number.Integer, '1')],
        "RETURN $x": [(Token.Keyword, 'RETURN'), (Token.Punctuation, '$'), (Token.Name.Variable.Global, 'x')],
        "RETURN 1 AS x": [(Token.Keyword, 'RETURN'), (Token.Literal.Number.Integer, '1'),
                          (Token.Keyword, 'AS'), (Token.Name.Variable, 'x')],
        "RETURN 1 AS `x y`": [(Token.Keyword, 'RETURN'), (Token.Literal.Number.Integer, '1'),
                              (Token.Keyword, 'AS'), (Token.Name.Variable, '`x y`')],
        "RETURN 1 AS `x ``y```": [(Token.Keyword, 'RETURN'), (Token.Literal.Number.Integer, '1'),
                                  (Token.Keyword, 'AS'), (Token.Name.Variable, '`x ``y```')],
        "WITH 1 AS x RETURN x": [(Token.Keyword, 'WITH'), (Token.Literal.Number.Integer, '1'),
                                 (Token.Keyword, 'AS'), (Token.Name.Variable, 'x'),
                                 (Token.Keyword, 'RETURN'), (Token.Name.Variable, 'x')],
        "UNWIND range(1, 10) AS n RETURN n": [(Token.Keyword, 'UNWIND'),
                                              (Token.Name.Function, 'range'), (Token.Punctuation, '('),
                                              (Token.Literal.Number.Integer, '1'), (Token.Punctuation, ','),
                                              (Token.Literal.Number.Integer, '10'), (Token.Punctuation, ')'),
                                              (Token.Keyword, 'AS'), (Token.Name.Variable, 'n'),
                                              (Token.Keyword, 'RETURN'), (Token.Name.Variable, 'n')],

    }

    multiple_statements = {
        "RETURN 1; RETURN 2": ["RETURN 1", "RETURN 2"],
        "RETURN ';' AS semicolon; RETURN 'semicolon' AS `;`": ["RETURN ';' AS semicolon", "RETURN 'semicolon' AS `;`"]
    }

    def test_single_statements(self):
        lexer = CypherLexer()
        for text, expected_tokens in self.single_statements.items():
            actual_tokens = [token for token in lexer.get_tokens(text) if token[0] is not Token.Text.Whitespace]
            self.assertEqual(actual_tokens, expected_tokens)

    def test_multiple_statements(self):
        lexer = CypherLexer()
        for text, expected_tokens in self.multiple_statements.items():
            actual_tokens = [token for token in lexer.get_statements(text)]
            self.assertEqual(actual_tokens, expected_tokens)

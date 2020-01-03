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
        "// Rest of line comment\nRETURN x": [
            (Token.Comment.Single, '//'),
            (Token.Comment.Single, ' Rest of line comment'),
            (Token.Keyword, 'RETURN'),
            (Token.Name.Variable, 'x'),
        ],
        "WITH x // Rest of line comment after code\nRETURN x": [
            (Token.Keyword, 'WITH'),
            (Token.Name.Variable, 'x'),
            (Token.Comment.Single, '//'),
            (Token.Comment.Single, ' Rest of line comment after code'),
            (Token.Keyword, 'RETURN'),
            (Token.Name.Variable, 'x'),
        ],
        "WITH '//' AS x // Rest of line comment after code containing '//'\n RETURN x": [
            (Token.Keyword, 'WITH'),
            (Token.Literal.String, "'//'"),
            (Token.Keyword, 'AS'),
            (Token.Name.Variable, 'x'),
            (Token.Comment.Single, "//"),
            (Token.Comment.Single, " Rest of line comment after code containing '//'"),
            (Token.Keyword, 'RETURN'),
            (Token.Name.Variable, 'x'),
        ],
        "SET o:`http://example.org` // This is a comment": [
            (Token.Keyword, 'SET'),
            (Token.Name.Variable, 'o'),
            (Token.Punctuation, ':'),
            (Token.Name.Label, '`http://example.org`'),
            (Token.Comment.Single, "//"),
            (Token.Comment.Single, " This is a comment"),
        ],
        "/* Block comment */": [
            (Token.Comment.Multiline, '/*'),
            (Token.Comment.Multiline, ' Block comment '),
            (Token.Comment.Multiline, '*/'),
        ],
        "WITH x /* Block comment within code */ RETURN x": [
            (Token.Keyword, 'WITH'),
            (Token.Name.Variable, 'x'),
            (Token.Comment.Multiline, '/*'),
            (Token.Comment.Multiline, ' Block comment within code '),
            (Token.Comment.Multiline, '*/'),
            (Token.Keyword, 'RETURN'),
            (Token.Name.Variable, 'x'),
        ],
        "WITH x /* Block comment\nwith line break */ RETURN x": [
            (Token.Keyword, 'WITH'),
            (Token.Name.Variable, 'x'),
            (Token.Comment.Multiline, '/*'),
            (Token.Comment.Multiline, ' Block comment\nwith line break '),
            (Token.Comment.Multiline, '*/'),
            (Token.Keyword, 'RETURN'),
            (Token.Name.Variable, 'x'),
        ],
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

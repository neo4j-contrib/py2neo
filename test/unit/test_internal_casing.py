#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright 2011-2019, Nigel Small
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


from py2neo.internal.casing import Case


def test_breakdown_of_string_with_spaces():
    x = Case("hello world")
    assert x.words == ("hello", "world")


def test_breakdown_of_string_with_underscores():
    x = Case("hello_world")
    assert x.words == ("hello", "world")


def test_breakdown_of_string_with_hyphens():
    x = Case("hello-world")
    assert x.words == ("hello", "world")


def test_breakdown_of_single_word_upper_case_string():
    x = Case("HELLO")
    assert x.words == ("HELLO",)


def test_breakdown_tuple():
    x = Case(("hello", "world"))
    assert x.words == ("hello", "world")


def test_upper():
    x = Case("Hello world")
    assert x.upper() == "HELLO WORLD"


def test_lower():
    x = Case("Hello world")
    assert x.lower() == "hello world"


def test_title():
    x = Case("Hello WORLD")
    assert x.title() == "Hello WORLD"


def test_snake():
    x = Case("Hello world")
    assert x.snake() == "hello_world"


def test_camel():
    x = Case("Hello world")
    assert x.camel() == "helloWorld"


def test_camel_with_upper_first():
    x = Case("Hello world")
    assert x.camel(upper_first=True) == "HelloWorld"

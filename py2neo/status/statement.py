#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright 2011-2015, Nigel Small
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


from py2neo.status import ClientError


# ArithmeticError
class ArithmeticError(ClientError):
    """ Invalid use of arithmetic, such as dividing by zero.
    """


# Constraint...?
class ConstraintViolation(ClientError):
    """ A constraint imposed by the statement is violated by the data
    in the database.
    """


# EntityNotFound
class EntityNotFound(ClientError):
    """ The statement is directly referring to an entity that does not
    exist.
    """


# ArgumentError
class InvalidArguments(ClientError):
    """ The statement is attempting to perform operations using invalid
    arguments
    """


# SemanticError
class InvalidSemantics(ClientError):
    """ The statement is syntactically valid, but expresses something
    that the database cannot do.
    """


# SyntaxError
class InvalidSyntax(ClientError):
    """ The statement contains invalid or unsupported syntax.
    """


# TypeError
class InvalidType(ClientError):
    """ The statement is attempting to perform operations on values
    with types that are not supported by the operation.
    """


# LabelNotFound
class NoSuchLabel(ClientError):
    """ The statement is referring to a label that does not exist.
    """


# PropertyNotFound
class NoSuchProperty(ClientError):
    """ The statement is referring to a property that does not exist.
    """


# ParameterNotFound
class ParameterMissing(ClientError):
    """ The statement is referring to a parameter that was not provided
    in the request.
    """

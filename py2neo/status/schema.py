#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright 2011-2016, Nigel Small
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


# ConstraintAlreadyExists
class ConstraintAlreadyExists(ClientError):
    """ Unable to perform operation because it would clash with a
    pre-existing constraint.
    """


# Constraint...?
class ConstraintVerificationFailure(ClientError):
    """ Unable to create constraint because data that exists in the
    database violates it.
    """


# Constraint...?
class ConstraintViolation(ClientError):
    """ A constraint imposed by the database was violated.
    """


# TokenNameError
class IllegalTokenName(ClientError):
    """ A token name, such as a label, relationship type or property
    key, used is not valid. Tokens cannot be empty strings and cannot
    be null.
    """


# IndexAlreadyExists
class IndexAlreadyExists(ClientError):
    """ Unable to perform operation because it would clash with a
    pre-existing index.
    """


# IndexLocked? (dependency error)
class IndexBelongsToConstraint(ClientError):
    """ A requested operation can not be performed on the specified
    index because the index is part of a constraint. If you want to
    drop the index, for instance, you must drop the constraint.
    """


# IndexLimitReached
class IndexLimitReached(ClientError):
    """ The maximum number of index entries supported has been reached,
    no more entities can be indexed.
    """


# LabelLimitReached
class LabelLimitReached(ClientError):
    """ The maximum number of labels supported has been reached, no
    more labels can be created.
    """


# ConstraintNotFound
class NoSuchConstraint(ClientError):
    """ The request (directly or indirectly) referred to a constraint
    that does not exist.
    """


# IndexNotFound
class NoSuchIndex(ClientError):
    """ The request (directly or indirectly) referred to an index that
    does not exist.
    """

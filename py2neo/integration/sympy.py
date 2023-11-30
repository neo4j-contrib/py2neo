#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright (c) "Neo4j"
# Neo4j Sweden AB [https://neo4j.com]
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


from __future__ import absolute_import, print_function, unicode_literals


"""
Provides integration with `sympy <https://www.sympy.org/>`_.

.. note::
   This module requires sympy to be installed, and will raise a
   warning if this is not available.

"""


from warnings import warn

try:
    # noinspection PyPackageRequirements
    from sympy import MutableMatrix, ImmutableMatrix
except ImportError:
    warn("The py2neo.integration.sympy module expects sympy to be "
         "installed but it does not appear to be available.")
    raise


def cursor_to_matrix(cursor, mutable=False):
    """ Consume and extract the entire result as a
    `sympy.Matrix <https://docs.sympy.org/latest/tutorial/matrices.html>`_.

    .. note::
       This method requires `sympy` to be installed.

    :param cursor:
    :param mutable:
    :returns: `Matrix
        <https://docs.sympy.org/latest/tutorial/matrices.html>`_ object.
    """
    if mutable:
        return MutableMatrix(list(map(list, cursor)))
    else:
        return ImmutableMatrix(list(map(list, cursor)))

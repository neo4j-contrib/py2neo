#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright 2011-2021, Nigel Small
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
Provides integration with `numpy <https://numpy.org/>`_.

.. note::
   This module requires numpy to be installed, and will raise a
   warning if this is not available.

"""


from warnings import warn

try:
    # noinspection PyPackageRequirements
    from numpy import array
except ImportError:
    warn("The py2neo.integration.numpy module expects numpy to be "
         "installed but it does not appear to be available.")
    raise


def cursor_to_ndarray(cursor, dtype=None, order='K'):
    """ Consume and extract the entire result as a
    `numpy.ndarray <https://numpy.org/doc/stable/reference/generated/numpy.ndarray.html>`_.

    .. note::
       This method requires `numpy` to be installed.

    :param cursor:
    :param dtype:
    :param order:
    :returns: `ndarray
        <https://numpy.org/doc/stable/reference/generated/numpy.ndarray.html>`__ object.
    """
    return array(list(map(list, cursor)), dtype=dtype, order=order)

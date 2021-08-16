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
Provides integration with `pandas <https://pandas.pydata.org/>`_.

.. note::
   This module requires pandas to be installed, and will raise a
   warning if this is not available.

Example:

    >>> from py2neo import Graph
    >>> from py2neo.integration.pandas import cursor_to_data_frame
    >>> graph = Graph()
    >>> df = cursor_to_data_frame(graph.query("MATCH (a:Person) RETURN a.name AS name, a.born AS born"))
    >>> df
                       name    born
    0          Keanu Reeves  1964.0
    1      Carrie-Anne Moss  1967.0
    2    Laurence Fishburne  1961.0
    3          Hugo Weaving  1960.0
    4       Lilly Wachowski  1967.0
    ..                  ...     ...
    128      Penny Marshall  1943.0
    129         Paul Blythe     NaN
    130        Angela Scope     NaN
    131    Jessica Thompson     NaN
    132      James Thompson     NaN

    [133 rows x 2 columns]

"""


from warnings import warn

try:
    # noinspection PyPackageRequirements
    from pandas import DataFrame, Series
except ImportError:
    warn("The py2neo.integration.pandas module expects pandas to be "
         "installed but it does not appear to be available.")
    raise

from py2neo.bulk import create_nodes, merge_nodes


def cursor_to_series(cursor, field=0, index=None, dtype=None):
    """ Consume and extract one field of the entire result as a
    `pandas.Series <https://pandas.pydata.org/pandas-docs/stable/dsintro.html#series>`_.

    :param cursor:
    :param field:
    :param index:
    :param dtype:
    :returns: `Series
        <https://pandas.pydata.org/pandas-docs/stable/dsintro.html#series>`__ object.
    """
    return Series([record[field] for record in cursor], index=index, dtype=dtype)


def cursor_to_data_frame(cursor, index=None, columns=None, dtype=None):
    """ Consume and extract the entire result as a
    `pandas.DataFrame <https://pandas.pydata.org/pandas-docs/stable/dsintro.html#dataframe>`_.

    :param cursor:
    :param index: Index to use for resulting frame.
    :param columns: Column labels to use for resulting frame.
    :param dtype: Data type to force.
    :returns: `DataFrame
        <https://pandas.pydata.org/pandas-docs/stable/dsintro.html#series>`__ object.
    """
    return DataFrame(list(map(dict, cursor)), index=index, columns=columns, dtype=dtype)


def create_nodes_from_data_frame(tx, df, labels=None):
    """ Create nodes from a DataFrame.

    This function wraps the :func:`py2neo.bulk.create_nodes` function,
    allowing a `DataFrame <https://pandas.pydata.org/pandas-docs/stable/dsintro.html#series>`__
    object to be passed in place of the regular `data` argument.

    :param tx:
    :param df:
    :param labels:
    :return:
    """
    create_nodes(tx, df.itertuples(index=False, name=None),
                 labels=labels, keys=df.keys())


def merge_nodes_from_data_frame(tx, df, merge_key, labels=None, preserve=None):
    """ Merge nodes from a DataFrame.

    This function wraps the :func:`py2neo.bulk.merge_nodes` function,
    allowing a `DataFrame <https://pandas.pydata.org/pandas-docs/stable/dsintro.html#series>`__
    object to be passed in place of the regular `data` argument.

    :param tx:
    :param df:
    :param merge_key:
    :param labels:
    :param preserve:
    :return:
    """
    merge_nodes(tx, df.itertuples(index=False, name=None), merge_key,
                labels=labels, keys=df.keys(), preserve=preserve)

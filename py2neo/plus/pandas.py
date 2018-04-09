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


from py2neo.database import Graph


def read_cypher(cypher, parameters=None, graph=None, index=None, columns=None, dtype=None):
    """ Run Cypher and extract the entire result as a
    `pandas.DataFrame <http://pandas.pydata.org/pandas-docs/stable/dsintro.html#dataframe>`_.

    ::

        >>> from py2neo import Graph
        >>> from py2neo.plus.pandas import read_cypher
        >>> graph = Graph()
        >>> read_cypher("MATCH (a:Person) RETURN a.name, a.born LIMIT 4", graph=graph)
           a.born              a.name
        0    1964        Keanu Reeves
        1    1967    Carrie-Anne Moss
        2    1961  Laurence Fishburne
        3    1960        Hugo Weaving

    :param cypher:
    :param parameters:
    :param graph:
    :param index:
    :param columns:
    :param dtype:
    :return: data frame
    """
    try:
        from pandas import DataFrame
    except ImportError:
        raise ImportError("Pandas must be installed to use py2neo.plus.pandas")
    if graph is None:
        graph = Graph()
    return DataFrame(list(map(dict, graph.run(cypher, parameters))), index=index, columns=columns, dtype=dtype)

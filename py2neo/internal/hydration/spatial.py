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


"""
This module defines spatial data types.
"""


from py2neo.connect.packstream import Structure
from py2neo.spatial import Point


def hydrate_point(srid, *coordinates):
    """ Create a new instance of a Point subclass from a raw
    set of fields. The subclass chosen is determined by the
    given SRID; a ValueError will be raised if no such
    subclass can be found.
    """
    try:
        point_class, dim = Point.class_for_srid(srid)
    except KeyError:
        point = Point(coordinates)
        point.srid = srid
        return point
    else:
        if len(coordinates) != dim:
            raise ValueError("SRID %d requires %d coordinates (%d provided)" % (srid, dim, len(coordinates)))
        return point_class(coordinates)


def dehydrate_point(value):
    """ Dehydrator for Point data.

    :param value:
    :type value: Point
    :return:
    """
    dim = len(value)
    if dim == 2:
        return Structure(b"X", value.srid, *value)
    elif dim == 3:
        return Structure(b"Y", value.srid, *value)
    else:
        raise ValueError("Cannot dehydrate Point with %d dimensions" % dim)


__hydration_functions = {
    b"X": hydrate_point,
    b"Y": hydrate_point,
}

__dehydration_functions = {
    Point: dehydrate_point,
}
__dehydration_functions.update({cls: dehydrate_point for cls in Point.__subclasses__()})


def hydration_functions():
    return __hydration_functions


def dehydration_functions():
    return __dehydration_functions

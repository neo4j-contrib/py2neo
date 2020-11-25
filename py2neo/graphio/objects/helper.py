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


from itertools import chain, islice
import logging
from py2neo import ClientError

log = logging.getLogger(__name__)


def chunks(iterable, size=10):
    """
    Get chunks of an iterable without pre-walking it.

    https://stackoverflow.com/questions/24527006/split-a-generator-into-chunks-without-pre-walking-it

    :param iterable: The iterable.
    :param size: Chunksize.
    :return: Yield chunks of defined size.
    """
    iterator = iter(iterable)
    for first in iterator:
        yield chain([first], islice(iterator, int(size) - 1))


def create_single_index(graph, label, prop):
    """
    Create an inidex on a single property.

    :param label: The label.
    :param prop: The property.
    """
    try:
        log.debug("Create index {}, {}".format(label, prop))
        q = "CREATE INDEX ON :{0}({1})".format(label, prop)
        log.debug(q)
        graph.run(q)

    except ClientError:
        # TODO check if the index exists instead of catching the (very general) ClientError
        log.debug("Index {}, {} cannot be created, it likely exists alredy.".format(label, prop))


def create_composite_index(graph, label, properties):
    """
    Create an inidex on a single property.

    :param label: The label.
    :param prop: The property.
    """
    try:
        property_string = ', '.join(properties)
        log.debug("Create index {}, {}".format(label, property_string))
        q = "CREATE INDEX ON :{0}({1})".format(label, property_string)
        log.debug(q)
        graph.run(q)

    except ClientError:
        # TODO check if the index exists instead of catching the (very general) ClientError
        log.debug("Index {}, {} cannot be created, it likely exists alredy.".format(label, properties))

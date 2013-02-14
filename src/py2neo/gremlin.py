#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright 2011-2013, Nigel Small
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

"""Gremlin utility module
"""

import logging

from . import rest

logger = logging.getLogger(__name__)


def execute(script, graph_db):
    """
    Execute a script against a database using the Gremlin plugin, if available.

    :param script:              a string containing the Gremlin script to execute
    :raise NotImplementedError: if the Gremlin plugin is not available
    :return:                    the result of the Gremlin script
    """
    try:
        uri = graph_db._extension_uri('GremlinPlugin', 'execute_script')
    except NotImplementedError:
        raise NotImplementedError("Gremlin functionality not available")
    rs = graph_db._send(rest.Request(graph_db, "POST", uri, {'script': script}))
    return rs.body

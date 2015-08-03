#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright 2011-2014, Nigel Small
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


import logging

import pytest

from py2neo import Graph
from py2neo.env import NEO4J_HTTP_URI


PY2NEO_LOGGING_LEVEL_COLOUR_MAP = {
    10: (None, 'blue', True),
    20: (None, 'blue', True),
    30: (None, 'yellow', True),
    40: ('red', 'white', True),
    50: ('red', 'white', True),
}

logger = logging.getLogger('pytest_configure')
logger.setLevel(logging.INFO)


@pytest.fixture
def graph(request):
    return Graph(NEO4J_HTTP_URI)

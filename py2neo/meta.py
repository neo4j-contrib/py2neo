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

from sys import platform, version_info

import urllib3
import neo4j


__all__ = ["__author__", "__copyright__", "__email__", "__license__", "__package__", "__version__",
           "BOLT_USER_AGENT", "HTTP_USER_AGENT"]

__author__ = "Nigel Small <technige@nige.tech>"
__copyright__ = "2011-2016, Nigel Small"
__email__ = "py2neo@nige.tech"
__license__ = "Apache License, Version 2.0"
__package__ = "py2neo"
__version__ = "4.0.0b1"


BOLT_USER_AGENT = "{}/{} neo4j-python/{} Python/{}.{}.{}-{}-{} ({})".format(
    *((__package__, __version__, neo4j.__version__,) + tuple(version_info) + (platform,)))
HTTP_USER_AGENT = "{}/{} urllib3/{} Python/{}.{}.{}-{}-{} ({})".format(
    *((__package__, __version__, urllib3.__version__,) + tuple(version_info) + (platform,)))

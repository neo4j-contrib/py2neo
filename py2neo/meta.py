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


__all__ = [
    "__author__",
    "__copyright__",
    "__email__",
    "__license__",
    "__package__",
    "__version__",
    "NEO4J_URI",
    "NEO4J_AUTH",
    "NEO4J_USER_AGENT",
    "NEO4J_SECURE",
    "NEO4J_VERIFY",
    "bolt_user_agent",
    "http_user_agent",
]

__author__ = "Nigel Small <technige@nige.tech>"
__copyright__ = "2011-2020, Nigel Small"
__email__ = "py2neo@nige.tech"
__license__ = "Apache License, Version 2.0"
__package__ = "py2neo"
__version__ = "5.0.dev0"


from os import getenv


NEO4J_URI = getenv("NEO4J_URI")
NEO4J_AUTH = getenv("NEO4J_AUTH")
NEO4J_USER_AGENT = getenv("NEO4J_USER_AGENT")
NEO4J_SECURE = True if getenv("NEO4J_SECURE") == "1" else False if getenv("NEO4J_SECURE") == "0" else None
NEO4J_VERIFY = True if getenv("NEO4J_VERIFY") == "1" else False if getenv("NEO4J_VERIFY") == "0" else None


def bolt_user_agent():
    from sys import platform, version_info
    fields = (__package__, __version__,) + tuple(version_info) + (platform,)
    return "{}/{} Python/{}.{}.{}-{}-{} ({})".format(*fields)


def http_user_agent():
    from sys import platform, version_info
    import urllib3
    fields = (__package__, __version__, urllib3.__version__,) + tuple(version_info) + (platform,)
    return "{}/{} urllib3/{} Python/{}.{}.{}-{}-{} ({})".format(*fields)

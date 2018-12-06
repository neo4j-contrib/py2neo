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


__all__ = ["__author__", "__copyright__", "__email__", "__license__", "__package__", "__version__",
           "bolt_user_agent", "http_user_agent"]

__author__ = "Nigel Small <technige@nige.tech>"
__copyright__ = "2011-2018, Nigel Small"
__email__ = "py2neo@nige.tech"
__license__ = "Apache License, Version 2.0"
__package__ = "py2neo"
__version__ = "4.1.3"


def bolt_user_agent():
    from sys import platform, version_info
    from neobolt.meta import version as neobolt_version
    return "{}/{} neobolt/{} Python/{}.{}.{}-{}-{} ({})".format(
        *((__package__, __version__, neobolt_version,) + tuple(version_info) + (platform,)))


def http_user_agent():
    from sys import platform, version_info
    import urllib3
    return "{}/{} urllib3/{} Python/{}.{}.{}-{}-{} ({})".format(
        *((__package__, __version__, urllib3.__version__,) + tuple(version_info) + (platform,)))

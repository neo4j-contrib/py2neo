#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright 2011-2017, Nigel Small
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


from os import getenv

from py2neo.internal.compat import urlsplit
from py2neo.meta import bolt_user_agent, http_user_agent


def get_connection_data(uri=None, **settings):
    """ Generate a dictionary of connection data for an optional URI plus
    additional connection settings.

    :param uri:
    :param settings:
    :return:
    """
    data = {
        "user_agent": None,
        "secure": None,
        "scheme": None,
        "user": None,
        "password": None,
        "host": None,
        "port": None,
    }
    # apply uri
    uri = uri or getenv("NEO4J_URI")
    if uri:
        parsed = urlsplit(uri)
        if parsed.scheme:
            data["scheme"] = parsed.scheme
            if data["scheme"] in ["https"]:
                data["secure"] = True
            elif data["scheme"] in ["http"]:
                data["secure"] = False
        if parsed.username:
            data["user"] = parsed.username
        if parsed.password:
            data["password"] = parsed.password
        if parsed.hostname:
            data["host"] = parsed.hostname
        if parsed.port:
            data["port"] = parsed.port
    # apply auth (this can override `uri`)
    if "auth" in settings:
        data["user"], data["password"] = settings["auth"]
    elif getenv("NEO4J_AUTH"):
        data["user"], _, data["password"] = getenv("NEO4J_AUTH").partition(":")
    # apply components (these can override `uri` and `auth`)
    if "user_agent" in settings:
        data["user_agent"] = settings["user_agent"]
    if "secure" in settings:
        data["secure"] = settings["secure"]
    if "scheme" in settings:
        data["scheme"] = settings["scheme"]
    if "user" in settings:
        data["user"] = settings["user"]
    if "password" in settings:
        data["password"] = settings["password"]
    if "host" in settings:
        data["host"] = settings["host"]
    if "port" in settings:
        data["port"] = settings["port"]
    # apply default port for scheme
    if data["scheme"] and not data["port"]:
        if data["scheme"] == "http":
            data["port"] = 7474
        elif data["scheme"] == "https":
            data["port"] = 7473
        elif data["scheme"] in ["bolt", "bolt+routing"]:
            data["port"] = 7687
    # apply other defaults
    if not data["user_agent"]:
        data["user_agent"] = http_user_agent() if data["scheme"] in ["http", "https"] else bolt_user_agent()
    if data["secure"] is None:
        data["secure"] = False
    if not data["scheme"]:
        data["scheme"] = "bolt"
    if not data["user"]:
        data["user"] = "neo4j"
    if not data["password"]:
        data["password"] = "password"
    if not data["host"]:
        data["host"] = "localhost"
    if not data["port"]:
        data["port"] = 7687
    # apply composites
    data["auth"] = (data["user"], data["password"])
    data["uri"] = "%s://%s:%s" % (data["scheme"], data["host"], data["port"])
    data["hash"] = hash(tuple(sorted(data.items())))
    return data

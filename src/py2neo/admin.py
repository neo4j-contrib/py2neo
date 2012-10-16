#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright 2011-2012 Nigel Small
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

""" Server administration module
"""


from datetime import datetime
from . import rest, util


DEFAULT_URI = "http://localhost:7474/db/manage/"

class _Service(rest.Resource):

    def __init__(self, admin_uri, service, metadata=None):
        rest.Resource.__init__(self, admin_uri, "/server", metadata=metadata)
        rs = self._send(rest.Request(self, "GET", self._uri))
        self._update_metadata(rs.body)
        # force URI adjustment (in case supplied without trailing slash)
        self._uri = rest.URI(rs.uri, "/")
        self._service_uri = self._metadata('services')[service]


class Monitor(_Service):

    def __init__(self, admin_uri=None, metadata=None):
        admin_uri = admin_uri or DEFAULT_URI
        _Service.__init__(self, admin_uri, "monitor", metadata=metadata)
        self._resource_uris = self._send(
            rest.Request(None, "GET", self._service_uri)
        ).body["resources"]

    def fetch_latest_data(self):
        uri = self._resource_uris["latest_data"]
        rs = self._send(rest.Request(None, "GET", uri))
        timestamps = rs.body["timestamps"]
        data = rs.body["data"]
        data = zip(
            (datetime.fromtimestamp(t) for t in timestamps),
            zip(
                (util.numberise(n) for n in data["node_count"]),
                (util.numberise(n) for n in data["relationship_count"]),
                (util.numberise(n) for n in data["property_count"]),
            ),
        )
        return data

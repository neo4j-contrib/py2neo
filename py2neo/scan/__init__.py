#!/usr/bin/env python
# coding: utf-8

# Copyright 2011-2021, Nigel Small
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


from pansi.console import Console

from py2neo.client import ConnectionProfile, Connection, ConnectionUnavailable


class Scanner(Console):

    def __init__(self, profile=None, **settings):
        super(Scanner, self).__init__("py2neo", verbosity=1)
        self.profile = ConnectionProfile(profile, **settings)

    def scan(self):
        from pprint import pprint
        result = self.check_connection(self.profile)
        pprint(result)

    def check_connection(self, profile):
        result = {"profile": profile.uri}
        try:
            cx = Connection.open(profile)
        except ConnectionUnavailable:
            print("Cannot connect to %r" % (profile,))
            raise
        else:
            result["protocol_version"] = ".".join(map(str, cx.protocol_version))
            result["user_agent"] = cx.user_agent
            result["server"] = cx.server_agent
            result["neo4j_version"] = cx.neo4j_version
            routers, readers, writers, ttl = cx.route()
            result["routing_table"] = {
                "routers": [p.uri for p in routers],
                "readers": [p.uri for p in readers],
                "writers": [p.uri for p in writers],
                "ttl": ttl,
            }
            cx.close()
        return result


if __name__ == "__main__":
    Scanner().scan()

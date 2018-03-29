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


from uuid import uuid4

from pytest import main as test_main

from py2neo import reset_py2neo
from py2neo.install import Warehouse


def main():
    edition = "community"
    versions = ["3.4", "3.3", "3.2", "3.1", "3.0"]
    user = "neo4j"
    password = "password"
    warehouse = Warehouse()
    for version in versions:
        name = uuid4().hex
        installation = warehouse.install(name, edition, version)
        print("Installed Neo4j %s to %s" % (version, installation.home))
        existing_user_names = [u.name for u in list(installation.auth)]
        if user in existing_user_names:
            installation.auth.update(user, password)
        else:
            installation.auth.append(user, password)
        pid = installation.server.start()
        print("Started Neo4j server with PID %d" % pid)
        test_main()
        installation.server.stop()
        warehouse.uninstall(name)
        reset_py2neo()


if __name__ == "__main__":
    main()

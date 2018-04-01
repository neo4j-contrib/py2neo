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


from uuid import uuid4

from pytest import main as test_main

from py2neo import reset_py2neo
from py2neo.admin import Warehouse


def run_tests(versions, user, password):
    warehouse = Warehouse()
    for version in versions:
        name = uuid4().hex
        installation = warehouse.install(name, "community", version)
        print("Installed Neo4j community %s to %s" % (version, installation.home))
        installation.auth.update(user, password)
        pid = installation.server.start()
        print("Started Neo4j server with PID %d" % pid)
        try:
            test_main()
        finally:
            installation.server.stop()
            warehouse.uninstall(name)
            reset_py2neo()


def main():
    versions = ["3.4", "3.3", "3.2", "3.1", "3.0"]
    user = "neo4j"
    password = "password"
    print("Running tests")
    run_tests(versions, user, password)


if __name__ == "__main__":
    main()

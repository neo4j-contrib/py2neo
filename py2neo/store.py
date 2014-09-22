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


import os
from shutil import rmtree


class GraphStore(object):

    # TODO: instances

    @classmethod
    def for_server(cls, server):
        # TODO: actually sniff config files for the true path
        return GraphStore(os.path.join(server.home, "data", "graph.db"))

    def __init__(self, path):
        self.path = path

    @property
    def locked(self):
        return os.path.isfile(os.path.join(self.path, "lock"))

    def drop(self, force=False):
        if not force and self.locked:
            raise RuntimeError("Refusing to drop database store while in use")
        else:
            rmtree(self.path, ignore_errors=True)

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


import os

from py2neo.packages.httpstream.packages.urimagic import URI


__all__ = ["NEO4J_AUTH", "NEO4J_HOME", "NEO4J_URI"]


#: Auth string, stored as `user:password`.
NEO4J_AUTH = os.getenv("NEO4J_AUTH", None)

#: Default path for GraphServer instances.
NEO4J_HOME = os.getenv("NEO4J_HOME", ".")

#: Default URI for DBMS instances.
NEO4J_URI = URI(os.getenv("NEO4J_URI", "http://localhost:7474/"))

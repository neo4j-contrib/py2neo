#!/usr/bin/env python
# -*- encoding: utf-8 -*-

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


from datetime import date as _date

from py2neo.meta import get_metadata as _get_metadata


metadata = _get_metadata()

__author__ = metadata["author"]
__copyright__ = "{}-{}, {}".format(2011, _date.today().year, metadata["author"])
__email__ = metadata["author_email"]
__license__ = metadata["license"]
__package__ = metadata["name"]
__version__ = metadata["version"]


from py2neo.data import *
from py2neo.database import *
from py2neo.errors import *
from py2neo.matching import *

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


from __future__ import print_function

from argparse import ArgumentParser
from inspect import getdoc

from py2neo.compat import argument


@argument("-u", "--uri",
          help="Set the connection URI.")
@argument("-a", "--auth", metavar="USER:PASSWORD",
          help="Set the user and password.")
def monitor(uri, auth=None):
    """ Scan...
    """
    from py2neo.monitor import Monitor
    Monitor(uri, auth=auth).run()


def main():
    parser = ArgumentParser(description=getdoc(monitor))
    for a, kw in monitor.arguments:
        parser.add_argument(*a, **kw)

    args = parser.parse_args()
    kwargs = vars(args)
    monitor(**kwargs)


if __name__ == "__main__":
    main()

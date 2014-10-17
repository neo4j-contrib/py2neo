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


from __future__ import print_function

import os
import sys

from py2neo.ext.distro.core import download
from py2neo.util import ustr


HELP = """\
Usage: {script} «edition» «version» [«path»]
"""


def _help(script):
    print(HELP.format(script=os.path.basename(script)))


def main():
    script, args = sys.argv[0], sys.argv[1:]
    try:
        if args:
            if len(args) == 3:
                edition, version, path = args
                download(edition, version, path)
            elif len(args) == 2:
                edition, version = args
                download(edition, version)
            else:
                _help(script)
        else:
            _help(script)
    except Exception as error:
        sys.stderr.write(ustr(error))
        sys.stderr.write("\n")
        _help(script)
        sys.exit(1)


if __name__ == "__main__":
    main()

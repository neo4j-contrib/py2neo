#!/usr/bin/env python
# -*- coding: utf-8 -*-

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

from py2neo.util import ustr
from py2neo.packages.httpstream import download as _download


DIST_SCHEME = "http"
DIST_HOST = "dist.neo4j.org"

USAGE = """\
Usage:
    {0} «edition» «version» [«path»]
"""


def dist_name(edition, version):
    return "neo4j-%s-%s" % (edition, version)


def dist_archive_name(edition, version):
    return "%s-unix.tar.gz" % dist_name(edition, version)


def download(edition, version, path="."):
    archive_name = dist_archive_name(edition, version)
    uri = "%s://%s/%s" % (DIST_SCHEME, DIST_HOST, archive_name)
    filename = os.path.join(os.path.abspath(path), archive_name)
    _download(uri, filename)
    return filename


def usage(script):
    print(USAGE.format(os.path.basename(script)))


def main(script, *args):
    if args:
        if len(args) == 3:
            edition, version, path = args
            download(edition, version, path)
        elif len(args) == 2:
            edition, version = args
            download(edition, version)
        else:
            usage(script)
    else:
        usage(script)


if __name__ == "__main__":
    try:
        main(*sys.argv)
    except Exception as error:
        sys.stderr.write(ustr(error))
        sys.stderr.write("\n")
        sys.exit(1)

#!/usr/bin/env python
# coding: utf-8

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


from os import getenv, makedirs
from os.path import exists as path_exists, isfile, join as path_join
from re import compile as re_compile

from py2neo.compat import urlretrieve


version_string_pattern = re_compile("(\d+)\.(\d+)\.(\d+)-?(.*)")


editions = [
    "community",
    "enterprise",
]

versions = [

    # 2.0 series
    "2.0.0", "2.0.1", "2.0.2", "2.0.3", "2.0.4", "2.0.5",

    # 2.1 series
    "2.1.2", "2.1.3", "2.1.4", "2.1.5", "2.1.6", "2.1.7", "2.1.8",

    # 2.2 series
    "2.2.0", "2.2.1", "2.2.2", "2.2.3", "2.2.4", "2.2.5", "2.2.6", "2.2.7", "2.2.8", "2.2.8", "2.2.10",

    # 2.3 series
    "2.3.0", "2.3.1", "2.3.2", "2.3.3", "2.3.4", "2.3.5", "2.3.6", "2.3.7", "2.3.8", "2.3.9", "2.3.10", "2.3.11",
    "2.3.12",

    # 3.0 series
    "3.0.0", "3.0.1", "3.0.2", "3.0.3", "3.0.4", "3.0.5", "3.0.6", "3.0.7", "3.0.8", "3.0.9", "3.0.10", "3.0.11",
    "3.0.12",

    # 3.1 series
    "3.1.0", "3.1.1", "3.1.2", "3.1.3", "3.1.4", "3.1.5", "3.1.6", "3.1.7", "3.1.8",

    # 3.2 series
    "3.2.0", "3.2.1", "3.2.2", "3.2.3",

    # 3.3 series
    "3.3.0", "3.3.1", "3.3.2", "3.3.3", "3.3.4",

    # 3.4 series
    "3.4.0-alpha10",

]


class Version(tuple):

    def __new__(cls, string):
        return tuple.__new__(cls, version_string_pattern.match(string).groups())

    def __str__(self):
        return ".".join(self[:3]) + "".join("-%s" % part for part in self[3:] if part)

    def __eq__(self, other):
        return (int(self[0]), int(self[1]), int(self[2]), self[3]) == (int(other[0]), int(other[1]), int(other[2]), other[3])

    def __ne__(self, other):
        return not self.__eq__(other)

    def __lt__(self, other):
        return (int(self[0]), int(self[1]), int(self[2]), self[3]) < (int(other[0]), int(other[1]), int(other[2]), other[3])

    @property
    def major(self):
        return int(self[0])

    @property
    def minor(self):
        return int(self[1])

    @property
    def patch(self):
        return int(self[2])

# TODO: make this a bit easier to read
version_tuples = [Version(v) for v in versions]
minor_version_tuples = sorted({(str(v.major), str(v.minor)) for v in version_tuples})
minor_versions = [".".join(map(str, v)) for v in minor_version_tuples]
latest_version_tuples = {w: sorted(v for v in version_tuples if v[:2] == w)[-1] for w in minor_version_tuples}
latest_versions = {".".join(map(str, k)): str(v) for k, v in latest_version_tuples.items()}
version_aliases = dict(latest_versions, **{k + "-LATEST": v for k, v in latest_versions.items()})
version_aliases["LATEST"] = versions[-1]

dist = "http://{}".format(getenv("DIST_HOST") or "dist.neo4j.org")
dist_overrides = {
    # "3.0.0-NIGHTLY": "http://alpha.neohq.net/dist",
}

archive_format = getenv("ARCHIVE_FORMAT") or "gz"


class Distribution(object):
    """ Represents a Neo4j archive.
    """

    def __init__(self, edition=None, version=None):
        edition = edition.lower() if edition else "community"
        if edition in editions:
            self.edition = edition
        else:
            raise ValueError("Unknown edition %r" % edition)
        version = version.upper() if version else "LATEST"
        self.snapshot = "SNAPSHOT" in version
        if version in version_aliases:
            version = version_aliases[version]
        if version in versions:
            self.version = version
        else:
            raise ValueError("Unknown version %r" % version)

    @property
    def key(self):
        """ The unique key that identifies the archive, e.g.
        ``community-2.3.2``.
        """
        return "%s-%s" % (self.edition, self.version)

    @property
    def name(self):
        """ The full name of the archive file, e.g.
        ``neo4j-community-2.3.2-unix.tar.gz``.
        """
        return "neo4j-{}-unix.tar.{}".format(self.key, archive_format)

    @property
    def uri(self):
        """ The URI from which this archive may be downloaded, e.g.
        ``http://dist.neo4j.org/neo4j-community-2.3.2-unix.tar.gz``.
        """
        if self.version in dist_overrides:
            return "%s/%s" % (dist_overrides[self.version], self.name)
        else:
            return "%s/%s" % (dist, self.name)

    def download(self, path=".", overwrite=False):
        """ Download a Neo4j distribution to the specified path.

        :param path:
        :param overwrite:
        :return: the name of the downloaded file
        """
        file_name = path_join(path, self.name)
        if overwrite:
            if path_exists(file_name) and not isfile(file_name):
                raise IOError("Cannot overwrite directory %r" % file_name)
        elif not self.snapshot and path_exists(file_name):
            return file_name
        try:
            makedirs(path)
        except OSError:
            pass
        print("Downloading <%s>" % self.uri)
        urlretrieve(self.uri, file_name)
        return file_name

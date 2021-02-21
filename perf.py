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
from timeit import Timer

from py2neo import Graph
from py2neo.client import ConnectionProfile
from py2neo.compat import argument


class Performer(object):

    def __init__(self, profile, setup=None):
        self.profile = profile
        self.setup = setup

    def run_setup_task(self):
        pass

    def run_measured_task(self):
        pass

    def run(self):
        timer = Timer(self.run_measured_task, setup=self.run_setup_task)
        return min(timer.repeat(repeat=3, number=1))


class ResultIterationPerformer(Performer):

    def __init__(self, profile, setup=None):
        super(ResultIterationPerformer, self).__init__(profile, setup=setup)
        self.graph = Graph(self.profile)
        self.result = None

    def run_setup_task(self):
        if callable(self.setup):
            self.setup(self)

    def run_measured_task(self):
        for _ in self.result:
            pass


@argument("-u", "--uri",
          help="Set the connection URI.")
@argument("-a", "--auth", metavar="USER:PASSWORD",
          help="Set the user and password.")
@argument("-r", "--routing", action="store_true", default=False,
          help="Enable connection routing.")
@argument("-s", "--secure", action="store_true", default=False,
          help="Use encrypted communication (TLS).")
def run(uri, auth=None, routing=False, secure=False):
    """ Run one or more Cypher queries through the client console, or
    open the console for interactive use if no queries are specified.
    """
    profile = ConnectionProfile(uri, auth=auth, secure=secure)
    g = Graph(profile, routing=routing)
    result = [()]

    def iterate_result():
        for _ in result[0]:
            pass

    scale = 250000
    print("Scale = {}".format(scale))

    ##########
    # TEST 1 #
    ##########

    def run_long_narrow(p):
        p.result = p.graph.run("UNWIND range(1, $n) AS _ "
                               "RETURN 0", n=scale)

    print("Iterating long narrow result... ", end="", flush=True)
    print("{:.03f}s".format(ResultIterationPerformer(profile, run_long_narrow).run()))

    ##########
    # TEST 2 #
    ##########

    def run_long_triple_string(p):
        p.result = p.graph.run("UNWIND range(1, $n) AS _ RETURN 'aaaaaaaaa' AS a, "
                               "'bbbbbbbbb' AS b, 'ccccccccc' AS c", n=scale)

    print("Iterating long triple string result... ", end="", flush=True)
    print("{:.03f}s".format(ResultIterationPerformer(profile, run_long_triple_string).run()))

    ##########
    # TEST 3 #
    ##########

    def run_long_multi_type(p):
        p.result = p.graph.run("UNWIND range(1, $n) AS _ RETURN null, true, 0, 3.14, "
                               "'Abc', [1, 2, 3], {one: 1, two: 2, three: 3}", n=scale)

    print("Iterating long multi-type result... ", end="", flush=True)
    print("{:.03f}s".format(ResultIterationPerformer(profile, run_long_multi_type).run()))


def main():
    parser = ArgumentParser(description=getdoc(run))
    for a, kw in run.arguments:
        parser.add_argument(*a, **kw)

    args = parser.parse_args()
    kwargs = vars(args)
    run(**kwargs)


if __name__ == "__main__":
    main()

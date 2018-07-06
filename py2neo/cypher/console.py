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


from os import environ
from subprocess import call
from sys import argv, exit

from jupyter_client.kernelspec import KernelSpecManager

from py2neo.cypher.kernel import KERNEL_NAME, install_kernel


def main():
    if KERNEL_NAME not in KernelSpecManager().find_kernel_specs():
        install_kernel()
    command = "jupyter-console --kernel %s" % KERNEL_NAME
    for arg in argv[1:]:
        command += " \"{}\"".format(arg)
    exit(call(command, env=environ.copy(), shell=True))


if __name__ == "__main__":
    main()

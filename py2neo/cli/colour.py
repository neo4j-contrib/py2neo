#!/usr/bin/env python
# -*- encoding: utf-8 -*-

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


def black(s):
    return "\x1b[30m{:s}\x1b[0m".format(s)


def red(s):
    return "\x1b[31m{:s}\x1b[0m".format(s)


def green(s):
    return "\x1b[32m{:s}\x1b[0m".format(s)


def yellow(s):
    return "\x1b[33m{:s}\x1b[0m".format(s)


def blue(s):
    return "\x1b[34m{:s}\x1b[0m".format(s)


def magenta(s):
    return "\x1b[35m{:s}\x1b[0m".format(s)


def cyan(s):
    return "\x1b[36m{:s}\x1b[0m".format(s)


def white(s):
    return "\x1b[37m{:s}\x1b[0m".format(s)


def bright_black(s):
    return "\x1b[30;1m{:s}\x1b[0m".format(s)


def bright_red(s):
    return "\x1b[31;1m{:s}\x1b[0m".format(s)


def bright_green(s):
    return "\x1b[32;1m{:s}\x1b[0m".format(s)


def bright_yellow(s):
    return "\x1b[33;1m{:s}\x1b[0m".format(s)


def bright_blue(s):
    return "\x1b[34;1m{:s}\x1b[0m".format(s)


def bright_magenta(s):
    return "\x1b[35;1m{:s}\x1b[0m".format(s)


def bright_cyan(s):
    return "\x1b[36;1m{:s}\x1b[0m".format(s)


def bright_white(s):
    return "\x1b[37;1m{:s}\x1b[0m".format(s)

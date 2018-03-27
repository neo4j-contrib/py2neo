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


import logging
from sys import stderr


class ColourFormatter(logging.Formatter):
    """ Colour formatter for pretty log output.
    """

    def format(self, record):
        s = super(ColourFormatter, self).format(record)
        return "\x1b[36m%s\x1b[0m" % s


class Watcher(object):
    """ Log watcher for monitoring driver and protocol activity.
    """

    handlers = {}

    def __init__(self, logger_name):
        super(Watcher, self).__init__()
        self.logger_name = logger_name
        self.logger = logging.getLogger(self.logger_name)
        self.formatter = ColourFormatter("%(asctime)s  %(message)s")

    def __enter__(self):
        self.watch()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()

    def watch(self, level=logging.INFO, out=stderr):
        self.stop()
        handler = logging.StreamHandler(out)
        handler.setFormatter(self.formatter)
        self.handlers[self.logger_name] = handler
        self.logger.addHandler(handler)
        self.logger.setLevel(level)

    def stop(self):
        try:
            self.logger.removeHandler(self.handlers[self.logger_name])
        except KeyError:
            pass


def watch(logger_name, level=logging.INFO, out=stderr):
    """ Quick wrapper for using the Watcher.

    :param logger_name: name of logger to watch
    :param level: minimum log level to show (default INFO)
    :param out: where to send output (default stdout)
    :return: Watcher instance
    """
    watcher = Watcher(logger_name)
    watcher.watch(level, out)
    return watcher

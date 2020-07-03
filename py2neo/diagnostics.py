#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright 2011-2020, Nigel Small
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


from logging import CRITICAL, ERROR, WARNING, INFO, DEBUG, Formatter, StreamHandler, getLogger
from sys import stderr


class ColourFormatter(Formatter):
    """ Colour formatter for pretty log output.
    """

    def format(self, record):
        from pansi import ansi
        s = super(ColourFormatter, self).format(record)
        if record.levelno == CRITICAL:
            return "{RED}{}{_}".format(s, **ansi)
        elif record.levelno == ERROR:
            return "{red}{}{_}".format(s, **ansi)
        elif record.levelno == WARNING:
            return "{yellow}{}{_}".format(s, **ansi)
        elif record.levelno == INFO:
            return "{white}{}{_}".format(s, **ansi)
        elif record.levelno == DEBUG:
            return "{cyan}{}{_}".format(s, **ansi)
        else:
            return s


class Watcher(object):
    """ Log watcher for monitoring driver and protocol activity.
    """

    handlers = {}

    def __init__(self, *logger_names):
        super(Watcher, self).__init__()
        self.logger_names = logger_names
        self.loggers = [getLogger(name) for name in self.logger_names]
        self.formatter = ColourFormatter("%(asctime)s  %(message)s")

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()

    def start(self, verbosity=0, out=stderr):
        self.stop()
        handler = StreamHandler(out)
        handler.setFormatter(self.formatter)
        for logger in self. loggers:
            self.handlers[logger.name] = handler
            logger.addHandler(handler)
            if verbosity > 0:
                logger.setLevel(DEBUG)
            elif verbosity == 0:
                logger.setLevel(INFO)
            elif verbosity == -1:
                logger.setLevel(WARNING)
            elif verbosity == -2:
                logger.setLevel(ERROR)
            else:
                logger.setLevel(CRITICAL)

    def stop(self):
        try:
            for logger in self.loggers:
                logger.removeHandler(self.handlers[logger.name])
        except KeyError:
            pass


def watch(logger_name, verbosity=0, out=stderr):
    """ Quick wrapper for using the Watcher.

    :param logger_name: name of logger to watch
    :param verbosity:
    :param out: where to send output (default stderr)
    :return: Watcher instance
    """
    watcher = Watcher(logger_name)
    watcher.start(verbosity, out)
    return watcher

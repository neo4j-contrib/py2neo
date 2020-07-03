#!/usr/bin/env python
# coding: utf-8

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


from __future__ import print_function

from logging import getLogger, Formatter, StreamHandler, \
    DEBUG, INFO, WARNING, ERROR, CRITICAL
from os.path import expanduser, join, isfile
from readline import read_history_file, write_history_file
from sys import stdout

from pansi import ansi
from six.moves import input


class ConsoleLogFormatter(Formatter):
    """ Colour formatter for pretty log output.
    """

    def formatTime(self, record, datefmt=None):
        s = super(ConsoleLogFormatter, self).formatTime(record, datefmt)
        return "{BLACK}{}{_}".format(s, **ansi)

    def formatMessage(self, record):
        if record.levelno == DEBUG:
            record.message = "{cyan}{}{_}".format(record.message, **ansi)
        elif record.levelno == WARNING:
            record.message = "{yellow}{}{_}".format(record.message, **ansi)
        elif record.levelno == ERROR:
            record.message = "{red}{}{_}".format(record.message, **ansi)
        elif record.levelno == CRITICAL:
            record.message = "{RED}{}{_}".format(record.message, **ansi)
        return super(ConsoleLogFormatter, self).formatMessage(record)


class Console(object):

    prompt = "{BLACK}->{_} ".format(**ansi)

    def __init__(self, name, history=None, time_format=None):
        self.__name = name
        self.__handler = StreamHandler(stdout)
        if time_format is None:
            self.__formatter = ConsoleLogFormatter("%(message)s")
        else:
            self.__formatter = ConsoleLogFormatter("%(asctime)s  %(message)s", time_format)
        self.__handler.setFormatter(self.__formatter)
        self.__log = getLogger(self.__name)
        self.__verbosity = 0
        self.__status = 0
        self.__history = history or expanduser(join("~", ".%s.history" % name))
        self.__log.addHandler(self.__handler)

    @property
    def name(self):
        return self.__name

    @property
    def verbosity(self):
        return self.__verbosity

    @verbosity.setter
    def verbosity(self, value):
        self.__verbosity = value
        if self.__verbosity >= 1:
            self.__log.setLevel(DEBUG)
        elif self.__verbosity == 0:
            self.__log.setLevel(INFO)
        elif self.__verbosity == -1:
            self.__log.setLevel(WARNING)
        elif self.__verbosity == -2:
            self.__log.setLevel(ERROR)
        else:
            self.__log.setLevel(CRITICAL)

    def write(self, *values, sep=" ", end="\n"):
        if self.verbosity >= 0:
            print(*values, sep=sep, end=end, file=stdout)

    def debug(self, msg, *args, **kwargs):
        self.__log.debug(msg, *args, **kwargs)

    def info(self, msg, *args, **kwargs):
        self.__log.info(msg, *args, **kwargs)

    def warning(self, msg, *args, **kwargs):
        self.__log.warning(msg, *args, **kwargs)

    def error(self, msg, *args, **kwargs):
        self.__log.error(msg, *args, **kwargs)

    def critical(self, msg, *args, **kwargs):
        self.__log.critical(msg, *args, **kwargs)

    def exception(self, msg, *args, **kwargs):
        self.__log.exception(msg, *args, **kwargs)

    def loop(self):
        if isfile(self.__history):
            read_history_file(self.__history)
        while True:
            try:
                line = self.read()
            except KeyboardInterrupt:   # Ctrl+C
                print()
                continue
            except EOFError:            # Ctrl+D
                print()
                break
            else:
                self.process(line)
        write_history_file(self.__history)

    def read(self):
        """ Get input.
        """
        return input(self.prompt)

    def process(self, line):
        """ Handle input.
        """
        if line:
            self.write("OK")

    def exit(self, status=None):
        self.__log.removeHandler(self.__handler)
        if status is not None:
            self.__status = status
        raise SystemExit(self.__status)


def main():
    console = Console("test")
    console.verbosity = 1
    console.loop()
    console.exit()


if __name__ == "__main__":
    main()

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


class Wire:
    """ Socket wrapper for reading and writing bytes.
    """

    __closed = False

    __broken = False

    def __init__(self, s):
        self.__socket = s
        self.__input = bytearray()
        self.__output = bytearray()

    def read(self, n):
        while len(self.__input) < n:
            try:
                received = self.__socket.recv(n)
            except OSError:
                self.__broken = True
                raise
            else:
                if received:
                    self.__input.extend(received)
                else:
                    self.__broken = True
                    raise OSError("Network read incomplete "
                                  "(received %d of %d bytes)" %
                                  (len(self.__input), n))
        data = self.__input[:n]
        self.__input[:n] = []
        return data

    def write(self, b):
        self.__output.extend(b)

    def send(self):
        if self.__closed:
            raise OSError("Closed")
        sent = 0
        while self.__output:
            try:
                n = self.__socket.send(self.__output)
            except OSError:
                self.__broken = True
                raise
            else:
                self.__output[:n] = []
                sent += n
        return sent

    def close(self):
        try:
            self.__socket.close()
        except OSError:
            self.__broken = True
            raise
        else:
            self.__closed = True

    @property
    def closed(self):
        return self.__closed

    @property
    def broken(self):
        return self.__broken

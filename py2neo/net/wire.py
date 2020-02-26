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


class Breakable:
    """ Mixin for objects that can break, resulting in an unusable,
    unrecoverable state.
    """

    __broken = False

    def set_broken(self):
        self.__broken = True

    @property
    def broken(self):
        return self.__broken


class ByteReader(Breakable, object):

    def __init__(self, s):
        self.socket = s
        self.buffer = bytearray()

    def read(self, n):
        while len(self.buffer) < n:
            try:
                received = self.socket.recv(n)
            except OSError:
                self.set_broken()
                raise
            else:
                if received:
                    self.buffer.extend(received)
                else:
                    self.set_broken()
                    raise OSError("Network read incomplete "
                                  "(received %d of %d bytes)" % (len(self.buffer), n))
        data = self.buffer[:n]
        self.buffer[:n] = []
        return data


class ByteWriter(Breakable, object):

    def __init__(self, s):
        self.__socket = s
        self.__buffer = bytearray()
        self.__closed = False

    def write(self, b):
        self.__buffer.extend(b)

    def send(self):
        if self.__closed:
            raise OSError("Closed")
        sent = 0
        while self.__buffer:
            try:
                n = self.__socket.send(self.__buffer)
            except OSError:
                self.set_broken()
                raise
            else:
                self.__buffer[:n] = []
                sent += n
        return sent

    def close(self):
        try:
            self.__socket.close()
        except OSError:
            self.set_broken()
            raise
        else:
            self.__closed = True

    @property
    def closed(self):
        return self.__closed

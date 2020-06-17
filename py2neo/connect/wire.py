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


from socket import socket, SOL_SOCKET, SO_KEEPALIVE

from py2neo.connect.addressing import Address


class Wire(object):
    """ Socket wrapper for reading and writing bytes.
    """

    __closed = False

    __broken = False

    @classmethod
    def open(cls, address, timeout=None, keep_alive=False):
        s = socket(family=address.family)
        if keep_alive:
            s.setsockopt(SOL_SOCKET, SO_KEEPALIVE, 1)
        s.settimeout(timeout)
        s.connect(address)
        return cls(s)

    def __init__(self, s):
        s.settimeout(None)  # ensure wrapped socket is in blocking mode
        self.__socket = s
        self.__input = bytearray()
        self.__output = bytearray()

    def secure(self, verify=True, hostname=None):
        from ssl import SSLContext, PROTOCOL_TLS, CERT_NONE, CERT_REQUIRED
        context = SSLContext(PROTOCOL_TLS)
        if verify:
            context.verify_mode = CERT_REQUIRED
            context.check_hostname = bool(hostname)
        else:
            context.verify_mode = CERT_NONE
        context.load_default_certs()
        try:
            self.__socket = context.wrap_socket(self.__socket, server_hostname=hostname)
        except (IOError, OSError):
            # TODO: add connection failure/diagnostic callback
            raise WireError("Unable to establish secure connection with remote peer")

    def read(self, n):
        while len(self.__input) < n:
            try:
                received = self.__socket.recv(n - len(self.__input))
            except (IOError, OSError):
                self.__broken = True
                raise WireError("Broken")
            else:
                if received:
                    self.__input.extend(received)
                else:
                    self.__broken = True
                    raise WireError("Network read incomplete "
                                    "(received %d of %d bytes)" %
                                    (len(self.__input), n))
        data = self.__input[:n]
        self.__input[:n] = []
        return data

    def write(self, b):
        self.__output.extend(b)

    def send(self):
        if self.__closed:
            raise WireError("Closed")
        sent = 0
        while self.__output:
            try:
                n = self.__socket.send(self.__output)
            except (IOError, OSError):
                self.__broken = True
                raise WireError("Broken")
            else:
                self.__output[:n] = []
                sent += n
        return sent

    def close(self):
        try:
            self.__socket.close()
        except (IOError, OSError):
            self.__broken = True
            raise WireError("Broken")
        else:
            self.__closed = True

    @property
    def closed(self):
        return self.__closed

    @property
    def broken(self):
        return self.__broken

    @property
    def local_address(self):
        return Address(self.__socket.getsockname())

    @property
    def remote_address(self):
        return Address(self.__socket.getpeername())


class WireError(OSError):

    pass

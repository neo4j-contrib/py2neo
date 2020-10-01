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


"""
Low-level module for network communication.

This module provides a convenience socket wrapper class (:class:`.Wire`)
as well as classes for modelling IP addresses, based on tuples.
"""


from socket import (
    getservbyname,
    socket,
    SOL_SOCKET,
    SO_KEEPALIVE,
    AF_INET,
    AF_INET6,
)
from socketserver import BaseRequestHandler

from py2neo.compat import xstr


BOLT_PORT_NUMBER = 7687


class Address(tuple):
    """ Address of a machine on a network.
    """

    @classmethod
    def parse(cls, s, default_host=None, default_port=None):
        s = xstr(s)
        if not isinstance(s, str):
            raise TypeError("Address.parse requires a string argument")
        if s.startswith("["):
            # IPv6
            host, _, port = s[1:].rpartition("]")
            port = port.lstrip(":")
            try:
                port = int(port)
            except (TypeError, ValueError):
                pass
            return cls((host or default_host or "localhost",
                        port or default_port or 0, 0, 0))
        else:
            # IPv4
            host, _, port = s.partition(":")
            try:
                port = int(port)
            except (TypeError, ValueError):
                pass
            return cls((host or default_host or "localhost",
                        port or default_port or 0))

    def __new__(cls, iterable):
        if isinstance(iterable, cls):
            return iterable
        n_parts = len(iterable)
        inst = tuple.__new__(cls, iterable)
        if n_parts == 2:
            inst.__class__ = IPv4Address
        elif n_parts == 4:
            inst.__class__ = IPv6Address
        else:
            raise ValueError("Addresses must consist of either "
                             "two parts (IPv4) or four parts (IPv6)")
        return inst

    #: Address family (AF_INET or AF_INET6)
    family = None

    def __repr__(self):
        return "{}({!r})".format(self.__class__.__name__, tuple(self))

    @property
    def host(self):
        return self[0]

    @property
    def port(self):
        return self[1]

    @property
    def port_number(self):
        if self.port == "bolt":
            # Special case, just because. The regular /etc/services
            # file doesn't contain this, but it can be found in
            # /usr/share/nmap/nmap-services if nmap is installed.
            return BOLT_PORT_NUMBER
        try:
            return getservbyname(self.port)
        except (OSError, TypeError):
            # OSError: service/proto not found
            # TypeError: getservbyname() argument 1 must be str, not X
            try:
                return int(self.port)
            except (TypeError, ValueError) as e:
                raise type(e)("Unknown port value %r" % self.port)


class IPv4Address(Address):
    """ Address subclass, specifically for IPv4 addresses.
    """

    family = AF_INET

    def __str__(self):
        return "{}:{}".format(*self)


class IPv6Address(Address):
    """ Address subclass, specifically for IPv6 addresses.
    """

    family = AF_INET6

    def __str__(self):
        return "[{}]:{}".format(*self)


class Wire(object):
    """ Buffered socket wrapper for reading and writing bytes.
    """

    __closed = False

    __broken = False

    @classmethod
    def open(cls, address, timeout=None, keep_alive=False):
        """ Open a connection to a given network :class:`.Address`.
        """
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
        """ Apply a layer of security onto this connection.
        """
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
        """ Read bytes from the network.
        """
        while len(self.__input) < n:
            required = n - len(self.__input)
            requested = max(required, 8192)
            try:
                received = self.__socket.recv(requested)
            except (IOError, OSError):
                self.__broken = True
                raise BrokenWireError("Broken")
            else:
                if received:
                    self.__input.extend(received)
                else:
                    self.__broken = True
                    raise BrokenWireError("Network read incomplete "
                                          "(received %d of %d bytes)" %
                                          (len(self.__input), n))
        data = self.__input[:n]
        self.__input[:n] = []
        return data

    def write(self, b):
        """ Write bytes to the output buffer.
        """
        self.__output.extend(b)

    def send(self):
        """ Send the contents of the output buffer to the network.
        """
        if self.__closed:
            raise WireError("Closed")
        sent = 0
        while self.__output:
            try:
                n = self.__socket.send(self.__output)
            except (IOError, OSError):
                self.__broken = True
                raise BrokenWireError("Broken")
            else:
                self.__output[:n] = []
                sent += n
        return sent

    def close(self):
        """ Close the connection.
        """
        try:
            # TODO: shutdown
            self.__socket.close()
        except (IOError, OSError):
            self.__broken = True
            raise BrokenWireError("Broken")
        else:
            self.__closed = True

    @property
    def closed(self):
        """ Flag indicating whether this connection has been closed locally.
        """
        return self.__closed

    @property
    def broken(self):
        """ Flag indicating whether this connection has been closed remotely.
        """
        return self.__broken

    @property
    def local_address(self):
        """ The local :class:`.Address` to which this connection is bound.
        """
        return Address(self.__socket.getsockname())

    @property
    def remote_address(self):
        """ The remote :class:`.Address` to which this connection is bound.
        """
        return Address(self.__socket.getpeername())


class WireRequestHandler(BaseRequestHandler):
    """ Base handler for use with the `socketserver` module that wraps
    the request attribute as a :class:`.Wire` object.
    """

    __wire = None

    @property
    def wire(self):
        if self.__wire is None:
            self.__wire = Wire(self.request)
        return self.__wire


class WireError(OSError):
    """ Raised when a connection error occurs.
    """


class BrokenWireError(WireError):
    """ Raised when a connection is broken by the network or remote peer.
    """

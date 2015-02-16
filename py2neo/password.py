#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright 2011-2014, Nigel Small
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

import base64
import os
import sys

from py2neo import GraphError, Service, ServiceRoot
from py2neo.env import NEO4J_URI
from py2neo.packages.httpstream.numbers import UNPROCESSABLE_ENTITY
from py2neo.util import ustr


HELP = """\
Usage: {script} «user_name» «password» [«new_password»]

Authenticate against a Neo4j database server, optionally changing
the password. If no new password is supplied, a simple auth check
is carried out and a response is output describing whether or not
a password change is required. If the new password is supplied, a
password change is attempted.

Report bugs to nigel@py2neo.org
"""


def auth_header_value(user_name, password, realm=None):
    """ Construct a value for the Authorization header based on the
    credentials supplied.

    :param user_name: the user name
    :param password: the password
    :param realm: the realm (optional)
    :return: string to be included in Authorization header
    """
    credentials = (user_name + ":" + password).encode("UTF-8")
    if realm:
        value = 'Basic realm="' + realm + '" '
    else:
        value = 'Basic '
    value += base64.b64encode(credentials).decode("ASCII")
    return value


class UserManager(Service):
    """ User management service.
    """

    @classmethod
    def for_user(cls, service_root, user_name, password):
        """ Fetch a UserManager instance for a given service root and user name.

        :param service_root: A valid :class:`py2neo.ServiceRoot` instance.
        :rtype: :class:`.UserManager`
        """
        uri = service_root.uri.resolve("/user/%s" % user_name)
        inst = cls(uri)
        inst.resource.headers["Authorization"] = auth_header_value(user_name, password, "Neo4j")
        return inst

    __instances = {}

    #: Instance metadata, updated on refresh.
    metadata = None

    def __new__(cls, uri):
        try:
            inst = cls.__instances[uri]
        except KeyError:
            inst = super(UserManager, cls).__new__(cls)
            inst.bind(uri)
            cls.__instances[uri] = inst
        return inst

    def refresh(self):
        """ Refresh the stored metadata.
        """
        rs = self.resource.get()
        self.metadata = rs.content

    @property
    def user_name(self):
        self.refresh()
        return self.metadata["username"]

    @property
    def password_manager(self):
        self.refresh()
        password_manager = PasswordManager(self.metadata["password_change"])
        password_manager.resource.headers["Authorization"] = self.resource.headers["Authorization"]
        return password_manager

    @property
    def password_change_required(self):
        self.refresh()
        return self.metadata["password_change_required"]


class PasswordManager(Service):
    """ Password management service.
    """

    __instances = {}

    def __new__(cls, uri):
        try:
            inst = cls.__instances[uri]
        except KeyError:
            inst = super(PasswordManager, cls).__new__(cls)
            inst.bind(uri)
            cls.__instances[uri] = inst
        return inst

    def change(self, new_password):
        """ Change the authentication password.

        :arg password: The current password.
        :arg new_password: A new password (must not match the current password).
        :return: A valid auth token.
        :raise ValueError: If the new password is invalid.
        """
        try:
            response = self.resource.post({"password": new_password})
        except GraphError as error:
            if error.response.status_code == UNPROCESSABLE_ENTITY:
                raise ValueError("Cannot change password")
            else:
                raise
        else:
            return response.status_code == 200


def _help(script):
    print(HELP.format(script=os.path.basename(script)))


def main():
    script, args = sys.argv[0], sys.argv[1:]
    if len(args) < 2 or len(args) > 3:
        _help(script)
        return
    try:
        service_root = ServiceRoot(NEO4J_URI)
        user_name = args[0]
        password = args[1]
        user_manager = UserManager.for_user(service_root, user_name, password)
        if len(args) == 2:
            # Check password
            if user_manager.password_change_required:
                print("Password change required")
            else:
                print("Password change not required")
        else:
            # Change password
            password_manager = user_manager.password_manager
            new_password = args[2]
            if password_manager.change(new_password):
                print("Password change succeeded")
            else:
                print("Password change failed")
    except Exception as error:
        sys.stderr.write("%s: %s\n" % (error.__class__.__name__, ustr(error)))
        sys.exit(1)


if __name__ == "__main__":
    main()

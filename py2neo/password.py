#!/usr/bin/env python
# -*- encoding: utf-8 -*-

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

from py2neo import GraphError, Resource, DBMS
from py2neo.compat import ustr
from py2neo.env import NEO4J_URI
from py2neo.packages.httpstream.numbers import UNPROCESSABLE_ENTITY


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


class UserManager(object):
    """ User management service.
    """

    @classmethod
    def for_user(cls, dbms, user_name, password):
        """ Fetch a UserManager instance for a given DBMS and user name.

        :param dbms: A valid :class:`py2neo.DBMS` instance.
        :rtype: :class:`.UserManager`
        """
        uri = dbms.uri.resolve("/user/%s" % user_name)
        inst = cls(uri)
        inst.resource._headers["Authorization"] = auth_header_value(user_name, password, "Neo4j")
        return inst

    #: Instance metadata, updated on refresh.
    metadata = None

    def __init__(self, uri):
        self.resource = Resource(uri)

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
        password_manager.resource._headers["Authorization"] = \
            self.resource._headers["Authorization"]
        return password_manager

    @property
    def password_change_required(self):
        self.refresh()
        return self.metadata["password_change_required"]


class PasswordManager(object):
    """ Password management service.
    """

    def __init__(self, uri):
        self.resource = Resource(uri)

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
        dbms = DBMS(NEO4J_URI)
        user_name = args[0]
        password = args[1]
        user_manager = UserManager.for_user(dbms, user_name, password)
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
                sys.exit(2)
    except Exception as error:
        sys.stderr.write("%s: %s\n" % (error.__class__.__name__, ustr(error)))
        sys.exit(1)


if __name__ == "__main__":
    main()

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

import os
import sys

from py2neo import GraphError, Resource, Service, ServiceRoot
from py2neo.env import NEO4J_URI
from py2neo.packages.httpstream.numbers import UNPROCESSABLE_ENTITY
from py2neo.util import ustr


HELP = """\
Usage: {script} «user_name» «password» [«new_password»]

Authenticate against a Neo4j database server, optionally changing
the password.

Report bugs to nigel@py2neo.org
"""


class AuthenticationError(GraphError):
    """ Authentication failed with the supplied credentials.
    """


class PasswordChangeRequired(GraphError):
    """ A new password is required before authentication can occur.
    """


class Authentication(Service):
    """ Authentication management service.
    """

    @classmethod
    def for_service(cls, service_root):
        """ Fetch an Authentication instance for a given service root.

        :param service_root: A valid :class:`py2neo.ServiceRoot` instance.
        :rtype: :class:`.Authentication`
        """
        try:
            return cls(service_root.resource.metadata["authentication"])
        except KeyError:
            raise NotImplementedError("Authentication is not required for the service "
                                      "at %r" % service_root.uri.string)

    __instances = {}

    #: Instance metadata, updated on refresh.
    metadata = None

    def __new__(cls, uri):
        try:
            inst = cls.__instances[uri]
        except KeyError:
            inst = super(Authentication, cls).__new__(cls)
            inst.bind(uri)
            cls.__instances[uri] = inst
        return inst

    def refresh(self, user_name, password):
        """ Perform authentication to refresh the stored metadata.

        :arg user_name: Name of user to authenticate as.
        :arg password: The current password for this user.
        """
        try:
            self.metadata = self.resource.post({"username": user_name,
                                                "password": password}).content
        except GraphError as error:
            if error.response.status_code == UNPROCESSABLE_ENTITY:
                raise AuthenticationError("Cannot authenticate for user %r" % user_name)
            else:
                raise

    def authenticate(self, user_name, password, new_password=None):
        """ Authenticate to retrieve an auth token.

        :arg service_root: The :class:`py2neo.ServiceRoot` object requiring authentication.
        :arg user_name: Name of user to authenticate as.
        :arg password: The current password for this user.
        :arg new_password: A new password (optional, must not match the current password).
        :return: A valid auth token.
        :raise ValueError: If the new password supplied is invalid.
        :raise AuthenticationError: If the user cannot be authenticated.
        """
        self.refresh(user_name, password)
        if new_password is None:
            if self.metadata["password_change_required"]:
                raise PasswordChangeRequired("A password change is required for the service "
                                             "at %r" % self.service_root.uri.string)
            else:
                return self.metadata.get("authorization_token")
        else:
            password_manager = PasswordManager(self.metadata["password_change"])
            return password_manager.change(password, new_password)


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

    def change(self, password, new_password):
        """ Change the authentication password.

        :arg password: The current password.
        :arg new_password: A new password (must not match the current password).
        :return: A valid auth token.
        :raise ValueError: If the new password is invalid.
        """
        try:
            response = self.resource.post({"password": password, "new_password": new_password})
        except GraphError as error:
            if error.response.status_code == UNPROCESSABLE_ENTITY:
                raise ValueError("Cannot change password")
            else:
                raise
        else:
            return response.content.get("authorization_token")


def get_auth_token(uri, user_name, password, new_password=None):
    """ Authenticate to retrieve an auth token.

    :arg uri: The root URI for the service requiring authentication.
    :arg user_name: Name of user to authenticate as.
    :arg password: The current password for this user.
    :arg new_password: A new password (optional, must not match the current password).
    :return: A valid auth token.
    :raise AuthenticationError: If the user cannot be authenticated.
    :raise ValueError: If the new password value is invalid.

    """
    auth = Authentication.for_service(ServiceRoot(uri))
    return auth.authenticate(user_name, password, new_password)


def _help(script):
    print(HELP.format(script=os.path.basename(script)))


def main():
    script, args = sys.argv[0], sys.argv[1:]
    try:
        if args:
            if 2 <= len(args) <= 3:
                print(get_auth_token(NEO4J_URI, *args))
            else:
                _help(script)
        else:
            _help(script)
    except Exception as error:
        sys.stderr.write(ustr(error))
        sys.stderr.write("\n")
        sys.exit(1)


if __name__ == "__main__":
    main()

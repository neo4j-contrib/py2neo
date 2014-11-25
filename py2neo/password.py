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
    """ Handler for the authentication endpoint.
    """

    __instances = {}

    def __new__(cls, service_root):
        try:
            auth_uri = service_root.resource.metadata["authentication"]
        except KeyError:
            raise NotImplementedError("Authentication is not required for the service "
                                      "at %r" % service_root.uri.string)
        try:
            inst = cls.__instances[auth_uri]
        except KeyError:
            inst = super(Authentication, cls).__new__(cls)
            inst.bind(auth_uri)
            cls.__instances[auth_uri] = inst
        return inst

    def _authenticate(self, user_name, password):
        try:
            return self.resource.post({"user_name": user_name, "password": password}).content
        except GraphError as error:
            if error.response.status_code == UNPROCESSABLE_ENTITY:
                raise AuthenticationError("Cannot authenticate for user %r" % user_name)
            else:
                raise

    def change_password(self, user_name, password, new_password):
        """ Change the password for a user and retrieve an auth token.

        :arg user_name: Name of user to authenticate as.
        :arg password: The current password for this user.
        :arg new_password: A new password (must not match the current password).
        :return: A valid auth token.
        :raise AuthenticationError: If the user cannot be authenticated.
        :raise ValueError: If the new password value is invalid.

        """
        auth_metadata = self._authenticate(user_name, password)
        resource = Resource(auth_metadata["password_change"])
        try:
            response = resource.post({"password": password, "new_password": new_password})
        except GraphError as error:
            if error.response.status_code == UNPROCESSABLE_ENTITY:
                raise ValueError("Cannot change password for %r" % user_name)
            else:
                raise
        else:
            return response.content.get("authorization_token")

    def authenticate(self, user_name, password):
        """ Authenticate to retrieve an auth token.

        :arg service_root: The :class:`py2neo.ServiceRoot` object requiring authentication.
        :arg user_name: Name of user to authenticate as.
        :arg password: The current password for this user.
        :return: A valid auth token.
        :raise AuthenticationError: If the user cannot be authenticated.

        """
        auth_metadata = self._authenticate(user_name, password)
        if auth_metadata["password_change_required"]:
            raise PasswordChangeRequired("A password change is required for the service "
                                         "at %r" % self.service_root.uri.string)
        else:
            return auth_metadata.get("authorization_token")


def change_password(uri, user_name, password, new_password):
    """ Change the password for a user and retrieve an auth token.

    :arg uri: The root URI for the service requiring authentication.
    :arg user_name: Name of user to authenticate as.
    :arg password: The current password for this user.
    :arg new_password: A new password (must not match the current password).
    :return: A valid auth token.
    :raise AuthenticationError: If the user cannot be authenticated.
    :raise ValueError: If the new password value is invalid.

    """
    auth = Authentication(ServiceRoot(uri))
    return auth.change_password(user_name, password, new_password)


def authenticate(uri, user_name, password):
    """ Authenticate to retrieve an auth token.

    :arg uri: The root URI for the service requiring authentication.
    :arg user_name: Name of user to authenticate as.
    :arg password: The current password for this user.
    :return: A valid auth token.
    :raise AuthenticationError: If the user cannot be authenticated.

    """
    auth = Authentication(ServiceRoot(uri))
    return auth.authenticate(user_name, password)


def _help(script):
    print(HELP.format(script=os.path.basename(script)))


def main():
    script, args = sys.argv[0], sys.argv[1:]
    try:
        if args:
            if len(args) == 3:
                user_name, password, new_password = args
                print(change_password(NEO4J_URI, user_name, password, new_password))
            elif len(args) == 2:
                user_name, password = args
                print(authenticate(NEO4J_URI, user_name, password))
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

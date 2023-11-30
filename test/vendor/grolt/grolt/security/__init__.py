#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright (c) "Neo4j"
# Neo4j Sweden AB [https://neo4j.com]
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


from __future__ import absolute_import

from collections import namedtuple
from os import chmod, path
from tempfile import mkdtemp
from uuid import uuid4

from grolt.images import is_legacy_image, resolve_image
from grolt.security._cryptography import (make_self_signed_certificate,
                                          install_certificate,
                                          install_private_key)


Auth = namedtuple("Auth", ["user", "password"])


def make_auth(value=None, default_user=None, default_password=None):
    try:
        user, _, password = str(value or "").partition(":")
    except AttributeError:
        raise ValueError("Invalid auth string {!r}".format(value))
    else:
        return Auth(user or default_user or "neo4j",
                    password or default_password or uuid4().hex)


def install_self_signed_certificate(image):
    """ Install a self-signed certificate for the given Docker image
    and return the installation directory.
    """
    if is_legacy_image(resolve_image(image)):
        return None  # Automatically available in 3.x
    cert, key = make_self_signed_certificate()
    certificates_dir = mkdtemp()
    chmod(certificates_dir, 0o755)
    subdirectories = [path.join(certificates_dir, subdir)
                      for subdir in ["bolt", "https"]]
    install_private_key(key, "private.key", *subdirectories)
    install_certificate(cert, "public.crt", *subdirectories)
    return certificates_dir

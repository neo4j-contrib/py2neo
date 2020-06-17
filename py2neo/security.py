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


from logging import getLogger
from os import makedirs, path
from socket import gethostname

from OpenSSL import crypto


log = getLogger(__name__)


def make_self_signed_certificate():

    # create a key pair
    log.info("Generating private key")
    key = crypto.PKey()
    key.generate_key(crypto.TYPE_RSA, 1024)

    # create a self-signed cert
    log.info("Generating self-signed certificate")
    cert = crypto.X509()
    cert.get_subject().C = "UK"
    cert.get_subject().ST = "London"
    cert.get_subject().L = "London"
    cert.get_subject().O = "Neo4j"
    cert.get_subject().OU = "Engineering"
    cert.get_subject().CN = gethostname()
    cert.set_serial_number(1000)
    cert.gmtime_adj_notBefore(0)
    cert.gmtime_adj_notAfter(10*365*24*60*60)
    cert.set_issuer(cert.get_subject())
    cert.set_pubkey(key)
    cert.sign(key, 'sha1')
    return cert, key


def install_certificate(cert, name, *cert_dirs):
    for cert_dir in cert_dirs:
        try:
            makedirs(cert_dir, exist_ok=True)
        except OSError:
            pass
        cert_file = path.join(cert_dir, name)
        log.info("Installing certificate to %r", cert_file)
        open(cert_file, "wb").write(
            crypto.dump_certificate(crypto.FILETYPE_PEM, cert))


def install_private_key(key, name, *key_dirs):
    for key_dir in key_dirs:
        try:
            makedirs(key_dir, exist_ok=True)
        except OSError:
            pass
        key_file = path.join(key_dir, name)
        log.info("Installing private key to %r", key_file)
        open(key_file, "wb").write(
            crypto.dump_privatekey(crypto.FILETYPE_PEM, key))

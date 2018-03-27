#!/usr/bin/env python
# coding: utf-8

# Copyright 2011-2017, Nigel Small
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


from hashlib import sha256
from random import randint

import click

from py2neo.compat import bstr


def hex_bytes(data):
    return "".join("%02X" % b for b in bytearray(data)).encode("utf-8")


def unhex_bytes(h):
    return bytearray(int(h[i:(i + 2)], 0x10) for i in range(0, len(h), 2))


class AuthUser(object):

    #: Name of user
    user = None

    #:
    digest = None

    @classmethod
    def create(cls, user_name, password):
        inst = cls(user_name, b"SHA-256", None, None)
        inst.set_password(password)
        return inst

    @classmethod
    def load(cls, s):
        assert isinstance(s, (bytes, bytearray))
        fields = s.rstrip().split(b":")
        name = fields[0]
        hash_algorithm, digest, salt = fields[1].split(b",")
        return cls(name, hash_algorithm, unhex_bytes(digest), unhex_bytes(salt))

    @classmethod
    def match(cls, s, user_name):
        assert isinstance(s, (bytes, bytearray))
        candidate_user_name, _, _ = s.partition(b":")
        return candidate_user_name == user_name

    def dump(self, eol=b"\r\n"):
        return (b"%s:%s,%s,%s:%s" %
                (self.name, self.hash_algorithm, hex_bytes(self.digest), hex_bytes(self.salt), eol))

    def __init__(self, name, hash_algorithm, digest, salt):
        assert hash_algorithm == b"SHA-256"
        self.name = bstr(name)
        self.hash_algorithm = hash_algorithm
        self.digest = digest
        self.salt = salt

    def __repr__(self):
        return "<AuthUser name=%r>" % self.name

    def set_password(self, password):
        assert self.hash_algorithm == b"SHA-256"
        salt = bytearray(randint(0x00, 0xFF) for _ in range(16))
        m = sha256()
        m.update(salt)
        m.update(bstr(password))
        self.digest = m.digest()
        self.salt = salt

    def check_password(self, password):
        assert self.hash_algorithm == b"SHA-256"
        m = sha256()
        m.update(self.salt)
        m.update(bstr(password))
        return m.digest() == self.digest


class AuthFile(object):

    def __init__(self, name):
        self.name = name

    def __iter__(self):
        with open(self.name, "rb") as f:
            for line in f:
                yield AuthUser.load(line)

    def append(self, user_name, password):
        line = AuthUser.create(user_name, password).dump()
        if self.name == "-":
            click.echo(line.decode("utf-8"), nl=False)
        else:
            with open(self.name, "ab") as f:
                f.write(line)

    def remove(self, user_name):
        with open(self.name, "rb") as f:
            lines = [line for line in f.readlines() if not AuthUser.match(line, user_name)]
        with open(self.name, "wb") as f:
            f.writelines(lines)

    def update(self, user_name, password):
        with open(self.name, "rb") as f:
            lines = []
            for line in f.readlines():
                if AuthUser.match(line, user_name):
                    lines.append(AuthUser.create(user_name, password).dump())
                else:
                    lines.append(line)
        with open(self.name, "wb") as f:
            f.writelines(lines)


@click.group(help="""\
Tool for managing Neo4j auth files.
""")
def cli():
    pass


@cli.command(help="""\
List users in the Neo4j auth file.
""")
@click.argument("auth_file")
def list(auth_file):
    for user in AuthFile(auth_file):
        click.echo(user.name)


@cli.command(help="""\
Add a user to the Neo4j auth file.

For general interactive use, password and confirmation prompts will be presented.
For batch mode, use the --password option.

Example:

    py2neo-auth add data/dbms/auth alice

If AUTH_FILE contains only a dash `-` then the auth file entry will be written to stdout instead.
""")
@click.argument("auth_file")
@click.argument("user_name")
@click.password_option()
def add(auth_file, user_name, password):
    AuthFile(auth_file).append(user_name, password)


@cli.command(help="""\
Remove a user from the Neo4j auth file.
""")
@click.argument("auth_file")
@click.argument("user_name")
def remove(auth_file, user_name):
    AuthFile(auth_file).remove(user_name)


@cli.command(help="""\
Set the password for a user in the Neo4j auth file.

For general interactive use, password and confirmation prompts will be presented.
For batch mode, use the --password option.

Example:

    py2neo-auth update data/dbms/auth alice
""")
@click.argument("auth_file")
@click.argument("user_name")
@click.password_option()
def update(auth_file, user_name, password):
    AuthFile(auth_file).update(user_name, password)


def main():
    try:
        cli(obj={})
    except Exception as error:
        click.secho(error.args[0], err=True)
        exit(1)
    else:
        exit(0)


if __name__ == "__main__":
    main()

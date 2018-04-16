#!/usr/bin/env python
# coding: utf-8

# Copyright 2011-2018, Nigel Small
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


import click

from py2neo.admin.install import AuthFile


@click.group(help="""\
Tool for managing Neo4j installations.
""")
def cli():
    pass


@cli.command("list-users", help="""\
List users in a Neo4j auth file.
""")
@click.argument("auth_file")
def list_users(auth_file):
    for user in AuthFile(auth_file):
        click.echo(user.name)


@cli.command("del-user", help="""\
Remove a user from a Neo4j auth file.
""")
@click.argument("auth_file")
@click.argument("user_name")
def del_user(auth_file, user_name):
    AuthFile(auth_file).remove(user_name)


@cli.command("put-user", help="""\
Create or set the password for a user in a Neo4j auth file.

For general interactive use, password and confirmation prompts will be presented.
For batch mode, use the --password option.

Example:

    py2neo-admin put-user data/dbms/auth alice

If AUTH_FILE contains only a dash `-` then the auth file entry will be written to stdout instead.
""")
@click.argument("auth_file")
@click.argument("user_name")
@click.password_option()
def put_user(auth_file, user_name, password):
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

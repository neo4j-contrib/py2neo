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


from inspect import getmembers
from logging import getLogger
from shlex import split as shlex_split
from time import sleep
from webbrowser import open as open_browser

import click

# The readline import allows for extended input functionality, including
# up/down arrow navigation. This should not be removed.
#
# noinspection PyUnresolvedReferences
try:
    import readline
except ModuleNotFoundError as e:
    # readline is not available for windows 10
    from pyreadline import Readline
    readline = Readline()

log = getLogger(__name__)


class Neo4jConsole:

    args = None

    def __init__(self, service):
        self.service = service

    def __iter__(self):
        for name, value in getmembers(self):
            if isinstance(value, click.Command):
                yield name

    def __getitem__(self, name):
        try:
            f = getattr(self, name)
        except AttributeError:
            raise click.UsageError('No such command "%s".' % name)
        else:
            if isinstance(f, click.Command):
                return f
            else:
                raise click.UsageError('No such command "%s".' % name)

    def _iter_instances(self, name):
        if not name:
            name = "a"
        for instance in self.service.instances:
            if name in (instance.name, instance.fq_name):
                yield instance

    def _for_each_instance(self, name, f):
        found = 0
        for instance_obj in self._iter_instances(name):
            f(instance_obj)
            found += 1
        return found

    def prompt(self):
        # We don't use click.prompt functionality here as that doesn't play
        # nicely with readline. Instead, we use click.echo for the main prompt
        # text and a raw input call to read from stdin.
        text = "".join([
            click.style(">"),
        ])
        prompt_suffix = " "
        click.echo(text, nl=False)
        return input(prompt_suffix)

    def run(self):
        while True:
            text = self.prompt()
            if text:
                self.args = shlex_split(text)
                self.invoke(*self.args)

    def invoke(self, *args):
        try:
            arg0, args = args[0], list(args[1:])
            f = self[arg0]
            ctx = f.make_context(arg0, args, obj=self)
            return f.invoke(ctx)
        except click.exceptions.Exit:
            pass
        except click.ClickException as e:
            click.echo(e.format_message())
        except RuntimeError as e:
            log.error("{}".format(e.args[0]))

    @click.command()
    @click.argument("machine", required=False)
    @click.pass_obj
    def browser(self, instance):
        """ Start the Neo4j browser.

        A machine name may optionally be passed, which denotes the server to
        which the browser should be tied. If no machine name is given, 'a' is
        assumed.
        """

        def f(i):
            try:
                uri = "https://{}".format(i.addresses["https"])
            except KeyError:
                uri = "http://{}".format(i.addresses["http"])
            click.echo("Opening web browser for machine {!r} at "
                       "«{}»".format(i.fq_name, uri))
            open_browser(uri)

        if not self._for_each_instance(instance, f):
            raise RuntimeError("Machine {!r} not found".format(instance))

    @click.command()
    @click.pass_obj
    def env(self):
        """ Show available environment variables.

        Each service exposes several environment variables which contain
        information relevant to that service. These are:

          BOLT_SERVER_ADDR   space-separated string of router addresses
          NEO4J_AUTH         colon-separated user and password

        """
        for key, value in sorted(self.service.env().items()):
            click.echo("%s=%r" % (key, value))

    @click.command()
    @click.pass_obj
    def exit(self):
        """ Shutdown all instances and exit the console.
        """
        raise SystemExit()

    @click.command()
    @click.argument("command", required=False)
    @click.pass_obj
    def help(self, command):
        """ Get help on a command or show all available commands.
        """
        if command:
            try:
                f = self[command]
            except KeyError:
                raise RuntimeError('No such command "%s".' % command)
            else:
                ctx = self.help.make_context(command, [], obj=self)
                click.echo(f.get_help(ctx))
        else:
            click.echo("Commands:")
            command_width = max(map(len, self))
            text_width = 73 - command_width
            template = "  {:<%d}   {}" % command_width
            for arg0 in sorted(self):
                f = self[arg0]
                text = [f.get_short_help_str(limit=text_width)]
                for i, line in enumerate(text):
                    if i == 0:
                        click.echo(template.format(arg0, line))
                    else:
                        click.echo(template.format("", line))

    @click.command()
    @click.option("-r", "--refresh", is_flag=True,
                  help="Refresh the routing table")
    @click.pass_obj
    def ls(self, refresh):
        """ Show a detailed list of the available servers.

        Routing information for the current transaction context is refreshed
        automatically if expired, or can be manually refreshed with the -r
        option. Each server is listed by name, along with the following
        details:

        \b
        - Bolt port
        - HTTP port
        - Server mode: CORE, READ_REPLICA or SINGLE
        - Whether or not the server is a router
        - Roles for the current tx context: (r)ead or (w)rite
        - Docker container in which the server is running
        - Debug port

        """
        click.echo("CONTAINER   NAME        "
                   "BOLT PORT   HTTP PORT   HTTPS PORT   MODE")
        for instance in self.service.instances:
            if instance is None:
                continue
            click.echo("{:<12}{:<12}{:<12}{:<12}{:<13}{:<15}".format(
                instance.container.short_id,
                instance.fq_name,
                instance.bolt_port,
                instance.http_port,
                instance.https_port or 0,
                instance.config.get("dbms.mode", "SINGLE"),
            ))

    @click.command()
    @click.argument("instance", required=False)
    @click.pass_obj
    def logs(self, instance):
        """ Display logs for a named server.

        If no server name is provided, 'a' is used as a default.
        """

        def f(m):
            click.echo(m.container.logs())

        if not self._for_each_instance(instance, f):
            raise RuntimeError("Machine {!r} not found".format(instance))

    @click.command()
    @click.argument("time", type=float)
    @click.argument("instance", required=False)
    @click.pass_obj
    def pause(self, time, instance):
        """ Pause a server for a given number of seconds.

        If no server name is provided, 'a' is used as a default.
        """

        def f(i):
            log.info("Pausing machine {!r} for {}s".format(i.fq_name, time))
            i.container.pause()
            sleep(time)
            i.container.unpause()
            i.ping(timeout=0)

        if not self._for_each_instance(instance, f):
            raise RuntimeError("Machine {!r} not found".format(instance))

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


from logging import CRITICAL, DEBUG, ERROR, INFO, WARNING, Formatter, StreamHandler, getLogger
from subprocess import call
import sys
from sys import stdout

import click
from click import ParamType, Path

from grolt import make_auth, Neo4jService, Neo4jDirectorySpec, __name__ as root_module_name
from grolt.compat import shlex_quote
from grolt.security import install_self_signed_certificate

# The readline import allows for extended input functionality, including
# up/down arrow navigation. This should not be removed.
try:
    import readline
except ModuleNotFoundError as e:
    # readline is not available for windows 10
    # noinspection PyUnresolvedReferences
    from pyreadline import Readline
    readline = Readline()


class AuthParamType(ParamType):

    name = "auth"

    def __init__(self, default_user=None, default_password=None):
        self.default_user = default_user
        self.default_password = default_password

    def convert(self, value, param, ctx):
        try:
            return make_auth(value, self.default_user, self.default_password)
        except ValueError as error:
            self.fail(error.args[0], param, ctx)

    def __repr__(self):
        return 'USER:PASSWORD'


class VolumeMount(object):
    def __init__(self, source, destination):
        self.source = source
        self.destination = destination


class VolumeMountParamType(click.ParamType):

    name = "vol"

    source_spec = Path(exists=True, dir_okay=True, readable=True, writable=True, allow_dash=False)
    destination_spec = Path(exists=False, allow_dash=False)

    def __init__(self):
        pass

    def convert(self, value, param, ctx):
        [source, destination] = value.split(":")
        return VolumeMount(
            self.source_spec.convert(source.strip(), None, None),
            self.destination_spec.convert(destination.strip(), None, None)
        )

    def __repr__(self):
        return 'SOURCE:DESTINATION'


class ConfigParamType(click.ParamType):

    name = "NAME=VALUE"

    def __repr__(self):
        return 'NAME=VALUE'


class ColourFormatter(Formatter):

    def format(self, record):
        s = super(ColourFormatter, self).format(record)
        bits = s.split("  ", 1)
        bits[0] = click.style(bits[0], fg="blue")
        if record.levelno == CRITICAL:
            bits[1] = click.style(bits[1], fg="bright_red")
        elif record.levelno == ERROR:
            bits[1] = click.style(bits[1], fg="bright_yellow")
        elif record.levelno == WARNING:
            bits[1] = click.style(bits[1], fg="yellow")
        elif record.levelno == INFO:
            pass
        elif record.levelno == DEBUG:
            bits[1] = click.style(bits[1], fg="cyan")
        return "  ".join(bits)


class Watcher(object):
    """ Log watcher for monitoring driver and protocol activity.
    """

    handlers = {}

    def __init__(self, logger_name):
        super(Watcher, self).__init__()
        self.logger_name = logger_name
        self.logger = getLogger(self.logger_name)
        self.formatter = ColourFormatter("%(asctime)s  %(message)s",
                                         "%H:%M:%S")

    def watch(self, level=INFO, out=stdout):
        self.stop()
        handler = StreamHandler(out)
        handler.setFormatter(self.formatter)
        self.handlers[self.logger_name] = handler
        self.logger.addHandler(handler)
        self.logger.setLevel(level)

    def stop(self):
        try:
            self.logger.removeHandler(self.handlers[self.logger_name])
        except KeyError:
            pass


def watch_log(ctx, param, value):
    watcher = Watcher(root_module_name)
    watcher.watch(DEBUG if value >= 1 else INFO)
    return watcher


@click.command(context_settings={"ignore_unknown_options": True}, help="""\
Run a Neo4j cluster or standalone server in one or more local Docker 
containers.

If an additional COMMAND is supplied, this will be executed after startup, 
with a shutdown occurring immediately afterwards. If no COMMAND is supplied,
an interactive command line console will be launched which allows direct
control of the service. This console can be shut down with Ctrl+C, Ctrl+D or
by entering the command 'exit'.

A couple of environment variables will also be made available to any COMMAND
passed. These are:

\b
- BOLT_SERVER_ADDR
- NEO4J_AUTH

""")
@click.option("-a", "--auth", type=AuthParamType(), envvar="NEO4J_AUTH",
              help="Credentials with which to bootstrap the service. These "
                   "must be specified as a 'user:password' pair and may "
                   "alternatively be supplied via the NEO4J_AUTH environment "
                   "variable. These credentials will also be exported to any "
                   "COMMAND executed during the service run.")
@click.option("-B", "--bolt-port", type=int,
              help="A port number (standalone) or base port number (cluster) "
                   "for Bolt traffic.")
@click.option("-c", "--n-cores", type=int,
              help="If specified, a cluster with this many cores will be "
                   "created. If omitted, a standalone service will be created "
                   "instead. See also -r for specifying the number of read "
                   "replicas.")
@click.option("-C", "--config", type=ConfigParamType(), multiple=True,
              help="Pass a configuration value into neo4j.conf. This can be "
                   "used multiple times.")
@click.option("-d", "--directory", multiple=True, type=VolumeMountParamType(),
              help="Share a local directory into the neo4j docker container(s) "
                   "(mount a volume in docker parlance). "
                   "N.b. the directory is shared to ALL docker containers.")
@click.option("-D", "--debug-port", type=int,
              help="The port number (standalone) or base port number (cluster) "
                   "for java remote debugging.")
@click.option("-e", "--env", type=ConfigParamType(), multiple=True,
              help="Pass an env value into neo4j docker containers. This can be "
                   "used multiple times.")
@click.option("-E", "--debug-suspend", is_flag=True,
              help="The first Neo4j server process (machine a) should hang "
                   "until a connection is made by a remote java debugger. This "
                   "option is only valid if a debug port is specified with -D.")
# -h / --help is automatically provided by click
@click.option("-H", "--http-port", type=int,
              help="A port number (standalone) or base port number (cluster) "
                   "for HTTP traffic.")
@click.option("--https-port", type=int,
              help="A port number (standalone) or base port number (cluster) "
                   "for HTTPS traffic.")
@click.option("-i", "--image",
              help="The Docker image tag to use for building containers. The "
                   "repository name can be included before the colon, but will "
                   "default to 'neo4j' if omitted. Note that a Neo4j "
                   "Enterprise Edition image is required for building "
                   "clusters. File URLs can also be passed, which can "
                   "allow for loading images from local tar files.")
@click.option("-I", "--import-dir", type=Path(exists=True, dir_okay=True,
                                              writable=True),
              help="Share a local directory for use by server import.")
@click.option("-L", "--logs-dir", type=Path(exists=True, dir_okay=True,
                                            writable=True),
              help="Share a local directory for use by server logs. A "
                   "subdirectory will be created for each machine.")
@click.option("-n", "--name",
              help="A Docker network name to which all servers will be "
                   "attached. If omitted, an auto-generated name will be "
                   "used.")
@click.option("-N", "--neo4j-source-dir", type=Path(exists=True, dir_okay=True),
              help="Path to neo4j source repo. Mounts and uses the "
                   "packaged neo4j jars and binaries from there.")
@click.option("-P", "--plugins-dir", type=Path(exists=True, dir_okay=True,
                                               writable=True),
              help="Share a local directory for use by server plugins.")
@click.option("-r", "--n-replicas", type=int,
              help="The number of read replicas to include within the "
                   "cluster. This option will only take effect if -c is also "
                   "used.")
@click.option("-R", "--server-side-routing", is_flag=True,
              help="Enable server-side routing.")
@click.option("-S", "--certificates-dir", type=Path(exists=True, dir_okay=True,
                                                    writable=True),
              help="Share a local directory for use by server certificates.")
@click.option("-u", "--user",
              help="User name or ID as whom to run the Docker container. "
                   "For the current user, use `-u $(whoami)`.")
@click.option("-v", "--verbose", count=True, callback=watch_log,
              expose_value=False, is_eager=True,
              help="Show more detail about the startup and shutdown process.")
@click.option("-Z", "--self-signed-certificate", is_flag=True,
              help="Generate and use a self-signed certificate.")
@click.argument("command", nargs=-1, type=click.UNPROCESSED)
def grolt(
        command,
        name,
        image,
        auth,
        user,
        n_cores,
        n_replicas,
        bolt_port,
        http_port,
        https_port,
        debug_port,
        env,
        debug_suspend,
        import_dir,
        logs_dir,
        plugins_dir,
        certificates_dir,
        neo4j_source_dir,
        directory,
        config,
        server_side_routing,
        self_signed_certificate,
):
    try:
        if self_signed_certificate:
            if certificates_dir is not None:
                click.echo("Incompatible certificate options specified", err=True)
                exit(1)
            certificates_dir = install_self_signed_certificate(image)

        dir_spec = Neo4jDirectorySpec(
            import_dir=import_dir,
            logs_dir=logs_dir,
            plugins_dir=plugins_dir,
            certificates_dir=certificates_dir,
            shared_dirs=directory,
            neo4j_source_dir=neo4j_source_dir,
        )
        config_dict = dict(item.partition("=")[::2] for item in config)
        if server_side_routing:
            config_dict["dbms.routing.enabled"] = "true"
        env_dict = dict(item.partition("=")[::2] for item in env)
        with Neo4jService(
                name,
                image,
                auth,
                user,
                n_cores,
                n_replicas,
                bolt_port,
                http_port,
                https_port,
                debug_port,
                debug_suspend,
                dir_spec,
                config_dict,
                env_dict
        ) as neo4j:
            if command:
                call(" ".join(map(shlex_quote, command)), shell=True,
                     env=neo4j.env())
            else:
                neo4j.run_console()
    except KeyboardInterrupt:
        sys.exit(130)
    except Exception as error:
        message = " ".join(map(str, error.args))
        if hasattr(error, 'explanation'):
            message += "\n" + error.explanation
        click.echo(message, err=True)
        sys.exit(1)


if __name__ == "__main__":
    grolt()

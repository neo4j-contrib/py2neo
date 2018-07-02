#!/usr/bin/env python
# -*- encoding: utf-8 -*-

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


from io import StringIO
import json
import os
from subprocess import call
from shlex import split as shlex_split
from shutil import rmtree
from sys import argv
from tempfile import mkdtemp

from ipykernel.kernelapp import launch_new_instance
from ipykernel.kernelbase import Kernel
from jupyter_client.kernelspec import KernelSpecManager
from neo4j.exceptions import ServiceUnavailable, CypherError
from traitlets.config import Config

from py2neo.internal.addressing import get_connection_data
from py2neo.database import Graph
from py2neo.meta import __version__ as py2neo_version


ESCAPE = "!"

KERNEL_NAME = "cypher"

BANNER = """\
Py2neo {py2neo_version} Interactive Cypher
Type '!help' for more information.
""".format(py2neo_version=py2neo_version)


def table_to_text_plain(table):
    s = StringIO()
    table.write(file=s, header=True)
    return s.getvalue()


def table_to_text_html(table):
    s = StringIO()
    table.write_html(file=s, header=True)
    return s.getvalue()


class CypherKernel(Kernel):

    implementation = 'py2neo.cypher'
    implementation_version = py2neo_version
    language_info = {
        "name": "cypher",
        "version": "Neo4j/3.4",
        "pygments_lexer": "py2neo.cypher",
        "file_extension": ".cypher",
    }
    banner = BANNER.strip()
    help_links = []

    def __init__(self, **kwargs):
        Kernel.__init__(self, **kwargs)
        self._server = {}
        self._graph = None

    @property
    def graph(self):
        if self._graph is None:
            if not self._server:
                raise NoServerConfigured()
            self._graph = Graph(**self._server)
        return self._graph

    def display(self, data):
        self.send_response(self.iopub_socket, "display_data", content={"data": data})

    def display_server_info(self, silent, **_):
        if silent:
            return
        try:
            product = self.graph.database.product
        except NoServerConfigured:
            self.display({
                "text/plain": "No server configured",
            })
        else:
            self.display({
                "text/plain": "Server  | {}\nProduct | {}".format(self._server["uri"], product)
            })

    def error(self, name, value, traceback=None):
        error_content = {
            "execution_count": self.execution_count,
            "ename": name,
            "evalue": value,
            "traceback": [name + ": " + value] + (traceback or []),
        }
        self.send_response(self.iopub_socket, "error", error_content)
        return dict(error_content, status="error")

    def _execute_server_command(self, *args, **kwargs):
        """ !server <uri>
        """
        if len(args[1:]) > 1:
            return self.error("Too many arguments", self._execute_server_command.__doc__)
        if not args[1:]:
            self.display_server_info(**kwargs)
            return
        self._server = get_connection_data(args[1])
        self._graph = None
        self.display_server_info(**kwargs)

    commands = {
        "server": _execute_server_command,
    }

    def _execute_command(self, code, **kwargs):
        args = shlex_split(code)
        if not args:
            for command, func in sorted(self.commands.items()):
                # TODO: actual message
                print(func.__doc__.strip() or command)
            return {
                'status': 'ok',
                'execution_count': self.execution_count,
                'payload': [],
                'user_expressions': {},
            }
        try:
            f = self.commands[args[0]]
        except KeyError:
            return self.error("Command not found", args[0])
        else:
            return dict({
                'status': 'ok',
                'execution_count': self.execution_count,
                'payload': [],
                'user_expressions': {},
            }, **(f(self, *args, **kwargs) or {}))

    def _execute_cypher(self, code, silent, store_history=True, user_expressions=None, allow_stdin=False):
        try:
            table = self.graph.run(code).to_table()
        except ServiceUnavailable as error:
            self._graph = None
            return self.error("Service Unavailable", error.args[0])
        except CypherError as error:
            return self.error(error.code or type(error).__name__, error.message or "")
        except NoServerConfigured:
            return self.error("No server configured", "use `" + ESCAPE + "server <uri>` to configure")
        else:
            if not silent:
                self.send_response(self.iopub_socket, "execute_result", {
                    "execution_count": self.execution_count,
                    "data": {
                        "text/plain": table_to_text_plain(table),
                        "text/html": table_to_text_html(table),
                    },
                })
            return {
                'status': 'ok',
                'execution_count': self.execution_count,
                'payload': [],
                'user_expressions': {},
            }

    def do_execute(self, code, silent, store_history=True, user_expressions=None, allow_stdin=False):
        bare_code = code.strip()
        if bare_code.startswith(ESCAPE):
            return self._execute_command(bare_code[1:], silent=silent, store_history=True,
                                         user_expressions=None, allow_stdin=False)
        else:
            return self._execute_cypher(code, silent, store_history=store_history,
                                        user_expressions=user_expressions, allow_stdin=allow_stdin)

    def do_complete(self, code, cursor_pos):
        return {'matches': [],
                'cursor_end': cursor_pos,
                'cursor_start': cursor_pos,
                'metadata': {},
                'status': 'ok'}

    def do_inspect(self, code, cursor_pos, detail_level=0):
        return {'status': 'ok', 'data': {}, 'metadata': {}, 'found': False}

    def do_history(self, hist_access_type, output, raw, session=None, start=None,
                   stop=None, n=None, pattern=None, unique=False):
        return {'status': 'ok', 'history': []}

    def do_shutdown(self, restart):
        return {'status': 'ok', 'restart': restart}

    def do_is_complete(self, code):
        return {'status': 'unknown'}

    def do_apply(self, content, bufs, msg_id, reply_metadata):
        """DEPRECATED"""
        raise NotImplementedError

    def do_clear(self):
        """DEPRECATED"""
        raise NotImplementedError


class NoServerConfigured(Exception):

    pass


def install_kernel(user=True, prefix=None):
    """ Install the kernel for use by Jupyter.
    """
    td = mkdtemp()
    try:
        os.chmod(td, 0o755)  # Starts off as 700, not user readable
        with open(os.path.join(td, "kernel.json"), "w") as f:
            json.dump({
                "argv": ["python", "-m", "py2neo.cypher.console", "kernel", "launch", "-f", "{connection_file}"],
                "display_name": "Cypher",
                "language": "cypher",
                "pygments_lexer": "py2neo.cypher",
            }, f, sort_keys=True)
        return KernelSpecManager().install_kernel_spec(td, KERNEL_NAME, user=user, prefix=prefix)
    finally:
        rmtree(td)


def launch_kernel():
    """ Launch a new Jupyter application instance, using this kernel.
    """
    c = Config()
    c.TerminalInteractiveShell.highlighting_style = "vim"
    launch_new_instance(kernel_class=CypherKernel, config=c)


def main():
    if not argv[1:]:
        if KERNEL_NAME not in KernelSpecManager().find_kernel_specs():
            install_kernel()
        call("jupyter console --kernel %s" % KERNEL_NAME, env=os.environ.copy(), shell=True)
        return
    command = argv[1]
    if command == "kernel":
        if not argv[2:]:
            launch_kernel()
        subcommand = argv[2]
        if subcommand == "install":
            d = install_kernel()
            print("Installed Cypher kernel to %s" % d)
        elif subcommand == "launch":
            launch_kernel()
        else:
            raise RuntimeError("Unknown kernel subcommand: %s" % subcommand)
    else:
        raise RuntimeError("Unknown command: %s" % command)


if __name__ == "__main__":
    main()

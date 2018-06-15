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
    banner = "Py2neo Jupyter kernel for Cypher"
    help_links = []

    def __init__(self, **kwargs):
        Kernel.__init__(self, **kwargs)
        self.server = {}
        self.graph = None

    def display(self, data):
        self.send_response(self.iopub_socket, "display_data", content={"data": data})

    def error(self, name, value, traceback=None):
        error_content = {
            "execution_count": self.execution_count,
            "ename": name,
            "evalue": value,
            "traceback": [name + ": " + value] + (traceback or []),
        }
        self.send_response(self.iopub_socket, "error", error_content)
        return dict(error_content, status="error")

    def _execute_server(self, *args, **kwargs):
        """ !server <uri>
        """
        if len(args[1:]) > 1:
            return self.error("Too many arguments", self._execute_server.__doc__)
        if not args[1:]:
            if not kwargs["silent"]:
                self.display({
                    "text/plain": "Using server at " + self.server["uri"] if self.server else "No server configured",
                })
            return
        self.server = get_connection_data(args[1])
        self.graph = None
        if not kwargs["silent"]:
            self.display({
                "text/plain": "Using server at " + self.server["uri"]
            })

    commands = {
        "server": _execute_server,
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
            if self.graph is None:
                if self.server:
                    self.graph = Graph(**self.server)
                else:
                    return self.error("No server configured", "use `" + ESCAPE + "server <uri>` to configure")
            table = self.graph.run(code).to_table()
        except ServiceUnavailable as error:
            self.graph = None
            return self.error("Service Unavailable", error.args[0])
        except CypherError as error:
            return self.error(error.code or type(error).__name__, error.message or "")
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


def install_kernel(user=True, prefix=None):
    td = mkdtemp()
    try:
        os.chmod(td, 0o755)  # Starts off as 700, not user readable
        with open(os.path.join(td, "kernel.json"), "w") as f:
            json.dump({
                "argv": ["python", "-m", "py2neo.cypher.kernel", "-f", "{connection_file}"],
                "display_name": "Cypher",
                "language": "cypher",
                "pygments_lexer": "py2neo.cypher",
            }, f, sort_keys=True)
        KernelSpecManager().install_kernel_spec(td, KERNEL_NAME, user=user, prefix=prefix)
    finally:
        rmtree(td)


def launch_kernel():
    c = Config()
    c.TerminalInteractiveShell.highlighting_style = "vim"
    launch_new_instance(kernel_class=CypherKernel, config=c)


def launch_console():
    if KERNEL_NAME not in KernelSpecManager().find_kernel_specs():
        install_kernel()
    call("jupyter console --kernel " + KERNEL_NAME, env=os.environ.copy(), shell=True)


if __name__ == "__main__":
    launch_kernel()

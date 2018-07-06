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
from neotime import DateTime
from traitlets.config import Config

from py2neo.internal.addressing import get_connection_data
from py2neo.database import Graph
from py2neo.meta import __version__ as py2neo_version


ESCAPE = "!"

KERNEL_NAME = "cypher"

BANNER = """\
Py2neo {py2neo_version} Interactive Cypher Console
Type `!help` for more information or Ctrl+D to exit

Using server at {{uri}}
""".format(py2neo_version=py2neo_version)


def table_to_text_plain(table):
    s = StringIO()
    table.write(file=s, header=True)
    return s.getvalue()


def table_to_text_csv(table):
    s = StringIO()
    table.write_csv(file=s, header=True)
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
    help_links = []

    def __init__(self, **kwargs):
        Kernel.__init__(self, **kwargs)
        self._graph = None
        self.server = get_connection_data()
        self.banner = BANNER.strip().format(uri=self.server["uri"])
        self.tx = None

    @property
    def graph(self):
        if self._graph is None:
            self._graph = Graph(**self.server)
        return self._graph

    def display(self, data):
        self.send_response(self.iopub_socket, "display_data", content={"data": data})

    def display_server_info(self, silent, **_):
        if silent:
            return
        try:
            product = self.graph.database.product
        except ServiceUnavailable as error:
            self.display({
                "text/plain": "Server: {}\nServiceUnavailable: {}".format(self.server["uri"], error.args[0]),
            })
        else:
            self.display({
                "text/plain": "Server: {}\nKernel: {}".format(self.server["uri"], product),
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

    def _execute_help_command(self, *args, **kwargs):
        """ !help
        """
        lines = []
        for command in sorted(self.commands):
            lines.append("!" + command)
        self.display({
            "text/plain": "\n".join(lines),
        })

    def _execute_server_command(self, uri=None, user=None, password=None, *args, **kwargs):
        """ !server <uri> <user> <password>
        """
        if args:
            return self.error("Too many arguments", self._execute_server_command.__doc__)
        if not uri:
            self.display_server_info(**kwargs)
            return
        self.server = get_connection_data(uri=uri, user=user, password=password)
        self._graph = None
        self.display_server_info(**kwargs)

    def _execute_begin_command(self, *args, **kwargs):
        if self.tx is None:
            self.tx = self.graph.begin()
            self.tx_counter = 1
            self.display({"text/plain": "Began transaction at {}".format(DateTime.now())}),
        else:
            return self.error("TransactionError", "A transaction has already begun")

    def _execute_commit_command(self, *args, **kwargs):
        if self.tx:
            self.tx = self.tx.commit()
            self.display({"text/plain": "Transaction committed at {}".format(DateTime.now())})
        else:
            return self.error("TransactionError", "No current transaction")

    def _execute_rollback_command(self, *args, **kwargs):
        if self.tx:
            self.tx = self.tx.rollback()
            self.display({"text/plain": "Transaction rolled back at {}".format(DateTime.now())})
        else:
            return self.error("TransactionError", "No current transaction")

    def _execute_play_command(self, *args, **kwargs):
        pass

    commands = {
        "help":
            _execute_help_command,
        "server":
            _execute_server_command,
        "begin":
            _execute_begin_command,
        "commit":
            _execute_commit_command,
        "rollback":
            _execute_rollback_command,
        "play":
            _execute_play_command,
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
            return self.error("ServiceUnavailable", error.args[0])
        except CypherError as error:
            self.tx = None
            return self.error(error.code or type(error).__name__, error.args[0] or "")
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
        upper_bare_code = bare_code.upper()
        if bare_code.startswith(ESCAPE):
            return self._execute_command(bare_code[1:], silent=silent, store_history=store_history,
                                         user_expressions=user_expressions, allow_stdin=allow_stdin)
        elif (upper_bare_code.startswith("BEGIN") or
              upper_bare_code.startswith("COMMIT") or
              upper_bare_code.startswith("ROLLBACK")):
            return self.do_execute("!" + bare_code, silent=silent, store_history=store_history,
                                   user_expressions=user_expressions, allow_stdin=allow_stdin)
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
    """ Install the kernel for use by Jupyter.
    """
    td = mkdtemp()
    try:
        os.chmod(td, 0o755)  # Starts off as 700, not user readable
        with open(os.path.join(td, "kernel.json"), "w") as f:
            json.dump({
                "argv": ["python", "-m", "py2neo.cypher.kernel", "launch", "-f", "{connection_file}"],
                "display_name": "Cypher",
                "language": "cypher",
                "pygments_lexer": "py2neo.cypher",
            }, f, sort_keys=True)
        return KernelSpecManager().install_kernel_spec(td, KERNEL_NAME, user=user, prefix=prefix)
    finally:
        rmtree(td)


def uninstall_kernel():
    """ Uninstall the kernel.
    """
    KernelSpecManager().remove_kernel_spec(KERNEL_NAME)


def launch_kernel():
    """ Launch a new Jupyter application instance, using this kernel.
    """
    c = Config()
    c.TerminalInteractiveShell.highlighting_style = "vim"
    launch_new_instance(kernel_class=CypherKernel, config=c)


def main():
    if not argv[1:]:
        launch_kernel()
    subcommand = argv[1]
    if subcommand == "install":
        d = install_kernel()
        print("Installed Cypher kernel to %s" % d)
    elif subcommand == "launch":
        launch_kernel()
    else:
        raise RuntimeError("Unknown kernel subcommand: %s" % subcommand)


if __name__ == "__main__":
    main()

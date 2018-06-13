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


try:
    from ipykernel.kernelapp import IPKernelApp
    from ipykernel.kernelbase import Kernel
    from IPython.core.display import display
    from IPython.utils.tempdir import TemporaryDirectory
    from jupyter_client.kernelspec import KernelSpecManager
    from traitlets.config import Config
except ImportError:
    from warnings import warn
    warn("The Cypher kernel requires installation of the [jupyter] extra")
    raise

from neo4j.exceptions import ServiceUnavailable, CypherError

from py2neo.database import Graph
from py2neo.meta import __version__


class CypherKernel(Kernel):

    implementation = 'py2neo.cypher'
    implementation_version = __version__
    language_info = {
        "name": "cypher",
        "version": "Neo4j/3.4",
        "pygments_lexer": "py2neo.cypher",
        "file_extension": ".cypher",
    }
    banner = ""
    help_links = []

    def __init__(self, **kwargs):
        Kernel.__init__(self, **kwargs)
        self.graph = Graph()
        self.banner = self.graph.database.query_jmx("org.neo4j", name="Kernel")["KernelVersion"]

    def do_execute(self, code, silent, store_history=True, user_expressions=None, allow_stdin=False):
        try:
            table = self.graph.run(code).to_table()
        except CypherError as error:
            error_content = {
                "execution_count": self.execution_count,
                "ename": error.code,
                "evalue": error.message,
                "traceback": [error.code, error.message],
            }
            self.send_response(self.iopub_socket, "error", error_content)
            return dict(error_content, status="error")
        else:
            if not silent:
                self.send_response(self.iopub_socket, "execute_result", {
                    "execution_count": self.execution_count,
                    "data": {
                        "text/plain": repr(table),
                        "text/html": table._repr_html_(),
                    },
                })
            return {
                'status': 'ok',
                'execution_count': self.execution_count,
                'payload': [],
                'user_expressions': {},
            }

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
        return {'status': 'unknown',
                }


def install(user=True, prefix=None):
    import os
    import json
    with TemporaryDirectory() as td:
        os.chmod(td, 0o755)  # Starts off as 700, not user readable
        with open(os.path.join(td, 'kernel.json'), 'w') as f:
            json.dump({
                "argv": ["python", "-m", "py2neo.cypher.kernel", "-f", "{connection_file}"],
                "display_name": "Cypher",
                "language": "cypher",
                "pygments_lexer": "py2neo.cypher",
            }, f, sort_keys=True)
        # TODO: Copy resources once they're specified

        print('Installing IPython kernel spec')
        KernelSpecManager().install_kernel_spec(td, "cypher", user=user, prefix=prefix)


def uninstall():
    KernelSpecManager().remove_kernel_spec("cypher")


def list():
    from pprint import pprint
    pprint(KernelSpecManager().find_kernel_specs())


def launch():
    c = Config()
    c.TerminalInteractiveShell.highlighting_style = "vim"
    kwargs = {
        "kernel_class": CypherKernel,
        "config": c,
    }
    try:
        IPKernelApp.launch_instance(**kwargs)
    except ServiceUnavailable:
        app = IPKernelApp.instance(**kwargs)
        app.exit(1)


def main():
    import sys
    args = sys.argv[1:]
    if args and args[0] == "install":
        install()
    elif args and args[0] == "uninstall":
        uninstall()
    elif args and args[0] == "list":
        list()
    else:
        launch()


if __name__ == "__main__":
    main()

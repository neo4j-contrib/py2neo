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

from py2neo.database import Graph
from py2neo.meta import __version__


class CypherKernel(Kernel):

    implementation = 'cypher_kernel'
    implementation_version = __version__
    language = "Cypher"
    language_version = "Neo4j/3.4"
    banner = ""
    language_info = {
        'name': 'cypher',
        'file_extension': '.cypher',
        "pygments_lexer": "py2neo.cypher",
        "pygments_style": "vim",
    }

    def __init__(self, **kwargs):
        Kernel.__init__(self, **kwargs)
        self.graph = Graph()
        self.banner = self.graph.database.query_jmx("org.neo4j", name="Kernel")["KernelVersion"]

    def do_apply(self, content, bufs, msg_id, reply_metadata):
        pass

    def do_clear(self):
        pass

    def do_complete(self, code, cursor_pos):
        pass

    def do_execute(self, code, silent, store_history=True, user_expressions=None, allow_stdin=False):
        table = self.graph.run(code).to_table()
        if not silent:
            stream_content = {'name': 'stdout', 'text': repr(table)}
            self.send_response(self.iopub_socket, 'stream', stream_content)
        return {'status': 'ok', 'execution_count': self.execution_count,
                'payload': [], 'user_expressions': {}}


def install_my_kernel_spec(user=True, prefix=None):
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
                "pygments_style": "vim",
            }, f, sort_keys=True)
        # TODO: Copy resources once they're specified

        print('Installing IPython kernel spec')
        KernelSpecManager().install_kernel_spec(td, "cypher", user=user, prefix=prefix)


def main():
    import sys
    args = sys.argv[1:]
    if args and args[0] == "install":
        install_my_kernel_spec()
    else:
        c = Config()
        c.TerminalInteractiveShell.highlighting_style = "vim"
        IPKernelApp.launch_instance(kernel_class=CypherKernel, config=c)


if __name__ == "__main__":
    main()

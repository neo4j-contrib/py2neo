#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright 2011-2014, Nigel Small
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


from __future__ import print_function

import os
import sys

from py2neo.ext.box.core import NeoBox
from py2neo.util import ustr


HELP = """\
Usage: {script} list [paths|pids|ports]
       {script} make «name» «edition» «version»
       {script} remove «name» [force]
       {script} rename «name» «new_name»
       {script} start «name»
       {script} stop «name»
       {script} drop «name»
"""


def _help(script):
    print(HELP.format(script=os.path.basename(script)))


def _list(*args):
    box = NeoBox()
    if len(args) == 0:
        template = "{name}"
    elif len(args) == 1 and args[0] in ("paths", "pids", "ports"):
        template = "{name} {%s}" % args[0][:-1]
    else:
        template = None
    if template:
        server_list = box.server_list()
        if server_list:
            server_paths = [box.server_home(name) for name in sorted(server_list.keys())]
            max_name_length = max(map(len, list(server_list.keys())))
            for i, (name, webserver_port) in enumerate(sorted(server_list.items())):
                webserver_https_port = webserver_port + 1
                path = server_paths[i]
                print(template.format(name=name.ljust(max_name_length), path=path,
                                      pid=(box.get_server(name).pid or ""),
                                      port=("%s %s" % (webserver_port, webserver_https_port))))
    else:
        raise ValueError("Bad arguments")


def main():
    script, args = sys.argv[0], sys.argv[1:]
    try:
        if args:
            command, args = args[0], args[1:]
            if command == "help":
                _help(script)
            elif command == "list":
                _list(*args)
            else:
                box = NeoBox()
                name, args = args[0], args[1:]
                if command == "make":
                    edition, version = args
                    box.make_server(name, edition, version)
                    server_list = box.server_list()
                    webserver_port = server_list[name]
                    webserver_https_port = webserver_port + 1
                    print("Created server instance %r configured on ports %s and %s" % (
                        name, webserver_port, webserver_https_port))
                elif command == "remove" and args == ():
                    box.remove_server(name)
                elif command == "remove" and args == ("force",):
                    box.remove_server(name, force=True)
                elif command == "rename":
                    new_name, = args
                    box.rename_server(name, new_name)
                elif command == "start":
                    ps = box.get_server(name).start()
                    print(ps.service_root.uri)
                elif command == "stop":
                    box.get_server(name).stop()
                elif command == "drop" and args == ():
                    box.get_server(name).store.drop()
                elif command == "drop" and args == ("force",):
                    box.get_server(name).store.drop(force=True)
                elif command == "load":
                    path, args = args[0], args[1:]
                    if args == ():
                        box.get_server(name).store.load(path)
                    elif args == ("force",):
                        box.get_server(name).store.load(path, force=True)
                    else:
                        raise ValueError("Bad command or arguments")
                elif command == "save":
                    path, args = args[0], args[1:]
                    if args == ():
                        box.get_server(name).store.save(path)
                    elif args == ("force",):
                        box.get_server(name).store.save(path, force=True)
                    else:
                        raise ValueError("Bad command or arguments")
                else:
                    raise ValueError("Bad command or arguments")
        else:
            _help(script)
    except Exception as error:
        sys.stderr.write(ustr(error))
        sys.stderr.write("\n")
        _help(script)
        sys.exit(1)


if __name__ == "__main__":
    main()

#!/usr/bin/env python
# coding: utf-8

# Copyright 2011-2021, Nigel Small
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


from collections import OrderedDict
from threading import Thread, Lock
from time import sleep

from pansi.console import Console
from prompt_toolkit import Application
from prompt_toolkit.formatted_text import to_formatted_text
from prompt_toolkit.key_binding import KeyBindings
from prompt_toolkit.layout import Layout, HSplit, VSplit, Window, FormattedTextControl
from prompt_toolkit.mouse_events import MouseEventType

from py2neo.client import ConnectionProfile, Connection, ConnectionUnavailable, \
    BrokenTransactionError, ConnectionRecord, TransactionRecord


class Monitor(Console):

    def __init__(self, profile=None, **settings):
        super(Monitor, self).__init__("py2neo", verbosity=0)
        self.app = Application(layout=self._get_layout(), key_bindings=self._get_key_bindings(),
                               full_screen=True, mouse_support=True)
        self.profile = ConnectionProfile(profile, **settings)
        self.scanner = Scanner(self)
        self.current_profile_index = 0
        self.current_profile = None
        self.current_list = "c"
        self.servers = OrderedDict([(self.profile, ServerScanner(self.profile))])

    def update(self):
        self.app.invalidate()

    def _get_key_bindings(self):
        kb = KeyBindings()

        @kb.add('c')
        def _(event):
            self.current_list = "c"

        @kb.add('t')
        def _(event):
            self.current_list = "t"

        @kb.add('q')
        def _(event):
            self.current_list = "q"

        @kb.add('pageup')
        def _(event):
            self.current_profile_index = max(0, self.current_profile_index - 1)
            for i, profile in enumerate(self.servers):
                if i == self.current_profile_index:
                    self.current_profile = profile
                    break

        @kb.add('pagedown')
        def _(event):
            self.current_profile_index = min(
                len(self.servers) - 1, self.current_profile_index + 1)
            for i, profile in enumerate(self.servers):
                if i == self.current_profile_index:
                    self.current_profile = profile
                    break

        @kb.add('c-c')
        def _(event):
            """
            Pressing Ctrl-C will exit the user interface.

            Setting a return value means: quit the event loop that drives the user
            interface and return this value from the `Application.run()` call.
            """
            self.scanner.stop()
            event.app.exit()

        return kb

    def _get_layout(self):
        return Layout(VSplit([
            Window(
                content=FormattedTextControl(
                    self._get_overview_fragments,
                    focusable=False,
                    show_cursor=False),
                # right_margins=[
                #    ScrollbarMargin(display_arrows=True),
                # ],
                width=self._get_overview_width,
                dont_extend_height=True),
            Window(width=2, char=" "),
            HSplit([
                Window(content=FormattedTextControl(text=self._get_title_fragments), height=4),
                Window(height=1, char=" "),
                Window(content=FormattedTextControl(text=self._get_list_fragments)),
            ])
        ]))

    def _get_overview_width(self):
        texts = []
        for i, (profile, scanner) in enumerate(self.servers.items()):
            if scanner.available:
                os = scanner.management_data["java.lang:type=OperatingSystem"]
                text = "{} {:.02f}".format(profile.address, os["SystemLoadAverage"])
            else:
                text = "{} ----".format(profile.address)
            texts.append(text)

        if self.servers:
            width = max(len(text) for text in texts)
        else:
            width = 0
        return width + 2

    def _get_overview_fragments(self):
        def mouse_handler(mouse_event):
            """
            Set `_selected_index` and `current_value` according to the y
            position of the mouse click event.
            """
            if mouse_event.event_type == MouseEventType.MOUSE_UP:
                self.current_index = mouse_event.position.y - 1

        texts = []
        for i, (profile, server) in enumerate(self.servers.items()):
            if server.available:
                os = server.management_data["java.lang:type=OperatingSystem"]
                text = "{} {:.02f}".format(profile.address, os["SystemLoadAverage"])
            else:
                text = "{} ----".format(profile.address)
            texts.append(text)

        if self.servers:
            width = max(len(text) for text in texts)
        else:
            width = 0

        fragments = [("", "Cluster Cores".ljust(width)), ('', '\n')]
        for i, (profile, server) in enumerate(self.servers.items()):
            selected = (i == self.current_profile_index)

            fragments.append(('', '  '))
            if selected:
                fragments.extend(to_formatted_text(texts[i], style='reverse'))
            else:
                fragments.extend(to_formatted_text(texts[i], style=''))
            fragments.append(('', '\n'))

        # Add mouse handler to all fragments.
        for i in range(len(fragments)):
            fragments[i] = (fragments[i][0], fragments[i][1], mouse_handler)

        fragments.pop()  # Remove last newline.
        return fragments

    def _get_title_fragments(self):
        try:
            server = self.servers[self.current_profile]
            assert isinstance(server, ServerScanner), "server is %r" % server
        except KeyError:
            return [("", "Loading...")]
        else:
            os = server.management_data["java.lang:type=OperatingSystem"]
            fragments = [
                ("", "Neo4j {} {}E at {} ({} {})".format(
                    server.neo4j_version,
                    server.neo4j_edition[:1].upper(),
                    self.current_profile.address,
                    self.current_profile.scheme,
                    ".".join(map(str, server.protocol_version)))),
                ("", "\n"),
                ("", "{} connections, {} transactions, {} queries".format(
                    len(server.connections),
                    len(server.transactions),
                    len(server.queries))),
                ("", "\n"),
                ("", "Load average: {}, memory ?".format(
                    os["SystemLoadAverage"])),
                ("", "\n"),
                ("", "JVM: ?"),
            ]
            return fragments

    def _get_list_fragments(self):
        if self.current_list == "c":
            return self._get_connection_list_fragments()
        elif self.current_list == "t":
            return self._get_transaction_list_fragments()
        elif self.current_list == "q":
            return self._get_query_list_fragments()
        else:
            return []

    def _get_connection_list_fragments(self):
        try:
            server = self.servers[self.current_profile]
            assert isinstance(server, ServerScanner), "server is %r" % server
        except KeyError:
            return [("", "Loading...")]
        else:
            fragments = []
            for cx in server.connections:
                assert isinstance(cx, ConnectionRecord)
                fragments.append(("", "{} {} {} {} {}".format(cx.cxid, cx.server_profile.user,
                                                              cx.client_address, cx.since,
                                                              cx.user_agent)))
                fragments.append(("", "\n"))
            return fragments

    def _get_transaction_list_fragments(self):
        try:
            server = self.servers[self.current_profile]
            assert isinstance(server, ServerScanner), "server is %r" % server
        except KeyError:
            return [("", "Loading...")]
        else:
            fragments = []
            for tx in server.transactions:
                assert isinstance(tx, TransactionRecord)
                fragments.append(("", "{} {} {} {}".format(tx.txid, tx.server_profile.user,
                                                           tx.status, tx.current_query)))
                fragments.append(("", "\n"))
            return fragments

    def run(self):
        self.scanner.start()
        self.app.run()


class Scanner(Thread):

    def __init__(self, monitor):
        super(Scanner, self).__init__()
        self.monitor = monitor
        self._running = False

    def run(self):
        self._running = True
        while self._running:
            self._scan()
            sleep(0.5)

    def stop(self):
        self._running = False

    def _scan(self):
        # TODO: allow manual scans
        base_profile = self.monitor.profile
        main_server = self.monitor.servers[base_profile]  # TODO: if the main address goes away
        main_server.scan(with_overview=True)
        for uuid, data in main_server.overview.items():
            address = data["addresses"][base_profile.scheme]
            profile = ConnectionProfile(base_profile, address=address)
            if profile not in self.monitor.servers:
                self.monitor.servers[profile] = ServerScanner(profile)
            self.monitor.servers[profile].scan()
        self.monitor.update()


class ServerScanner(object):

    def __init__(self, profile):
        self.lock = Lock()
        self.profile = profile
        self.connection = Connection.open(self.profile)
        self.available = None
        self.neo4j_version = None
        self.neo4j_edition = None
        self.overview = None
        self.config = None
        self.connections = None
        self.transactions = None
        self.queries = None
        self.management_data = None
        self.protocol_version = None
        self.user_agent = None
        self.server_agent = None

    def scan(self, with_overview=False):
        if self.lock.acquire(blocking=False):
            try:
                self._scan(with_overview)
            finally:
                self.lock.release()
            #try:
            #    if self.connection is None or self.connection.closed or self.connection.broken:
            #        self.connection = Connection.open(self.profile)
            #        self._scan(with_overview)
            #    else:
            #        try:
            #            self._scan(with_overview)
            #        except (ConnectionUnavailable, BrokenTransactionError):
            #            self.connection.close()
            #            self.connection = Connection.open(self.profile)
            #            self._scan(with_overview)
            #except (ConnectionUnavailable, BrokenTransactionError):
            #    self.available = False
            #    self.connection.close()
            #else:
            #    self.available = True

    def _scan(self, with_overview=False):
        self.neo4j_version = self.connection.neo4j_version
        self.neo4j_edition = self.connection.neo4j_edition
        tx = self.connection.begin(None)
        try:
            if with_overview:
                try:
                    self.overview = self.connection.get_cluster_overview(tx)
                except TypeError:
                    self.overview = {None: {"addresses":
                                                {self.profile.scheme: self.profile.address}}}
            self.config = self.connection.get_config(tx)
            self.connections = self.connection.get_connections(tx)
            self.transactions = self.connection.get_transactions(tx)
            self.queries = self.connection.get_queries(tx)
            self.management_data = self.connection.get_management_data(tx)
            self.protocol_version = self.connection.protocol_version
            self.user_agent = self.connection.user_agent
            self.server_agent = self.connection.server_agent
        except (ConnectionUnavailable, BrokenTransactionError):
            self.connection.rollback(tx)
            raise
        else:
            self.connection.commit(tx)

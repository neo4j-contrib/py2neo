#!/usr/bin/env python
# -*- encoding: utf-8 -*-

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


from pytest import mark, fixture, raises

from py2neo.client import Connection as _Connection
from py2neo.pep249 import connect, OperationalError, ProgrammingError


class BrokenConnection(_Connection):

    def __init__(self):
        super(BrokenConnection, self).__init__(None, None)

    @property
    def closed(self):
        return False

    @property
    def broken(self):
        return True

    @property
    def local_port(self):
        return 0

    @property
    def bytes_sent(self):
        return 0

    @property
    def bytes_received(self):
        return 0


@fixture(scope="function")
def clean_db(request):
    request.cls.cx = cx = connect()
    cx.execute("MATCH (_) DETACH DELETE _")
    assert list(cx.execute("MATCH (_) RETURN count(_)")) == [(0,)]
    cx.commit()
    yield
    cx.close()


@mark.usefixtures("clean_db")
class TestConnection(object):

    def test_commit(self):
        self.cx.execute("CREATE ()")
        self.cx.commit()
        assert list(self.cx.execute("MATCH (_) RETURN count(_)")) == [(1,)]

    def test_commit_is_idempotent(self):
        self.cx.execute("CREATE ()")
        self.cx.commit()
        self.cx.commit()
        assert list(self.cx.execute("MATCH (_) RETURN count(_)")) == [(1,)]

    def test_rollback(self):
        self.cx.execute("CREATE ()")
        self.cx.rollback()
        assert list(self.cx.execute("MATCH (_) RETURN count(_)")) == [(0,)]

    def test_rollback_is_idempotent(self):
        self.cx.execute("CREATE ()")
        self.cx.rollback()
        self.cx.rollback()
        assert list(self.cx.execute("MATCH (_) RETURN count(_)")) == [(0,)]

    def test_cursor_open_close(self):
        cur = self.cx.cursor()
        assert cur.connection is self.cx
        cur.close()

    def test_execute(self):
        assert list(self.cx.execute("RETURN 1")) == [(1,)]

    def test_connection_failure(self):
        with raises(OperationalError):
            _ = connect("bolt://no.such.server:666")

    def test_close(self):
        self.cx.close()

    def test_close_is_idempotent(self):
        self.cx.close()
        self.cx.close()

    def test_close_after_break(self):
        self.cx._cx = BrokenConnection()
        self.cx.close()

    def test_commit_after_close(self):
        self.cx.close()
        with raises(ProgrammingError):
            self.cx.commit()

    def test_commit_after_break(self):
        self.cx._cx = BrokenConnection()
        with raises(OperationalError):
            self.cx.commit()

    def test_rollback_after_close(self):
        self.cx.close()
        with raises(ProgrammingError):
            self.cx.rollback()

    def test_rollback_after_break(self):
        self.cx._cx = BrokenConnection()
        with raises(OperationalError):
            self.cx.rollback()

    def test_cursor_after_close(self):
        self.cx.close()
        with raises(ProgrammingError):
            _ = self.cx.cursor()

    def test_cursor_after_break(self):
        self.cx._cx = BrokenConnection()
        with raises(OperationalError):
            _ = self.cx.cursor()

    def test_execute_after_close(self):
        self.cx.close()
        with raises(ProgrammingError):
            _ = self.cx.execute("RETURN 1")

    def test_execute_after_break(self):
        self.cx._cx = BrokenConnection()
        with raises(OperationalError):
            _ = self.cx.execute("RETURN 1")

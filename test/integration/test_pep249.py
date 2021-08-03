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


from datetime import date, time, datetime

from pytest import mark, fixture, raises

from py2neo import Neo4jError
from py2neo.client import Connection as _Connection
from py2neo.pep249 import connect, OperationalError, ProgrammingError, \
    DateFromTicks, TimeFromTicks, TimestampFromTicks


class BrokenConnection(_Connection):

    def __init__(self):
        super(BrokenConnection, self).__init__(None, None)

    @property
    def closed(self):
        return False

    @property
    def broken(self):
        return True


class DodgyConnection(_Connection):

    def __init__(self):
        super(DodgyConnection, self).__init__(None, None)

    @property
    def closed(self):
        return False

    @property
    def broken(self):
        return False

    def begin(self, *args, **kwargs):
        raise Neo4jError.hydrate({"code": "Neo.DatabaseError.General.UnknownError",
                                  "description": "A fake error has occurred"})

    def commit(self, tx):
        raise Neo4jError.hydrate({"code": "Neo.DatabaseError.General.UnknownError",
                                  "description": "A fake error has occurred"})

    def rollback(self, tx):
        raise Neo4jError.hydrate({"code": "Neo.DatabaseError.General.UnknownError",
                                  "description": "A fake error has occurred"})


@fixture(scope="function")
def clean_db(request):
    request.cls.con = con = connect()
    con.execute("MATCH (_) DETACH DELETE _")
    assert list(con.execute("MATCH (_) RETURN count(_)")) == [(0,)]
    con.commit()
    yield
    con.close()


@fixture(scope="function")
def cursor_1_to_10(request, clean_db):
    request.cls.cur = cursor = request.cls.con.cursor()
    cursor.execute("UNWIND range(1, 10) AS n RETURN n")
    yield
    cursor.close()


def test_date_from_ticks():
    assert isinstance(DateFromTicks(0), date)


def test_time_from_ticks():
    assert isinstance(TimeFromTicks(0), time)


def test_timestamp_from_ticks():
    assert isinstance(TimestampFromTicks(0), datetime)


@mark.usefixtures("clean_db")
class TestConnection(object):

    def test_begin_with_error(self):
        self.con._cx = DodgyConnection()
        with raises(OperationalError):
            self.con.begin()

    def test_commit(self):
        self.con.execute("CREATE ()")
        self.con.commit()
        assert list(self.con.execute("MATCH (_) RETURN count(_)")) == [(1,)]

    def test_commit_is_idempotent(self):
        self.con.execute("CREATE ()")
        self.con.commit()
        self.con.commit()
        assert list(self.con.execute("MATCH (_) RETURN count(_)")) == [(1,)]

    def test_commit_after_close(self):
        self.con.close()
        with raises(ProgrammingError):
            self.con.commit()

    def test_commit_after_break(self):
        self.con._cx = BrokenConnection()
        with raises(OperationalError):
            self.con.commit()

    def test_commit_with_error(self):
        self.con.execute("CREATE ()")
        self.con._cx = DodgyConnection()
        with raises(OperationalError):
            self.con.commit()

    def test_in_transaction_before_and_after_commit(self):
        assert self.con.in_transaction is False
        self.con.execute("CREATE ()")
        assert self.con.in_transaction is True
        self.con.commit()
        assert self.con.in_transaction is False

    def test_rollback(self):
        self.con.execute("CREATE ()")
        self.con.rollback()
        assert list(self.con.execute("MATCH (_) RETURN count(_)")) == [(0,)]

    def test_rollback_is_idempotent(self):
        self.con.execute("CREATE ()")
        self.con.rollback()
        self.con.rollback()
        assert list(self.con.execute("MATCH (_) RETURN count(_)")) == [(0,)]

    def test_rollback_after_close(self):
        self.con.close()
        with raises(ProgrammingError):
            self.con.rollback()

    def test_rollback_after_break(self):
        self.con._cx = BrokenConnection()
        with raises(OperationalError):
            self.con.rollback()

    def test_rollback_with_error(self):
        self.con.execute("CREATE ()")
        self.con._cx = DodgyConnection()
        with raises(OperationalError):
            self.con.rollback()

    def test_in_transaction_before_and_after_rollback(self):
        assert self.con.in_transaction is False
        self.con.execute("CREATE ()")
        assert self.con.in_transaction is True
        self.con.rollback()
        assert self.con.in_transaction is False

    def test_cursor_open_close(self):
        cur = self.con.cursor()
        assert cur.connection is self.con
        cur.close()

    def test_cursor_after_close(self):
        self.con.close()
        with raises(ProgrammingError):
            _ = self.con.cursor()

    def test_cursor_after_break(self):
        self.con._cx = BrokenConnection()
        with raises(OperationalError):
            _ = self.con.cursor()

    def test_execute(self):
        assert list(self.con.execute("RETURN 1")) == [(1,)]

    def test_execute_without_consume(self):
        self.con.execute("RETURN 1")
        assert list(self.con.execute("RETURN 2")) == [(2,)]

    def test_execute_with_params(self):
        assert list(self.con.execute("RETURN $x", {"x": 1})) == [(1,)]

    def test_execute_after_close(self):
        self.con.close()
        with raises(ProgrammingError):
            _ = self.con.execute("RETURN 1")

    def test_execute_after_break(self):
        self.con._cx = BrokenConnection()
        with raises(OperationalError):
            _ = self.con.execute("RETURN 1")

    def test_executemany(self):
        self.con.executemany("CREATE ({x: $x})", [{"x": 1}, {"x": 2}])

    def test_executemany_after_close(self):
        self.con.close()
        with raises(ProgrammingError):
            self.con.executemany("CREATE ({x: $x})", [{"x": 1}, {"x": 2}])

    def test_executemany_after_break(self):
        self.con._cx = BrokenConnection()
        with raises(OperationalError):
            self.con.executemany("CREATE ({x: $x})", [{"x": 1}, {"x": 2}])

    def test_close(self):
        self.con.close()

    def test_close_is_idempotent(self):
        self.con.close()
        self.con.close()

    def test_close_after_break(self):
        self.con._cx = BrokenConnection()
        self.con.close()

    def test_close_after_execute_with_error(self):
        self.con.execute("CREATE ()")
        self.con._cx = DodgyConnection()
        self.con.close()

    def test_connection_failure(self):
        with raises(OperationalError):
            _ = connect("bolt://localhost:666")

    def test_bad_query(self):
        with raises(OperationalError):
            self.con.execute("X")


@mark.usefixtures("cursor_1_to_10")
class TestCursor(object):

    def test_connection(self):
        assert self.cur.connection is self.con

    def test_connection_after_cursor_close(self):
        self.cur.close()
        assert self.cur.connection is self.con

    def test_connection_after_connection_close(self):
        self.con.close()
        assert self.cur.connection is self.con

    def test_description(self):
        assert self.cur.description == [("n", None, None, None, None, None, None)]

    def test_description_after_cursor_close(self):
        self.cur.close()
        assert self.cur.description == [("n", None, None, None, None, None, None)]

    def test_description_after_connection_close(self):
        self.con.close()
        assert self.cur.description == [("n", None, None, None, None, None, None)]

    def test_description_after_break(self):
        self.con._cx = BrokenConnection()
        assert self.cur.description == [("n", None, None, None, None, None, None)]

    def test_description_before_execute(self):
        cur = self.con.cursor()
        assert cur.description is None

    def test_rowcount(self):
        assert self.cur.rowcount == -1

    def test_rowcount_after_cursor_close(self):
        self.cur.close()
        assert self.cur.rowcount == -1

    def test_rowcount_after_connection_close(self):
        self.con.close()
        assert self.cur.rowcount == -1

    def test_rowcount_after_break(self):
        self.con._cx = BrokenConnection()
        assert self.cur.rowcount == -1

    def test_summary(self):
        summary = self.cur.summary
        assert isinstance(summary, dict)
        assert len(summary) > 0

    def test_summary_after_cursor_close(self):
        self.cur.close()
        summary = self.cur.summary
        assert isinstance(summary, dict)
        assert len(summary) > 0

    def test_summary_after_connection_close(self):
        self.con.close()
        summary = self.cur.summary
        assert isinstance(summary, dict)
        assert len(summary) > 0

    def test_summary_after_break(self):
        self.con._cx = BrokenConnection()
        summary = self.cur.summary
        assert isinstance(summary, dict)
        assert len(summary) > 0

    def test_summary_before_execute(self):
        cur = self.con.cursor()
        assert cur.summary is None

    def test_iter(self):
        assert list(self.cur) == [(1,), (2,), (3,), (4,), (5,),
                                  (6,), (7,), (8,), (9,), (10,)]

    def test_iter_after_cursor_close(self):
        self.cur.close()
        with raises(ProgrammingError):
            _ = list(self.cur)

    def test_iter_after_connection_close(self):
        self.con.close()
        with raises(ProgrammingError):
            _ = list(self.cur)

    def test_iter_after_break(self):
        self.con._cx = BrokenConnection()
        with raises(OperationalError):
            _ = list(self.cur)

    def test_iter_before_execute(self):
        cursor = self.con.cursor()
        assert list(cursor) == []

    def test_close(self):
        self.cur.close()

    def test_close_is_idempotent(self):
        self.cur.close()
        self.cur.close()

    def test_close_after_break(self):
        self.con._cx = BrokenConnection()
        self.cur.close()

    def test_execute(self):
        assert list(self.cur.execute("RETURN 1")) == [(1,)]

    def test_execute_without_consume(self):
        self.cur.execute("RETURN 1")
        assert list(self.cur.execute("RETURN 2")) == [(2,)]

    def test_execute_with_params(self):
        assert list(self.cur.execute("RETURN $x", {"x": 1})) == [(1,)]

    def test_execute_after_cursor_close(self):
        self.cur.close()
        with raises(ProgrammingError):
            _ = self.cur.execute("RETURN 1")

    def test_execute_after_connection_close(self):
        self.con.close()
        with raises(ProgrammingError):
            _ = self.cur.execute("RETURN 1")

    def test_execute_after_break(self):
        self.con._cx = BrokenConnection()
        with raises(OperationalError):
            _ = self.cur.execute("RETURN 1")

    def test_executemany(self):
        self.cur.executemany("CREATE ({x: $x})", [{"x": 1}, {"x": 2}])

    def test_executemany_after_cursor_close(self):
        self.cur.close()
        with raises(ProgrammingError):
            self.cur.executemany("CREATE ({x: $x})", [{"x": 1}, {"x": 2}])

    def test_executemany_after_connection_close(self):
        self.con.close()
        with raises(ProgrammingError):
            self.cur.executemany("CREATE ({x: $x})", [{"x": 1}, {"x": 2}])

    def test_executemany_after_break(self):
        self.con._cx = BrokenConnection()
        with raises(OperationalError):
            self.cur.executemany("CREATE ({x: $x})", [{"x": 1}, {"x": 2}])

    def test_fetchone(self):
        assert self.cur.fetchone() == (1,)

    def test_fetchone_after_cursor_close(self):
        self.cur.close()
        with raises(ProgrammingError):
            _ = self.cur.fetchone()

    def test_fetchone_after_connection_close(self):
        self.con.close()
        with raises(ProgrammingError):
            _ = self.cur.fetchone()

    def test_fetchone_after_break(self):
        self.con._cx = BrokenConnection()
        with raises(OperationalError):
            _ = self.cur.fetchone()

    def test_fetchone_before_execute(self):
        cursor = self.con.cursor()
        assert cursor.fetchone() is None

    def test_fetchone_after_exhaustion(self):
        self.cur.fetchall()
        assert self.cur.fetchone() is None

    def test_fetchmany_with_size(self):
        assert self.cur.fetchmany(3) == [(1,), (2,), (3,)]

    def test_fetchmany_with_default_size(self):
        assert self.cur.arraysize == 1
        assert self.cur.fetchmany() == [(1,)]

    def test_fetchmany_with_arraysize(self):
        self.cur.arraysize = 3
        assert self.cur.fetchmany() == [(1,), (2,), (3,)]

    def test_fetchmany_after_cursor_close(self):
        self.cur.close()
        with raises(ProgrammingError):
            _ = self.cur.fetchmany()

    def test_fetchmany_after_connection_close(self):
        self.con.close()
        with raises(ProgrammingError):
            _ = self.cur.fetchmany()

    def test_fetchmany_after_break(self):
        self.con._cx = BrokenConnection()
        with raises(OperationalError):
            _ = self.cur.fetchmany()

    def test_fetchmany_before_execute(self):
        cursor = self.con.cursor()
        assert cursor.fetchmany() == []

    def test_fetchmany_after_exhaustion(self):
        self.cur.fetchall()
        assert self.cur.fetchmany() == []

    def test_fetchall(self):
        assert self.cur.fetchall() == [(1,), (2,), (3,), (4,), (5,),
                                       (6,), (7,), (8,), (9,), (10,)]

    def test_fetchall_after_cursor_close(self):
        self.cur.close()
        with raises(ProgrammingError):
            _ = self.cur.fetchall()

    def test_fetchall_after_connection_close(self):
        self.con.close()
        with raises(ProgrammingError):
            _ = self.cur.fetchall()

    def test_fetchall_after_break(self):
        self.con._cx = BrokenConnection()
        with raises(OperationalError):
            _ = self.cur.fetchall()

    def test_fetchall_before_execute(self):
        cursor = self.con.cursor()
        assert cursor.fetchall() == []

    def test_fetchall_after_exhaustion(self):
        self.cur.fetchall()
        assert self.cur.fetchall() == []

    def test_setinputsizes(self):
        self.cur.setinputsizes([])

    def test_setoutputsize(self):
        self.cur.setoutputsize(10)

    def test_bad_query(self):
        with raises(OperationalError):
            self.cur.execute("X")

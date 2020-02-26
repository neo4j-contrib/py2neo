#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright 2011-2020, Nigel Small
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


from __future__ import absolute_import

from collections import deque


class Task(object):

    def done(self):
        raise NotImplementedError

    def failed(self):
        raise NotImplementedError

    def audit(self):
        raise NotImplementedError


class ItemizedTask(Task):
    """ This class represents a form of dynamic checklist. Items may
    be added, up to a "final" item which marks the list as complete.
    Each item may then be marked as done.
    """

    def __init__(self):
        super(ItemizedTask, self).__init__()
        self._items = deque()
        self._complete = False

    def __bool__(self):
        return not self.done() and not self.failed()

    def items(self):
        return iter(self._items)

    def append(self, item, final=False):
        self._items.append(item)
        if final:
            self.set_complete()

    def set_complete(self):
        self._complete = True

    def complete(self):
        """ Flag to indicate whether all items have been appended to
        this task, whether or not they are done.
        """
        return self._complete

    def first(self):
        try:
            return self._items[0]
        except IndexError:
            return None

    def last(self):
        try:
            return self._items[-1]
        except IndexError:
            return None

    def done(self):
        """ Flag to indicate whether the list of items is complete and
        all items are done.
        """
        if self.complete():
            last = self.last()
            return (last and last.done()) or not last
        else:
            return False

    def failed(self):
        return any(item.failed() for item in self._items)

    def audit(self):
        for item in self._items:
            item.audit()


class Transaction(ItemizedTask):

    def __init__(self, db=None, readonly=False, bookmarks=None, metadata=None, timeout=None):
        super(Transaction, self).__init__()
        self.db = db
        self.readonly = readonly
        self.bookmarks = bookmarks
        self.metadata = metadata
        self.timeout = timeout

    @property
    def extra(self):
        extra = {}
        if self.db:
            extra["db"] = self.db
        if self.readonly:
            extra["mode"] = "r"
        # TODO: other extras
        return extra


class Result(object):

    def __init__(self):
        super(Result, self).__init__()

    def has_records(self):
        raise NotImplementedError

    def take_record(self):
        raise NotImplementedError


class TransactionError(Exception):

    pass


class Failure(Exception):

    def __init__(self, message, code):
        super(Failure, self).__init__(message)
        self.code = code
        _, self.classification, self.category, self.title = self.code.split(".")

    def __str__(self):
        return "[%s] %s" % (self.code, super(Failure, self).__str__())

    @property
    def message(self):
        return self.args[0]

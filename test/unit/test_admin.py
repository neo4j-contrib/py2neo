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


from os import remove, rmdir
from os.path import join as path_join
from tempfile import NamedTemporaryFile, mkdtemp
from unittest import TestCase

from py2neo.admin import AuthFile, AuthUser


class AuthFileTestCase(TestCase):

    def test_can_add_to_empty_file(self):
        with NamedTemporaryFile() as tf:
            af = AuthFile(tf.name)
            af.update("test", "123456")
            users = list(af)
            self.assertEqual(len(users), 1)
            user = users[0]
            self.assertIsInstance(user, AuthUser)
            self.assertTrue(repr(user).startswith("<AuthUser"))
            self.assertEqual(user.name, b"test")
            self.assertEqual(user.hash_algorithm, b"SHA-256")
            self.assertTrue(user.check_password("123456"))

    def test_can_create_file(self):
        td = mkdtemp()
        try:
            tf_name = path_join(td, "auth")
            try:
                af = AuthFile(tf_name)
                af.update("test", "123456")
                users = list(af)
                self.assertEqual(len(users), 1)
                user = users[0]
                self.assertIsInstance(user, AuthUser)
                self.assertEqual(user.name, b"test")
                self.assertEqual(user.hash_algorithm, b"SHA-256")
                self.assertTrue(user.check_password("123456"))
            finally:
                remove(tf_name)
        finally:
            rmdir(td)

    def test_can_create_path(self):
        td = mkdtemp()
        try:
            tf_dir = path_join(td, "subdir")
            tf_name = path_join(tf_dir, "auth")
            try:
                af = AuthFile(tf_name)
                af.update("test", "123456")
                users = list(af)
                self.assertEqual(len(users), 1)
                user = users[0]
                self.assertIsInstance(user, AuthUser)
                self.assertEqual(user.name, b"test")
                self.assertEqual(user.hash_algorithm, b"SHA-256")
                self.assertTrue(user.check_password("123456"))
            finally:
                remove(tf_name)
                rmdir(tf_dir)
        finally:
            rmdir(td)

    def test_can_update_existing_user(self):
        with NamedTemporaryFile() as tf:
            af = AuthFile(tf.name)
            af.update("test", "123456")
            af.update("test", "987654")
            users = list(af)
            self.assertEqual(len(users), 1)
            user = users[0]
            self.assertIsInstance(user, AuthUser)
            self.assertEqual(user.name, b"test")
            self.assertEqual(user.hash_algorithm, b"SHA-256")
            self.assertTrue(user.check_password("987654"))

    def test_can_add_after_existing_user(self):
        with NamedTemporaryFile() as tf:
            af = AuthFile(tf.name)
            af.update("test1", "123456")
            af.update("test2", "987654")
            users = list(af)
            self.assertEqual(len(users), 2)
            user = users[1]
            self.assertIsInstance(user, AuthUser)
            self.assertEqual(user.name, b"test2")
            self.assertEqual(user.hash_algorithm, b"SHA-256")
            self.assertTrue(user.check_password("987654"))

    def test_can_remove_user(self):
        with NamedTemporaryFile() as tf:
            af = AuthFile(tf.name)
            af.update("test1", "123456")
            af.update("test2", "987654")
            af.remove("test1")
            users = list(af)
            self.assertEqual(len(users), 1)
            user = users[0]
            self.assertIsInstance(user, AuthUser)
            self.assertEqual(user.name, b"test2")
            self.assertEqual(user.hash_algorithm, b"SHA-256")
            self.assertTrue(user.check_password("987654"))

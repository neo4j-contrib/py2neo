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


from platform import python_implementation
from unittest import TestCase, skipIf

from py2neo.caching import ThreadLocalEntityCache


IMPLEMENTATION = python_implementation()


class Entity(object):

    def __repr__(self):
        return str(id(self))[-5:]


class EntityCacheTestCase(TestCase):

    def test_update_with_value_constructor_where_key_does_not_exist(self):
        # Given
        cache = ThreadLocalEntityCache()

        # When
        key = "X"
        value = cache.update(key, Entity)

        # Then a new value should have been created, inserted and extracted
        assert key in cache._dict
        assert isinstance(value, Entity)

    def test_update_with_value_constructor_where_key_already_exists(self):
        # Given
        cache = ThreadLocalEntityCache()
        key = "X"
        old_value = Entity()
        cache._dict[key] = old_value

        # When
        value = cache.update(key, Entity)

        # Then the old value should be extracted
        assert key in cache._dict
        assert value is old_value

    def test_update_with_value_where_key_does_not_exist(self):
        # Given
        cache = ThreadLocalEntityCache()
        key = "X"

        # When
        new_value = Entity()
        value = cache.update(key, new_value)

        # Then the new value should have been inserted
        assert key in cache._dict
        assert value is new_value

    def test_update_with_value_where_key_already_exists(self):
        # Given
        cache = ThreadLocalEntityCache()
        key = "X"
        old_value = Entity()
        cache._dict[key] = old_value

        # When
        new_value = Entity()
        value = cache.update(key, new_value)

        # Then the old value should have been replaced by the new value
        assert key in cache._dict
        assert value is new_value

    def test_update_with_none_where_key_does_not_exist(self):
        # Given
        cache = ThreadLocalEntityCache()
        key = "X"

        # When
        cache.update(key, None)

        # Then the key should still not exist in the cache
        assert key not in cache._dict

    def test_update_with_none_where_key_already_exists(self):
        # Given
        cache = ThreadLocalEntityCache()
        key = "X"
        value = Entity()
        cache._dict[key] = value

        # When
        cache.update(key, None)

        # Then the key should no longer exist in the cache
        assert key not in cache._dict

    @skipIf(IMPLEMENTATION == "PyPy", "Test not supported in PyPy yet")
    def test_implicit_removal_by_value_deletion(self):
        # Given
        cache = ThreadLocalEntityCache()
        key = "X"
        value = Entity()
        cache._dict[key] = value

        # When
        del value

        # Then the key should no longer exist in the cache
        assert key not in cache._dict

    @skipIf(IMPLEMENTATION == "PyPy", "Test not supported in PyPy yet")
    def test_threaded_usage(self):
        from random import choice, randint
        from threading import Event, Lock, Thread
        from time import time

        cache = ThreadLocalEntityCache()

        keys = list("ABCDEFGHIJKLMNOPQRSTUVWXYZ")
        values = []
        values_lock = Lock()

        class CacheUserThread(Thread):

            def __init__(self, *args, **kwargs):
                super(CacheUserThread, self).__init__(*args, **kwargs)
                self.stopped = Event()

            def run(self):
                self.stopped.wait()

            def stop(self):
                self.stopped.set()

            def merge_random_key_and_new_value(self):
                key = choice(keys)
                value = cache.update(key, Entity())
                with values_lock:
                    values.append(value)

            def merge_random_key_and_old_value(self):
                key = choice(keys)
                with values_lock:
                    if values:
                        value = choice(values)
                    else:
                        return
                cache.update(key, value)

            def merge_random_key_and_constructor(self):
                key = choice(keys)
                value = cache.update(key, Entity)
                with values_lock:
                    if value not in values:
                        values.append(value)

            def remove_random_key(self):
                key = choice(keys)
                cache.update(key, None)

            def delete_random_values(self):
                with values_lock:
                    del_count = randint(0, len(values) // 3)
                    while values and del_count:
                        index = randint(0, len(values) - 1)
                        values.pop(index)
                        del_count -= 1
                        # print("[%s] Deleted value %d" % (self.name, index))

            def assert_integrity(self):
                with values_lock:
                    with cache.lock:
                        for key, value in cache._dict.items():
                            assert key in keys
                            assert value in values

            actions = [merge_random_key_and_new_value, merge_random_key_and_new_value,
                       merge_random_key_and_old_value, merge_random_key_and_old_value,
                       merge_random_key_and_constructor, merge_random_key_and_constructor,
                       remove_random_key, delete_random_values, assert_integrity]

            def perform_random_action(self):
                action = choice(self.actions)
                action(self)

        threads = []
        n_threads = 40
        for i in range(n_threads):
            thread = CacheUserThread(name="T%02d" % (i + 1))
            thread.start()
            threads.append(thread)

        try:
            t0 = time()
            count = 0
            while time() - t0 < 3:
                choice(threads).perform_random_action()
                count += 1
            print("Performed %d actions across %d threads in %.03fs" %
                  (count, n_threads, time() - t0))

        finally:
            for thread in threads:
                thread.stop()
            while threads:
                threads.pop().join()

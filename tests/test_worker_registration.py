import unittest
from unittest.mock import patch

from rq import Queue, ForkWorker
from rq.utils import ceildiv
from rq.defaults import REDIS_WORKER_KEYS
from rq.defaults import WORKERS_BY_QUEUE_KEY
from rq.worker import Worker, ThreadPoolWorker
from tests import RQTestCase


class TestWorkerRegistry(RQTestCase):
    def test_worker_registration(self):
        """Ensure worker.key is correctly set in Redis."""
        foo_queue = Queue(name='foo')
        bar_queue = Queue(name='bar')
        worker = ForkWorker([foo_queue, bar_queue])

        Worker.register(worker)
        redis = worker.connection

        self.assertTrue(redis.sismember(worker.redis_workers_keys, worker.key))
        self.assertEqual(self.rq.get_workers_count(), 1)
        self.assertTrue(redis.sismember(WORKERS_BY_QUEUE_KEY % foo_queue.name, worker.key))
        self.assertEqual(self.rq.get_workers_count(queue=foo_queue), 1)
        self.assertTrue(redis.sismember(WORKERS_BY_QUEUE_KEY % bar_queue.name, worker.key))
        self.assertEqual(self.rq.get_workers_count(queue=bar_queue), 1)

        Worker.unregister(worker)
        self.assertFalse(redis.sismember(worker.redis_workers_keys, worker.key))
        self.assertFalse(redis.sismember(WORKERS_BY_QUEUE_KEY % foo_queue.name, worker.key))
        self.assertFalse(redis.sismember(WORKERS_BY_QUEUE_KEY % bar_queue.name, worker.key))

    def test_get_keys_by_queue(self):
        """get_keys_by_queue only returns active workers for that queue"""
        foo_queue = Queue(name='foo')
        bar_queue = Queue(name='bar')
        baz_queue = Queue(name='baz')

        worker1 = ForkWorker([foo_queue, bar_queue])
        worker2 = ForkWorker([foo_queue])
        worker3 = ForkWorker([baz_queue])

        self.assertEqual(set(), Worker.get_keys(foo_queue))

        Worker.register(worker1)
        Worker.register(worker2)
        Worker.register(worker3)

        # get_keys(queue) will return worker keys for that queue
        self.assertEqual(set([worker1.key, worker2.key]), Worker.get_keys(foo_queue))
        self.assertEqual(set([worker1.key]), Worker.get_keys(bar_queue))

        # get_keys(connection=connection) will return all worker keys
        self.assertEqual(set([worker1.key, worker2.key, worker3.key]), Worker.get_keys(connection=worker1.connection))

        # Calling get_keys without arguments raises an exception
        self.assertRaises(ValueError, Worker.get_keys)

        Worker.unregister(worker1)
        Worker.unregister(worker2)
        Worker.unregister(worker3)

    def test_clean_registry(self):
        """clean_registry removes worker keys that don't exist in Redis"""
        queue = Queue(name='foo')
        worker = ForkWorker([queue])

        Worker.register(worker)
        redis = worker.connection

        self.assertTrue(redis.sismember(worker.redis_workers_keys, worker.key))
        self.assertTrue(redis.sismember(REDIS_WORKER_KEYS, worker.key))

        Worker.clean_worker_registry(queue)
        self.assertFalse(redis.sismember(worker.redis_workers_keys, worker.key))
        self.assertFalse(redis.sismember(REDIS_WORKER_KEYS, worker.key))

    def test_clean_large_registry(self):
        """
        clean_registry() splits invalid_keys into multiple lists for set removal to avoid sending more than redis can
        receive
        """
        MAX_WORKERS = 41
        MAX_KEYS = 37
        # srem is called twice per invalid key batch: once for WORKERS_BY_QUEUE_KEY; once for REDIS_WORKER_KEYS
        SREM_CALL_COUNT = 2

        queue = Queue(name='foo')
        for i in range(MAX_WORKERS):
            worker = ForkWorker([queue])
            Worker.register(worker)

        with patch('rq.defaults.MAX_KEYS', MAX_KEYS), patch.object(
            queue.connection, 'pipeline', wraps=queue.connection.pipeline
        ) as pipeline_mock:
            # clean_worker_registry creates a pipeline with a context manager. Configure the mock using the context
            # manager entry method __enter__
            pipeline_mock.return_value.__enter__.return_value.srem.return_value = None
            pipeline_mock.return_value.__enter__.return_value.execute.return_value = [0] * MAX_WORKERS

            Worker.clean_worker_registry(queue)

            expected_call_count = (ceildiv(MAX_WORKERS, MAX_KEYS)) * SREM_CALL_COUNT
            self.assertEqual(pipeline_mock.return_value.__enter__.return_value.srem.call_count, expected_call_count)


class TestThreadPoolWorkerRegistry(RQTestCase):

    def test_worker_registration(self):
        """Ensure worker.key is correctly set in Redis."""
        foo_queue = Queue(name='foo')
        bar_queue = Queue(name='bar')
        worker = ThreadPoolWorker([foo_queue, bar_queue])

        Worker.register(worker)
        redis = worker.connection

        self.assertTrue(redis.sismember(worker.redis_workers_keys, worker.key))
        self.assertEqual(self.rq.get_workers_count(), 1)
        self.assertTrue(
            redis.sismember(WORKERS_BY_QUEUE_KEY % foo_queue.name, worker.key)
        )
        self.assertEqual(self.rq.get_workers_count(queue=foo_queue), 1)
        self.assertTrue(
            redis.sismember(WORKERS_BY_QUEUE_KEY % bar_queue.name, worker.key)
        )
        self.assertEqual(self.rq.get_workers_count(queue=bar_queue), 1)

        Worker.unregister(worker)
        self.assertFalse(redis.sismember(worker.redis_workers_keys, worker.key))
        self.assertFalse(
            redis.sismember(WORKERS_BY_QUEUE_KEY % foo_queue.name, worker.key)
        )
        self.assertFalse(
            redis.sismember(WORKERS_BY_QUEUE_KEY % bar_queue.name, worker.key)
        )

    def test_get_keys_by_queue(self):
        """get_keys_by_queue only returns active workers for that queue"""
        foo_queue = Queue(name='foo')
        bar_queue = Queue(name='bar')
        baz_queue = Queue(name='baz')

        worker1 = ThreadPoolWorker([foo_queue, bar_queue])
        worker2 = ThreadPoolWorker([foo_queue])
        worker3 = ThreadPoolWorker([baz_queue])

        self.assertEqual(set(), Worker.get_keys(foo_queue))

        Worker.register(worker1)
        Worker.register(worker2)
        Worker.register(worker3)

        # get_keys(queue) will return worker keys for that queue
        self.assertEqual(
            set([worker1.key, worker2.key]),
            Worker.get_keys(foo_queue)
        )
        self.assertEqual(set([worker1.key]), Worker.get_keys(bar_queue))

        # get_keys(connection=connection) will return all worker keys
        self.assertEqual(
            set([worker1.key, worker2.key, worker3.key]),
            Worker.get_keys(connection=worker1.connection)
        )

        # Calling get_keys without arguments raises an exception
        self.assertRaises(ValueError, Worker.get_keys)

        Worker.unregister(worker1)
        Worker.unregister(worker2)
        Worker.unregister(worker3)

    def test_clean_registry(self):
        """clean_registry removes worker keys that don't exist in Redis"""
        queue = Queue(name='foo')
        worker = ThreadPoolWorker([queue])

        Worker.register(worker)
        redis = worker.connection

        self.assertTrue(redis.sismember(worker.redis_workers_keys, worker.key))
        self.assertTrue(redis.sismember(REDIS_WORKER_KEYS, worker.key))

        Worker.clean_worker_registry(queue)
        self.assertFalse(redis.sismember(worker.redis_workers_keys, worker.key))
        self.assertFalse(redis.sismember(REDIS_WORKER_KEYS, worker.key))

    @unittest.skip("ThreadPoolWorker registry WIP")
    def test_clean_large_registry(self):
        """
        clean_registry() splits invalid_keys into multiple lists for set removal to avoid sending more than redis can
        receive
        """
        MAX_WORKERS = 41
        MAX_KEYS = 37
        # srem is called twice per invalid key batch: once for WORKERS_BY_QUEUE_KEY; once for REDIS_WORKER_KEYS
        SREM_CALL_COUNT = 2

        queue = Queue(name='foo')
        for i in range(MAX_WORKERS):
            worker = ThreadPoolWorker([queue])
            Worker.register(worker)

        with patch('rq.defaults.MAX_KEYS', MAX_KEYS), \
             patch.object(queue.connection, 'pipeline', wraps=queue.connection.pipeline) as pipeline_mock:
            # clean_worker_registry creates a pipeline with a context manager. Configure the mock using the context
            # manager entry method __enter__
            pipeline_mock.return_value.__enter__.return_value.srem.return_value = None
            pipeline_mock.return_value.__enter__.return_value.execute.return_value = [0] * MAX_WORKERS

            Worker.clean_worker_registry(queue)

            expected_call_count = (ceildiv(MAX_WORKERS, MAX_KEYS)) * SREM_CALL_COUNT
            self.assertEqual(pipeline_mock.return_value.__enter__.return_value.srem.call_count, expected_call_count)

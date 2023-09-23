import json
import os
import signal
import subprocess
import sys
import threading
import time
import unittest
import zlib
import psutil
import pytest
import redis.exceptions

from datetime import datetime, timedelta
from multiprocessing import Process
from time import sleep
from unittest import mock, skipIf
from unittest.mock import Mock
from redis import Redis

from rq import Queue, Worker, ForkWorker, get_current_connection
from rq.const import ResultType
from rq.defaults import DEFAULT_MAINTENANCE_TASK_INTERVAL
from rq.job import Job, JobStatus, Retry
from rq.registries import FailedJobRegistry, FinishedJobRegistry, StartedJobRegistry
from rq.results import Result
from rq.serializers import JSONSerializer
from rq.suspension import resume, suspend
from rq.utils import as_text, get_version, utcnow
from rq.version import VERSION
from rq.worker import WorkerStatus
from tests import RQTestCase
from tests.fixtures import (
    CustomJob,
    CustomQueue,
    access_self,
    create_file,
    create_file_after_timeout,
    div_by_zero,
    do_nothing,
    kill_worker,
    launch_process_within_worker_and_store_pid,
    long_running_job,
    modify_self,
    modify_self_and_error,
    raise_exc_mock,
    save_key_ttl,
    say_hello,
    say_pid,
)



class TestWorker(RQTestCase):
    def test_create_worker(self):
        """Worker creation using various inputs."""

        # With single string argument
        w = ForkWorker('foo')
        self.assertEqual(w.queues[0].name, 'foo')

        # With list of strings
        w = ForkWorker(['foo', 'bar'])
        self.assertEqual(w.queues[0].name, 'foo')
        self.assertEqual(w.queues[1].name, 'bar')

        self.assertEqual(w.queue_keys(), [w.queues[0].key, w.queues[1].key])
        self.assertEqual(w.queue_names(), ['foo', 'bar'])

        # With iterable of strings
        w = ForkWorker(iter(['foo', 'bar']))
        self.assertEqual(w.queues[0].name, 'foo')
        self.assertEqual(w.queues[1].name, 'bar')

        # With single Queue
        w = ForkWorker(Queue('foo'))
        self.assertEqual(w.queues[0].name, 'foo')

        # With iterable of Queues
        w = ForkWorker(iter([Queue('foo'), Queue('bar')]))
        self.assertEqual(w.queues[0].name, 'foo')
        self.assertEqual(w.queues[1].name, 'bar')

        # With list of Queues
        w = ForkWorker([Queue('foo'), Queue('bar')])
        self.assertEqual(w.queues[0].name, 'foo')
        self.assertEqual(w.queues[1].name, 'bar')

        # With string and serializer
        w = ForkWorker('foo', serializer=json)
        self.assertEqual(w.queues[0].name, 'foo')

        # With queue having serializer
        w = ForkWorker(Queue('foo'), serializer=json)
        self.assertEqual(w.queues[0].name, 'foo')

    def test_work_and_quit(self):
        """Worker processes work, then quits."""
        fooq, barq = Queue('foo'), Queue('bar')
        w = ForkWorker([fooq, barq])
        self.assertEqual(w.work(burst=True), False, 'Did not expect any work on the queue.')

        fooq.enqueue(say_hello, name='Frank')
        self.assertEqual(w.work(burst=True), True, 'Expected at least some work done.')

    def test_work_and_quit_custom_serializer(self):
        """Worker processes work, then quits."""
        fooq, barq = Queue('foo', serializer=JSONSerializer), Queue('bar', serializer=JSONSerializer)
        w = ForkWorker([fooq, barq], serializer=JSONSerializer)
        self.assertEqual(w.work(burst=True), False, 'Did not expect any work on the queue.')

        fooq.enqueue(say_hello, name='Frank')
        self.assertEqual(w.work(burst=True), True, 'Expected at least some work done.')

    def test_worker_all(self):
        """Worker.all() works properly"""
        foo_queue = Queue('foo')
        bar_queue = Queue('bar')

        w1 = ForkWorker([foo_queue, bar_queue], name='w1')
        w1.register_birth()
        w2 = ForkWorker([foo_queue], name='w2')
        w2.register_birth()

        self.assertEqual(set(self.rq.get_workers(connection=foo_queue.connection)), set([w1, w2]))
        self.assertEqual(set(self.rq.get_workers(queue=foo_queue)), set([w1, w2]))
        self.assertEqual(set(self.rq.get_workers(queue=bar_queue)), set([w1]))

        w1.register_death()
        w2.register_death()

    def test_find_by_key(self):
        """Worker.load_by_key restores queues, state and job_id."""
        queues = [Queue('foo'), Queue('bar')]
        w = ForkWorker(queues)
        w.register_death()
        w.register_birth()
        w.set_state(WorkerStatus.STARTED)
        worker = ForkWorker.load_by_key(w.key)
        self.assertEqual(worker.queues, queues)
        self.assertEqual(worker.get_state(), WorkerStatus.STARTED)
        self.assertEqual(worker._job_id, None)
        self.assertTrue(worker.key in self.rq.get_workers_keys())
        self.assertEqual(worker.version, VERSION)

        # If worker is gone, its keys should also be removed
        worker.connection.delete(worker.key)
        ForkWorker.load_by_key(worker.key)
        self.assertFalse(worker.key in self.rq.get_workers_keys())

        self.assertRaises(ValueError, ForkWorker.load_by_key, 'foo')

    def test_worker_ttl(self):
        """Worker ttl."""
        w = ForkWorker([])
        w.register_birth()
        [worker_key] = self.testconn.smembers(ForkWorker.redis_workers_keys)
        self.assertIsNotNone(self.testconn.ttl(worker_key))
        w.register_death()

    def test_work_via_string_argument(self):
        """Worker processes work fed via string arguments."""
        q = Queue('foo')
        w = ForkWorker([q])
        job = q.enqueue('tests.fixtures.say_hello', name='Frank')
        self.assertEqual(w.work(burst=True), True, 'Expected at least some work done.')
        expected_result = 'Hi there, Frank!'
        self.assertEqual(job.result, expected_result)
        # Only run if Redis server supports streams
        if job.supports_redis_streams:
            self.assertEqual(Result.fetch_latest(job).return_value, expected_result)
        self.assertIsNone(job.worker_name)

    def test_job_times(self):
        """job times are set correctly."""
        q = Queue('foo')
        w = ForkWorker([q])
        before = utcnow()
        before = before.replace(microsecond=0)
        job = q.enqueue(say_hello)
        self.assertIsNotNone(job.enqueued_at)
        self.assertIsNone(job.started_at)
        self.assertIsNone(job.ended_at)
        self.assertEqual(w.work(burst=True), True, 'Expected at least some work done.')
        self.assertEqual(job.result, 'Hi there, Stranger!')
        after = utcnow()
        job.refresh()
        self.assertTrue(before <= job.enqueued_at <= after, 'Not %s <= %s <= %s' % (before, job.enqueued_at, after))
        self.assertTrue(before <= job.started_at <= after, 'Not %s <= %s <= %s' % (before, job.started_at, after))
        self.assertTrue(before <= job.ended_at <= after, 'Not %s <= %s <= %s' % (before, job.ended_at, after))

    def test_work_is_unreadable(self):
        """Unreadable jobs are put on the failed job registry."""
        q = Queue()
        self.assertEqual(q.count, 0)

        # NOTE: We have to fake this enqueueing for this test case.
        # What we're simulating here is a call to a function that is not
        # importable from the worker process.
        job = Job.create(func=div_by_zero, args=(3,), origin=q.name)
        job.save()

        job_data = job.data
        invalid_data = job_data.replace(b'div_by_zero', b'nonexisting')
        assert job_data != invalid_data
        self.testconn.hset(job.key, 'data', zlib.compress(invalid_data))

        # We use the low-level internal function to enqueue any data (bypassing
        # validity checks)
        q.push_job_id(job.id)

        self.assertEqual(q.count, 1)

        # All set, we're going to process it
        w = ForkWorker([q])
        w.work(burst=True)  # should silently pass
        self.assertEqual(q.count, 0)

        failed_job_registry = FailedJobRegistry(queue=q)
        self.assertTrue(job in failed_job_registry)

    def test_meta_is_unserializable(self):
        """Unserializable jobs are put on the failed job registry."""
        q = Queue()
        self.assertEqual(q.count, 0)

        # NOTE: We have to fake this enqueueing for this test case.
        # What we're simulating here is a call to a function that is not
        # importable from the worker process.
        job = Job.create(func=do_nothing, origin=q.name, meta={'key': 'value'})
        job.save()

        invalid_meta = '{{{{{{{{INVALID_JSON'
        self.testconn.hset(job.key, 'meta', invalid_meta)
        job.refresh()
        self.assertIsInstance(job.meta, dict)
        self.assertTrue('unserialized' in job.meta.keys())

    @mock.patch('rq.worker.logger.error')
    def test_deserializing_failure_is_handled(self, mock_logger_error):
        """
        Test that exceptions are properly handled for a job that fails to
        deserialize.
        """
        q = Queue()
        self.assertEqual(q.count, 0)

        # as in test_work_is_unreadable(), we create a fake bad job
        job = Job.create(func=div_by_zero, args=(3,), origin=q.name)
        job.save()

        # setting data to b'' ensures that pickling will completely fail
        job_data = job.data
        invalid_data = job_data.replace(b'div_by_zero', b'')
        assert job_data != invalid_data
        self.testconn.hset(job.key, 'data', zlib.compress(invalid_data))

        # We use the low-level internal function to enqueue any data (bypassing
        # validity checks)
        q.push_job_id(job.id)
        self.assertEqual(q.count, 1)

        # Now we try to run the job...
        w = ForkWorker([q])
        job, queue = w.dequeue_job_and_maintain_ttl(10)
        w.perform_job(job, queue)

        # An exception should be logged here at ERROR level
        self.assertIn("Traceback", mock_logger_error.call_args[0][3])

    def test_heartbeat(self):
        """Heartbeat saves last_heartbeat"""
        q = Queue()
        w = ForkWorker([q])
        w.register_birth()

        self.assertEqual(str(w.pid), as_text(self.testconn.hget(w.key, 'pid')))
        self.assertEqual(w.hostname, as_text(self.testconn.hget(w.key, 'hostname')))
        last_heartbeat = self.testconn.hget(w.key, 'last_heartbeat')
        self.assertIsNotNone(self.testconn.hget(w.key, 'birth'))
        self.assertTrue(last_heartbeat is not None)
        w = ForkWorker.load_by_key(w.key)
        self.assertIsInstance(w.last_heartbeat, datetime)

        # worker.refresh() shouldn't fail if last_heartbeat is None
        # for compatibility reasons
        self.testconn.hdel(w.key, 'last_heartbeat')
        w.refresh()
        # worker.refresh() shouldn't fail if birth is None
        # for compatibility reasons
        self.testconn.hdel(w.key, 'birth')
        w.refresh()

    def test_maintain_heartbeats(self):
        """worker.maintain_heartbeats() shouldn't create new job keys"""
        queue = Queue(connection=self.testconn)
        worker = ForkWorker([queue], connection=self.testconn)
        job = queue.enqueue(say_hello)
        worker.maintain_heartbeats(job)
        self.assertTrue(self.testconn.exists(worker.key))
        self.assertTrue(self.testconn.exists(job.key))

        self.testconn.delete(job.key)

        worker.maintain_heartbeats(job)
        self.assertFalse(self.testconn.exists(job.key))

    @pytest.mark.slow
    def test_heartbeat_survives_lost_connection(self):
        with mock.patch.object(ForkWorker, 'heartbeat') as mocked:
            # None -> Heartbeat is first called before the job loop
            mocked.side_effect = [None, redis.exceptions.ConnectionError()]
            q = Queue()
            w = ForkWorker([q])
            w.work(burst=True)
            # First call is prior to job loop, second raises the error,
            # third is successful, after "recovery"
            assert mocked.call_count == 3

    def test_job_timeout_moved_to_failed_job_registry(self):
        """Jobs that run long are moved to FailedJobRegistry"""
        queue = Queue()
        worker = ForkWorker([queue])
        job = queue.enqueue(long_running_job, 5, job_timeout=1)
        worker.work(burst=True)
        self.assertIn(job, job.failed_job_registry)
        job.refresh()
        self.assertIn('rq.exceptions.JobTimeoutException', job.exc_info)

    @pytest.mark.slow
    def test_heartbeat_busy(self):
        """Periodic heartbeats while horse is busy with long jobs"""
        q = Queue()
        w = ForkWorker([q], job_monitoring_interval=5)

        for timeout, expected_heartbeats in [(2, 0), (7, 1), (12, 2)]:
            job = q.enqueue(long_running_job, args=(timeout,), job_timeout=30, result_ttl=-1)
            with mock.patch.object(w, 'heartbeat', wraps=w.heartbeat) as mocked:
                w.execute_job(job, q)
                self.assertEqual(mocked.call_count, expected_heartbeats)
            job = Job.fetch(job.id)
            self.assertEqual(job.get_status(), JobStatus.FINISHED)

    def test_work_fails(self):
        """Failing jobs are put on the failed queue."""
        q = Queue()
        self.assertEqual(q.count, 0)

        # Action
        job = q.enqueue(div_by_zero)
        self.assertEqual(q.count, 1)

        # keep for later
        enqueued_at_date = str(job.enqueued_at)

        w = ForkWorker([q])
        w.work(burst=True)

        # Postconditions
        self.assertEqual(q.count, 0)
        failed_job_registry = FailedJobRegistry(queue=q)
        self.assertTrue(job in failed_job_registry)
        self.assertEqual(w.get_current_job_id(), None)

        # Check the job
        job = Job.fetch(job.id)
        self.assertEqual(job.origin, q.name)

        # Should be the original enqueued_at date, not the date of enqueueing
        # to the failed queue
        self.assertEqual(str(job.enqueued_at), enqueued_at_date)
        self.assertTrue(job.exc_info)  # should contain exc_info
        if job.supports_redis_streams:
            result = Result.fetch_latest(job)
            self.assertEqual(result.exc_string, job.exc_info)
            self.assertEqual(result.type, ResultType.FAILED)

    def test_statistics(self):
        """Successful and failed job counts are saved properly"""
        queue = Queue(connection=self.connection)
        job = queue.enqueue(div_by_zero)
        worker = ForkWorker([queue])
        worker.register_birth()

        self.assertEqual(worker.failed_job_count, 0)
        self.assertEqual(worker.successful_job_count, 0)
        self.assertEqual(worker.total_working_time, 0)

        registry = StartedJobRegistry(connection=worker.connection)
        job.started_at = utcnow()
        job.ended_at = job.started_at + timedelta(seconds=0.75)
        worker.handle_job_failure(job, queue)
        worker.handle_job_success(job, queue, registry)

        worker.refresh()
        self.assertEqual(worker.failed_job_count, 1)
        self.assertEqual(worker.successful_job_count, 1)
        self.assertEqual(worker.total_working_time, 1.5)  # 1.5 seconds

        worker.handle_job_failure(job, queue)
        worker.handle_job_success(job, queue, registry)

        worker.refresh()
        self.assertEqual(worker.failed_job_count, 2)
        self.assertEqual(worker.successful_job_count, 2)
        self.assertEqual(worker.total_working_time, 3.0)

    def test_handle_retry(self):
        """handle_job_failure() handles retry properly"""
        connection = self.testconn
        queue = Queue(connection=connection)
        retry = Retry(max=2)
        job = queue.enqueue(div_by_zero, retry=retry)
        registry = FailedJobRegistry(queue=queue)

        worker = ForkWorker([queue])

        # If job is configured to retry, it will be put back in the queue
        # and not put in the FailedJobRegistry.
        # This is the original execution
        queue.empty()
        worker.handle_job_failure(job, queue)
        job.refresh()
        self.assertEqual(job.retries_left, 1)
        self.assertEqual([job.id], queue.job_ids)
        self.assertFalse(job in registry)

        # First retry
        queue.empty()
        worker.handle_job_failure(job, queue)
        job.refresh()
        self.assertEqual(job.retries_left, 0)
        self.assertEqual([job.id], queue.job_ids)

        # Second retry
        queue.empty()
        worker.handle_job_failure(job, queue)
        job.refresh()
        self.assertEqual(job.retries_left, 0)
        self.assertEqual([], queue.job_ids)
        # If a job is no longer retries, it's put in FailedJobRegistry
        self.assertTrue(job in registry)

    def test_total_working_time(self):
        """worker.total_working_time is stored properly"""
        queue = Queue()
        job = queue.enqueue(long_running_job, 0.05)
        worker = ForkWorker([queue])
        worker.register_birth()

        worker.perform_job(job, queue)
        worker.refresh()
        # total_working_time should be a little bit more than 0.05 seconds
        self.assertGreaterEqual(worker.total_working_time, 0.05)
        # in multi-user environments delays might be unpredictable,
        # please adjust this magic limit accordingly in case if It takes even longer to run
        self.assertLess(worker.total_working_time, 1)

    def test_max_jobs(self):
        """Worker exits after number of jobs complete."""
        queue = Queue()
        job1 = queue.enqueue(do_nothing)
        job2 = queue.enqueue(do_nothing)
        worker = ForkWorker([queue])
        worker.work(max_jobs=1)

        self.assertEqual(JobStatus.FINISHED, job1.get_status())
        self.assertEqual(JobStatus.QUEUED, job2.get_status())

    def test_disable_default_exception_handler(self):
        """
        Job is not moved to FailedJobRegistry when default custom exception
        handler is disabled.
        """
        queue = Queue(name='default', connection=self.testconn)

        job = queue.enqueue(div_by_zero)
        worker = ForkWorker([queue], disable_default_exception_handler=False)
        worker.work(burst=True)

        registry = FailedJobRegistry(queue=queue)
        self.assertTrue(job in registry)

        # Job is not added to FailedJobRegistry if
        # disable_default_exception_handler is True
        job = queue.enqueue(div_by_zero)
        worker = ForkWorker([queue], disable_default_exception_handler=True)
        worker.work(burst=True)
        self.assertFalse(job in registry)

    def test_custom_exc_handling(self):
        """Custom exception handling."""

        def first_handler(job, *exc_info):
            job.meta = {'first_handler': True}
            job.save_meta()
            return True

        def second_handler(job, *exc_info):
            job.meta.update({'second_handler': True})
            job.save_meta()

        def black_hole(job, *exc_info):
            # Don't fall through to default behaviour (moving to failed queue)
            return False

        q = Queue()
        self.assertEqual(q.count, 0)
        job = q.enqueue(div_by_zero)

        w = ForkWorker([q], exception_handlers=first_handler)
        w.work(burst=True)

        # Check the job
        job.refresh()
        self.assertEqual(job.is_failed, True)
        self.assertTrue(job.meta['first_handler'])

        job = q.enqueue(div_by_zero)
        w = ForkWorker([q], exception_handlers=[first_handler, second_handler])
        w.work(burst=True)

        # Both custom exception handlers are run
        job.refresh()
        self.assertEqual(job.is_failed, True)
        self.assertTrue(job.meta['first_handler'])
        self.assertTrue(job.meta['second_handler'])

        job = q.enqueue(div_by_zero)
        w = ForkWorker([q], exception_handlers=[first_handler, black_hole, second_handler])
        w.work(burst=True)

        # second_handler is not run since it's interrupted by black_hole
        job.refresh()
        self.assertEqual(job.is_failed, True)
        self.assertTrue(job.meta['first_handler'])
        self.assertEqual(job.meta.get('second_handler'), None)

    def test_deleted_jobs_arent_executed(self):
        """Cancelling jobs."""

        SENTINEL_FILE = '/tmp/rq-tests.txt'  # noqa

        try:
            # Remove the sentinel if it is leftover from a previous test run
            os.remove(SENTINEL_FILE)
        except OSError as e:
            if e.errno != 2:
                raise

        q = Queue()
        job = q.enqueue(create_file, SENTINEL_FILE)

        # Here, we cancel the job, so the sentinel file may not be created
        self.testconn.delete(job.key)

        w = ForkWorker([q])
        w.work(burst=True)
        assert q.count == 0

        # Should not have created evidence of execution
        self.assertEqual(os.path.exists(SENTINEL_FILE), False)

    def test_cancel_running_parent_job(self):
        """Cancel a running parent job and verify that
        dependent jobs are not started."""

        def cancel_parent_job(job):
            while job.is_queued:
                time.sleep(1)

            job.cancel()
            return

        q = Queue(
            "low",
        )
        parent_job = q.enqueue(long_running_job, 5)

        job = q.enqueue(say_hello, depends_on=parent_job)
        job2 = q.enqueue(say_hello, depends_on=job)
        status_thread = threading.Thread(target=cancel_parent_job, args=(parent_job,))
        status_thread.start()

        w = ForkWorker([q])
        w.work(
            burst=True,
        )
        status_thread.join()

        self.assertNotEqual(parent_job.result, None)
        self.assertEqual(job.get_status(), JobStatus.DEFERRED)
        self.assertEqual(job.result, None)
        self.assertEqual(job2.get_status(), JobStatus.DEFERRED)
        self.assertEqual(job2.result, None)
        self.assertEqual(q.count, 0)

    def test_cancel_dependent_job(self):
        """Cancel job and verify that when the parent job is finished,
        the dependent job is not started."""

        q = Queue(
            "low",
        )
        parent_job = q.enqueue(long_running_job, 5, job_id="parent_job")
        job = q.enqueue(say_hello, depends_on=parent_job, job_id="job1")
        job2 = q.enqueue(say_hello, depends_on=job, job_id="job2")
        job.cancel()

        w = ForkWorker([q])
        w.work(
            burst=True,
        )
        self.assertTrue(job.is_canceled)
        self.assertNotEqual(parent_job.result, None)
        self.assertEqual(job.get_status(), JobStatus.CANCELED)
        self.assertEqual(job.result, None)
        self.assertEqual(job2.result, None)
        self.assertEqual(job2.get_status(), JobStatus.DEFERRED)
        self.assertEqual(q.count, 0)

    def test_cancel_job_enqueue_dependent(self):
        """Cancel a job in a chain and enqueue the dependent jobs."""

        q = Queue(
            "low",
        )
        parent_job = q.enqueue(long_running_job, 5, job_id="parent_job")
        job = q.enqueue(say_hello, depends_on=parent_job, job_id="job1")
        job2 = q.enqueue(say_hello, depends_on=job, job_id="job2")
        job3 = q.enqueue(say_hello, depends_on=job2, job_id="job3")

        job.cancel(enqueue_dependents=True)

        w = ForkWorker([q])
        w.work(
            burst=True,
        )
        self.assertTrue(job.is_canceled)
        self.assertNotEqual(parent_job.result, None)
        self.assertEqual(job.get_status(), JobStatus.CANCELED)
        self.assertEqual(job.result, None)
        self.assertNotEqual(job2.result, None)
        self.assertEqual(job2.get_status(), JobStatus.FINISHED)
        self.assertEqual(job3.get_status(), JobStatus.FINISHED)

        self.assertEqual(q.count, 0)

    @pytest.mark.slow
    def test_max_idle_time(self):
        q = Queue()
        w = ForkWorker([q])
        q.enqueue(say_hello, args=('Frank',))
        self.assertIsNotNone(w.dequeue_job_and_maintain_ttl(1))

        # idle for 1 second
        self.assertIsNone(w.dequeue_job_and_maintain_ttl(1, max_idle_time=1))

        # idle for 3 seconds
        now = utcnow()
        self.assertIsNone(w.dequeue_job_and_maintain_ttl(1, max_idle_time=3))
        self.assertLess((utcnow() - now).total_seconds(), 5)  # 5 for some buffer

        # idle for 2 seconds because idle_time is less than timeout
        now = utcnow()
        self.assertIsNone(w.dequeue_job_and_maintain_ttl(3, max_idle_time=2))
        self.assertLess((utcnow() - now).total_seconds(), 4)  # 4 for some buffer

        # idle for 3 seconds because idle_time is less than two rounds of timeout
        now = utcnow()
        self.assertIsNone(w.dequeue_job_and_maintain_ttl(2, max_idle_time=3))
        self.assertLess((utcnow() - now).total_seconds(), 5)  # 5 for some buffer

    @pytest.mark.slow  # noqa
    def test_timeouts(self):
        """Worker kills jobs after timeout."""
        sentinel_file = '/tmp/.rq_sentinel'

        q = Queue()
        w = ForkWorker([q])

        # Put it on the queue with a timeout value
        res = q.enqueue(create_file_after_timeout, args=(sentinel_file, 4), job_timeout=1)

        try:
            os.unlink(sentinel_file)
        except OSError as e:
            if e.errno == 2:
                pass

        self.assertEqual(os.path.exists(sentinel_file), False)
        w.work(burst=True)
        self.assertEqual(os.path.exists(sentinel_file), False)

        # TODO: Having to do the manual refresh() here is really ugly!
        res.refresh()
        self.assertIn('JobTimeoutException', as_text(res.exc_info))

    def test_dequeue_job_and_maintain_ttl_non_blocking(self):
        """Not passing a timeout should return immediately with None as a result"""
        q = Queue()
        w = ForkWorker([q])

        self.assertIsNone(w.dequeue_job_and_maintain_ttl(None))

    def test_worker_ttl_param_resolves_timeout(self):
        """
        Ensures the worker_ttl param is being considered in the dequeue_timeout and
        connection_timeout params, takes into account 15 seconds gap (hard coded)
        """
        q = Queue()
        w = ForkWorker([q])
        self.assertEqual(w.dequeue_timeout, 405)
        self.assertEqual(w.connection_timeout, 415)
        w = ForkWorker([q], default_worker_ttl=500)
        self.assertEqual(w.dequeue_timeout, 485)
        self.assertEqual(w.connection_timeout, 495)

    def test_worker_sets_result_ttl(self):
        """Ensure that Worker properly sets result_ttl for individual jobs."""
        q = Queue()
        job = q.enqueue(say_hello, args=('Frank',), result_ttl=10)
        w = ForkWorker([q])
        self.assertIn(job.get_id().encode(), self.testconn.lrange(q.key, 0, -1))
        w.work(burst=True)
        self.assertNotEqual(self.testconn.ttl(job.key), 0)
        self.assertNotIn(job.get_id().encode(), self.testconn.lrange(q.key, 0, -1))

        # Job with -1 result_ttl don't expire
        job = q.enqueue(say_hello, args=('Frank',), result_ttl=-1)
        w = ForkWorker([q])
        self.assertIn(job.get_id().encode(), self.testconn.lrange(q.key, 0, -1))
        w.work(burst=True)
        self.assertEqual(self.testconn.ttl(job.key), -1)
        self.assertNotIn(job.get_id().encode(), self.testconn.lrange(q.key, 0, -1))

        # Job with result_ttl = 0 gets deleted immediately
        job = q.enqueue(say_hello, args=('Frank',), result_ttl=0)
        w = ForkWorker([q])
        self.assertIn(job.get_id().encode(), self.testconn.lrange(q.key, 0, -1))
        w.work(burst=True)
        self.assertEqual(self.testconn.get(job.key), None)
        self.assertNotIn(job.get_id().encode(), self.testconn.lrange(q.key, 0, -1))

    def test_worker_sets_job_status(self):
        """Ensure that worker correctly sets job status."""
        q = Queue()
        w = ForkWorker([q])

        job = q.enqueue(say_hello)
        self.assertEqual(job.get_status(), JobStatus.QUEUED)
        self.assertEqual(job.is_queued, True)
        self.assertEqual(job.is_finished, False)
        self.assertEqual(job.is_failed, False)

        w.work(burst=True)
        job = Job.fetch(job.id)
        self.assertEqual(job.get_status(), JobStatus.FINISHED)
        self.assertEqual(job.is_queued, False)
        self.assertEqual(job.is_finished, True)
        self.assertEqual(job.is_failed, False)

        # Failed jobs should set status to "failed"
        job = q.enqueue(div_by_zero, args=(1,))
        w.work(burst=True)
        job = Job.fetch(job.id)
        self.assertEqual(job.get_status(), JobStatus.FAILED)
        self.assertEqual(job.is_queued, False)
        self.assertEqual(job.is_finished, False)
        self.assertEqual(job.is_failed, True)

    def test_get_current_job(self):
        """Ensure worker.get_current_job() works properly"""
        q = Queue()
        worker = ForkWorker([q])
        job = q.enqueue_call(say_hello)

        self.assertEqual(self.testconn.hget(worker.key, 'current_job'), None)
        worker.set_current_job_id(job.id)
        self.assertEqual(worker.get_current_job_id(), as_text(self.testconn.hget(worker.key, 'current_job')))
        self.assertEqual(worker.get_current_job(), job)

    def test_custom_job_class(self):
        """Ensure Worker accepts custom job class."""
        q = Queue()
        worker = ForkWorker([q], job_class=CustomJob)
        self.assertEqual(worker.job_class, CustomJob)

    def test_custom_queue_class(self):
        """Ensure Worker accepts custom queue class."""
        q = CustomQueue()
        worker = ForkWorker([q], queue_class=CustomQueue)
        self.assertEqual(worker.queue_class, CustomQueue)

    def test_custom_queue_class_is_not_global(self):
        """Ensure Worker custom queue class is not global."""
        q = CustomQueue()
        worker_custom = ForkWorker([q], queue_class=CustomQueue)
        q_generic = Queue()
        worker_generic = ForkWorker([q_generic])
        self.assertEqual(worker_custom.queue_class, CustomQueue)
        self.assertEqual(worker_generic.queue_class, Queue)
        self.assertEqual(ForkWorker.queue_class, Queue)

    def test_custom_job_class_is_not_global(self):
        """Ensure Worker custom job class is not global."""
        q = Queue()
        worker_custom = ForkWorker([q], job_class=CustomJob)
        q_generic = Queue()
        worker_generic = ForkWorker([q_generic])
        self.assertEqual(worker_custom.job_class, CustomJob)
        self.assertEqual(worker_generic.job_class, Job)
        self.assertEqual(ForkWorker.job_class, Job)

    def test_work_via_simpleworker(self):
        """Worker processes work, with forking disabled,
        then returns."""
        fooq, barq = Queue('foo'), Queue('bar')
        w = Worker([fooq, barq])
        self.assertEqual(w.work(burst=True), False, 'Did not expect any work on the queue.')

        job = fooq.enqueue(say_pid)
        self.assertEqual(w.work(burst=True), True, 'Expected at least some work done.')
        self.assertEqual(job.result, os.getpid(), 'PID mismatch, fork() is not supposed to happen here')

    def test_simpleworker_heartbeat_ttl(self):
        """SimpleWorker's key must last longer than job.timeout when working"""
        queue = Queue('foo')

        worker = Worker([queue])
        job_timeout = 300
        job = queue.enqueue(save_key_ttl, worker.key, job_timeout=job_timeout)
        worker.work(burst=True)
        job.refresh()
        self.assertGreater(job.meta['ttl'], job_timeout)

    def test_prepare_job_execution(self):
        """Prepare job execution does the necessary bookkeeping."""
        queue = Queue(connection=self.testconn)
        job = queue.enqueue(say_hello)
        worker = ForkWorker([queue])
        worker.prepare_job_execution(job)

        # Updates working queue
        registry = StartedJobRegistry(connection=self.testconn)
        self.assertEqual(registry.get_job_ids(), [job.id])

        # Updates worker's current job
        self.assertEqual(worker.get_current_job_id(), job.id)

        # job status is also updated
        self.assertEqual(job._status, JobStatus.STARTED)
        self.assertEqual(job.worker_name, worker.name)

    @skipIf(get_version(Redis()) < (6, 2, 0), 'Skip if Redis server < 6.2.0')
    def test_prepare_job_execution_removes_key_from_intermediate_queue(self):
        """Prepare job execution removes job from intermediate queue."""
        queue = Queue(connection=self.testconn)
        job = queue.enqueue(say_hello)

        Queue.dequeue_any([queue], timeout=None, connection=self.testconn)
        self.assertIsNotNone(self.testconn.lpos(queue.intermediate_queue_key, job.id))
        worker = ForkWorker([queue])
        worker.prepare_job_execution(job, remove_from_intermediate_queue=True)
        self.assertIsNone(self.testconn.lpos(queue.intermediate_queue_key, job.id))
        self.assertEqual(queue.count, 0)

    @skipIf(get_version(Redis()) < (6, 2, 0), 'Skip if Redis server < 6.2.0')
    def test_work_removes_key_from_intermediate_queue(self):
        """Worker removes job from intermediate queue."""
        queue = Queue(connection=self.testconn)
        job = queue.enqueue(say_hello)
        worker = ForkWorker([queue])
        worker.work(burst=True)
        self.assertIsNone(self.testconn.lpos(queue.intermediate_queue_key, job.id))

    def test_work_unicode_friendly(self):
        """Worker processes work with unicode description, then quits."""
        q = Queue('foo')
        w = ForkWorker([q])
        job = q.enqueue('tests.fixtures.say_hello', name='Adam', description='你好 世界!')
        self.assertEqual(w.work(burst=True), True, 'Expected at least some work done.')
        self.assertEqual(job.result, 'Hi there, Adam!')
        self.assertEqual(job.description, '你好 世界!')

    def test_work_log_unicode_friendly(self):
        """Worker process work with unicode or str other than pure ascii content,
        logging work properly"""
        q = Queue("foo")
        w = ForkWorker([q])

        job = q.enqueue('tests.fixtures.say_hello', name='阿达姆', description='你好 世界!')
        w.work(burst=True)
        self.assertEqual(job.get_status(), JobStatus.FINISHED)

        job = q.enqueue('tests.fixtures.say_hello_unicode', name='阿达姆', description='你好 世界!')
        w.work(burst=True)
        self.assertEqual(job.get_status(), JobStatus.FINISHED)

    def test_suspend_worker_execution(self):
        """Test Pause Worker Execution"""

        SENTINEL_FILE = '/tmp/rq-tests.txt'  # noqa

        try:
            # Remove the sentinel if it is leftover from a previous test run
            os.remove(SENTINEL_FILE)
        except OSError as e:
            if e.errno != 2:
                raise

        q = Queue()
        q.enqueue(create_file, SENTINEL_FILE)

        w = ForkWorker([q])

        suspend(self.testconn)

        w.work(burst=True)
        assert q.count == 1

        # Should not have created evidence of execution
        self.assertEqual(os.path.exists(SENTINEL_FILE), False)

        resume(self.testconn)
        w.work(burst=True)
        assert q.count == 0
        self.assertEqual(os.path.exists(SENTINEL_FILE), True)

    @pytest.mark.slow
    def test_suspend_with_duration(self):
        q = Queue()
        for _ in range(5):
            q.enqueue(do_nothing)

        w = ForkWorker([q])

        # This suspends workers for working for 2 second
        suspend(self.testconn, 2)

        # So when this burst of work happens the queue should remain at 5
        w.work(burst=True)
        assert q.count == 5

        sleep(3)

        # The suspension should be expired now, and a burst of work should now clear the queue
        w.work(burst=True)
        assert q.count == 0

    def test_worker_hash_(self):
        """Workers are hashed by their .name attribute"""
        q = Queue('foo')
        w1 = ForkWorker([q], name="worker1")
        w2 = ForkWorker([q], name="worker2")
        w3 = ForkWorker([q], name="worker1")
        worker_set = set([w1, w2, w3])
        self.assertEqual(len(worker_set), 2)

    def test_worker_sets_birth(self):
        """Ensure worker correctly sets worker birth date."""
        q = Queue()
        w = ForkWorker([q])

        w.register_birth()

        birth_date = w.birth_date
        self.assertIsNotNone(birth_date)
        self.assertEqual(type(birth_date).__name__, 'datetime')

    def test_worker_sets_death(self):
        """Ensure worker correctly sets worker death date."""
        q = Queue()
        w = ForkWorker([q])

        w.register_death()

        death_date = w.death_date
        self.assertIsNotNone(death_date)
        self.assertIsInstance(death_date, datetime)

    def test_clean_queue_registries(self):
        """worker.clean_registries sets last_cleaned_at and cleans registries."""
        foo_queue = Queue('foo', connection=self.testconn)
        foo_registry = StartedJobRegistry('foo', connection=self.testconn)
        self.testconn.zadd(foo_registry.key, {'foo': 1})
        self.assertEqual(self.testconn.zcard(foo_registry.key), 1)

        bar_queue = Queue('bar', connection=self.testconn)
        bar_registry = StartedJobRegistry('bar', connection=self.testconn)
        self.testconn.zadd(bar_registry.key, {'bar': 1})
        self.assertEqual(self.testconn.zcard(bar_registry.key), 1)

        worker = ForkWorker([foo_queue, bar_queue])
        self.assertEqual(worker.last_cleaned_at, None)
        worker.clean_registries()
        self.assertNotEqual(worker.last_cleaned_at, None)
        self.assertEqual(self.testconn.zcard(foo_registry.key), 0)
        self.assertEqual(self.testconn.zcard(bar_registry.key), 0)

        # worker.clean_registries() only runs once every 15 minutes
        # If we add another key, calling clean_registries() should do nothing
        self.testconn.zadd(bar_registry.key, {'bar': 1})
        worker.clean_registries()
        self.assertEqual(self.testconn.zcard(bar_registry.key), 1)

    def test_should_run_maintenance_tasks(self):
        """Workers should run maintenance tasks on startup and every hour."""
        queue = Queue(connection=self.testconn)
        worker = ForkWorker(queue)
        self.assertTrue(worker.should_run_maintenance_tasks)

        worker.last_cleaned_at = utcnow()
        self.assertFalse(worker.should_run_maintenance_tasks)
        worker.last_cleaned_at = utcnow() - timedelta(seconds=DEFAULT_MAINTENANCE_TASK_INTERVAL + 100)
        self.assertTrue(worker.should_run_maintenance_tasks)

        # custom maintenance_interval
        worker = ForkWorker(queue, maintenance_interval=10)
        self.assertTrue(worker.should_run_maintenance_tasks)
        worker.last_cleaned_at = utcnow()
        self.assertFalse(worker.should_run_maintenance_tasks)
        worker.last_cleaned_at = utcnow() - timedelta(seconds=11)
        self.assertTrue(worker.should_run_maintenance_tasks)

    def test_worker_calls_clean_registries(self):
        """Worker calls clean_registries when run."""
        queue = Queue(connection=self.testconn)
        registry = StartedJobRegistry(connection=self.testconn)
        self.testconn.zadd(registry.key, {'foo': 1})

        worker = ForkWorker(queue, connection=self.testconn)
        worker.work(burst=True)
        self.assertEqual(self.testconn.zcard(registry.key), 0)

    def test_self_modification_persistence(self):
        """Make sure that any meta modification done by
        the job itself persists completely through the
        queue/worker/job stack."""
        q = Queue()
        # Also make sure that previously existing metadata
        # persists properly
        job = q.enqueue(modify_self, meta={'foo': 'bar', 'baz': 42}, args=[{'baz': 10, 'newinfo': 'waka'}])

        w = ForkWorker([q])
        w.work(burst=True)

        job_check = Job.fetch(job.id)
        self.assertEqual(job_check.meta['foo'], 'bar')
        self.assertEqual(job_check.meta['baz'], 10)
        self.assertEqual(job_check.meta['newinfo'], 'waka')

    def test_self_modification_persistence_with_error(self):
        """Make sure that any meta modification done by
        the job itself persists completely through the
        queue/worker/job stack -- even if the job errored"""
        q = Queue()
        # Also make sure that previously existing metadata
        # persists properly
        job = q.enqueue(modify_self_and_error, meta={'foo': 'bar', 'baz': 42}, args=[{'baz': 10, 'newinfo': 'waka'}])

        w = ForkWorker([q])
        w.work(burst=True)

        # Postconditions
        self.assertEqual(q.count, 0)
        failed_job_registry = FailedJobRegistry(queue=q)
        self.assertTrue(job in failed_job_registry)
        self.assertEqual(w.get_current_job_id(), None)

        job_check = Job.fetch(job.id)
        self.assertEqual(job_check.meta['foo'], 'bar')
        self.assertEqual(job_check.meta['baz'], 10)
        self.assertEqual(job_check.meta['newinfo'], 'waka')

    @mock.patch('rq.worker.logger.info')
    def test_log_result_lifespan_true(self, mock_logger_info):
        """Check that log_result_lifespan True causes job lifespan to be logged."""
        q = Queue()

        w = ForkWorker([q])
        job = q.enqueue(say_hello, args=('Frank',), result_ttl=10)
        w.perform_job(job, q)
        mock_logger_info.assert_called_with('Result is kept for %s seconds', 10)
        self.assertIn('Result is kept for %s seconds', [c[0][0] for c in mock_logger_info.call_args_list])

    @mock.patch('rq.worker.logger.info')
    def test_log_result_lifespan_false(self, mock_logger_info):
        """Check that log_result_lifespan False causes job lifespan to not be logged."""
        q = Queue()

        class TestWorker(ForkWorker):
            log_result_lifespan = False

        w = TestWorker([q])
        job = q.enqueue(say_hello, args=('Frank',), result_ttl=10)
        w.perform_job(job, q)
        self.assertNotIn('Result is kept for 10 seconds', [c[0][0] for c in mock_logger_info.call_args_list])

    @mock.patch('rq.worker.logger.info')
    def test_log_job_description_true(self, mock_logger_info):
        """Check that log_job_description True causes job lifespan to be logged."""
        q = Queue()
        w = ForkWorker([q])
        q.enqueue(say_hello, args=('Frank',), result_ttl=10)
        w.dequeue_job_and_maintain_ttl(10)
        self.assertIn("Frank", mock_logger_info.call_args[0][2])

    @mock.patch('rq.worker.logger.info')
    def test_log_job_description_false(self, mock_logger_info):
        """Check that log_job_description False causes job lifespan to not be logged."""
        q = Queue()
        w = ForkWorker([q], log_job_description=False)
        q.enqueue(say_hello, args=('Frank',), result_ttl=10)
        w.dequeue_job_and_maintain_ttl(10)
        self.assertNotIn("Frank", mock_logger_info.call_args[0][2])

    def test_worker_configures_socket_timeout(self):
        """Ensures that the worker correctly updates Redis client connection to have a socket_timeout"""
        q = Queue()
        _ = ForkWorker([q])
        connection_kwargs = q.connection.connection_pool.connection_kwargs
        self.assertEqual(connection_kwargs["socket_timeout"], 415)

    def test_worker_version(self):
        q = Queue()
        w = ForkWorker([q])
        w.version = '0.0.0'
        w.register_birth()
        self.assertEqual(w.version, '0.0.0')
        w.refresh()
        self.assertEqual(w.version, '0.0.0')
        # making sure that version is preserved when worker is retrieved by key
        worker = ForkWorker.load_by_key(w.key)
        self.assertEqual(worker.version, '0.0.0')

    def test_python_version(self):
        python_version = sys.version
        q = Queue()
        w = ForkWorker([q])
        w.register_birth()
        self.assertEqual(w.python_version, python_version)
        # now patching version
        python_version = 'X.Y.Z.final'  # dummy version
        self.assertNotEqual(python_version, sys.version)  # otherwise tests are pointless
        w2 = ForkWorker([q])
        w2.python_version = python_version
        w2.register_birth()
        self.assertEqual(w2.python_version, python_version)
        # making sure that version is preserved when worker is retrieved by key
        worker = ForkWorker.load_by_key(w2.key)
        self.assertEqual(worker.python_version, python_version)

    def test_dequeue_random_strategy(self):
        qs = [Queue('q%d' % i) for i in range(5)]

        for i in range(5):
            for j in range(3):
                qs[i].enqueue(say_pid, job_id='q%d_%d' % (i, j))

        w = ForkWorker(qs)
        w.work(burst=True, dequeue_strategy="random")

        start_times = []
        for i in range(5):
            for j in range(3):
                job = Job.fetch('q%d_%d' % (i, j))
                start_times.append(('q%d_%d' % (i, j), job.started_at))
        sorted_by_time = sorted(start_times, key=lambda tup: tup[1])
        sorted_ids = [tup[0] for tup in sorted_by_time]
        expected_rr = ['q%d_%d' % (i, j) for j in range(3) for i in range(5)]
        expected_ser = ['q%d_%d' % (i, j) for i in range(5) for j in range(3)]

        self.assertNotEqual(sorted_ids, expected_rr)
        self.assertNotEqual(sorted_ids, expected_ser)
        expected_rr.reverse()
        expected_ser.reverse()
        self.assertNotEqual(sorted_ids, expected_rr)
        self.assertNotEqual(sorted_ids, expected_ser)
        sorted_ids.sort()
        expected_ser.sort()
        self.assertEqual(sorted_ids, expected_ser)

    def test_dequeue_round_robin(self):
        qs = [Queue('q%d' % i) for i in range(5)]

        for i in range(5):
            for j in range(3):
                qs[i].enqueue(say_pid, job_id='q%d_%d' % (i, j))

        w = ForkWorker(qs)
        w.work(burst=True, dequeue_strategy="round_robin")

        start_times = []
        for i in range(5):
            for j in range(3):
                job = Job.fetch('q%d_%d' % (i, j))
                start_times.append(('q%d_%d' % (i, j), job.started_at))
        sorted_by_time = sorted(start_times, key=lambda tup: tup[1])
        sorted_ids = [tup[0] for tup in sorted_by_time]
        expected = [
            'q0_0',
            'q1_0',
            'q2_0',
            'q3_0',
            'q4_0',
            'q0_1',
            'q1_1',
            'q2_1',
            'q3_1',
            'q4_1',
            'q0_2',
            'q1_2',
            'q2_2',
            'q3_2',
            'q4_2',
        ]

        self.assertEqual(expected, sorted_ids)


class TestForkWorker(RQTestCase):

    def test_request_force_stop_ignores_consecutive_signals(self):
        """Ignore signals sent within 1 second of the last signal"""
        queue = Queue(connection=self.testconn)
        worker = ForkWorker([queue])
        worker._horse_pid = 1
        worker._shutdown_requested_date = utcnow()
        with mock.patch.object(worker, 'kill_horse') as mocked:
            worker.request_force_stop(1, frame=None)
            self.assertEqual(mocked.call_count, 0)
        # If signal is sent a few seconds after, kill_horse() is called
        worker._shutdown_requested_date = utcnow() - timedelta(seconds=2)
        with mock.patch.object(worker, 'kill_horse') as mocked:
            self.assertRaises(SystemExit, worker.request_force_stop, 1, frame=None)

    def test_horse_fails(self):
        """Tests that job status is set to FAILED even if horse unexpectedly fails"""
        q = Queue()
        self.assertEqual(q.count, 0)

        # Action
        job = q.enqueue(say_hello)
        self.assertEqual(q.count, 1)

        # keep for later
        enqueued_at_date = str(job.enqueued_at)

        w = ForkWorker([q])
        with mock.patch.object(w, 'perform_job', new_callable=raise_exc_mock):
            w.work(burst=True)  # should silently pass

        # Postconditions
        self.assertEqual(q.count, 0)
        failed_job_registry = FailedJobRegistry(queue=q)
        self.assertTrue(job in failed_job_registry)
        self.assertEqual(w.get_current_job_id(), None)

        # Check the job
        job = Job.fetch(job.id)
        self.assertEqual(job.origin, q.name)

        # Should be the original enqueued_at date, not the date of enqueueing
        # to the failed queue
        self.assertEqual(str(job.enqueued_at), enqueued_at_date)
        self.assertTrue(job.exc_info)  # should contain exc_info

    def test_job_dependency_race_condition(self):
        """Dependencies added while the job gets finished shouldn't get lost."""

        # This patches the enqueue_dependents to enqueue a new dependency AFTER
        # the original code was executed.
        orig_enqueue_dependents = Queue.enqueue_dependents

        def new_enqueue_dependents(self, job, *args, **kwargs):
            orig_enqueue_dependents(self, job, *args, **kwargs)
            if hasattr(Queue, '_add_enqueue') and Queue._add_enqueue is not None and Queue._add_enqueue.id == job.id:
                Queue._add_enqueue = None
                Queue().enqueue_call(say_hello, depends_on=job)

        Queue.enqueue_dependents = new_enqueue_dependents

        q = Queue()
        w = ForkWorker([q])
        with mock.patch.object(ForkWorker, 'execute_job', wraps=w.execute_job) as mocked:
            parent_job = q.enqueue(say_hello, result_ttl=0)
            Queue._add_enqueue = parent_job
            job = q.enqueue_call(say_hello, depends_on=parent_job)
            w.work(burst=True)
            job = Job.fetch(job.id)
            self.assertEqual(job.get_status(), JobStatus.FINISHED)

            # The created spy checks two issues:
            # * before the fix of #739, 2 of the 3 jobs where executed due
            #   to the race condition
            # * during the development another issue was fixed:
            #   due to a missing pipeline usage in Queue.enqueue_job, the job
            #   which was enqueued before the "rollback" was executed twice.
            #   So before that fix the call count was 4 instead of 3
            self.assertEqual(mocked.call_count, 3)


class TimeoutTestCase:
    def setUp(self):
        # we want tests to fail if signal are ignored and the work remain
        # running, so set a signal to kill them after X seconds
        self.killtimeout = 15
        signal.signal(signal.SIGALRM, self._timeout)
        signal.alarm(self.killtimeout)

    def _timeout(self, signal, frame):
        raise AssertionError(
            "test still running after %i seconds, likely the worker wasn't shutdown correctly" % self.killtimeout
        )


class WorkerShutdownTestCase(TimeoutTestCase, RQTestCase):
    @pytest.mark.slow
    def test_idle_worker_warm_shutdown(self):
        """worker with no ongoing job receiving single SIGTERM signal and shutting down"""
        w = ForkWorker('foo')
        self.assertFalse(w._stop_requested)
        p = Process(target=kill_worker, args=(os.getpid(), False))
        p.start()

        w.work()

        p.join(1)
        self.assertFalse(w._stop_requested)

    @pytest.mark.slow
    def test_working_worker_warm_shutdown(self):
        """worker with an ongoing job receiving single SIGTERM signal, allowing job to finish then shutting down"""
        fooq = Queue('foo')
        w = ForkWorker(fooq)

        sentinel_file = '/tmp/.rq_sentinel_warm'
        fooq.enqueue(create_file_after_timeout, sentinel_file, 2)
        self.assertFalse(w._stop_requested)
        p = Process(target=kill_worker, args=(os.getpid(), False))
        p.start()

        w.work()

        p.join(2)
        self.assertFalse(p.is_alive())
        self.assertTrue(w._stop_requested)
        self.assertTrue(os.path.exists(sentinel_file))

        self.assertIsNotNone(w.shutdown_requested_date)
        self.assertEqual(type(w.shutdown_requested_date).__name__, 'datetime')

    @pytest.mark.slow
    def test_working_worker_cold_shutdown(self):
        """Busy worker shuts down immediately on double SIGTERM signal"""
        fooq = Queue('foo')
        w = ForkWorker(fooq)

        sentinel_file = '/tmp/.rq_sentinel_cold'
        self.assertFalse(
            os.path.exists(sentinel_file), '{sentinel_file} file should not exist yet, delete that file and try again.'
        )
        fooq.enqueue(create_file_after_timeout, sentinel_file, 5)
        self.assertFalse(w._stop_requested)
        p = Process(target=kill_worker, args=(os.getpid(), True))
        p.start()

        self.assertRaises(SystemExit, w.work)

        p.join(1)
        self.assertTrue(w._stop_requested)
        self.assertFalse(os.path.exists(sentinel_file))

        shutdown_requested_date = w.shutdown_requested_date
        self.assertIsNotNone(shutdown_requested_date)
        self.assertEqual(type(shutdown_requested_date).__name__, 'datetime')

    @pytest.mark.slow
    def test_work_horse_death_sets_job_failed(self):
        """worker with an ongoing job whose work horse dies unexpectadly (before
        completing the job) should set the job's status to FAILED
        """
        fooq = Queue('foo')
        self.assertEqual(fooq.count, 0)
        w = ForkWorker(fooq)
        sentinel_file = '/tmp/.rq_sentinel_work_horse_death'
        if os.path.exists(sentinel_file):
            os.remove(sentinel_file)
        fooq.enqueue(create_file_after_timeout, sentinel_file, 100)
        job, queue = w.dequeue_job_and_maintain_ttl(5)
        w.fork_work_horse(job, queue)
        p = Process(target=wait_and_kill_work_horse, args=(w._horse_pid, 0.5))
        p.start()
        w.monitor_work_horse(job, queue)
        job_status = job.get_status()
        p.join(1)
        self.assertEqual(job_status, JobStatus.FAILED)
        failed_job_registry = FailedJobRegistry(queue=fooq)
        self.assertTrue(job in failed_job_registry)
        self.assertEqual(fooq.count, 0)

    @pytest.mark.slow
    @unittest.skip('Very slow')
    def test_work_horse_force_death(self):
        """Simulate a frozen worker that doesn't observe the timeout properly.
        Fake it by artificially setting the timeout of the parent process to
        something much smaller after the process is already forked.
        """
        fooq = Queue('foo')
        self.assertEqual(fooq.count, 0)
        w = ForkWorker([fooq], job_monitoring_interval=1)

        sentinel_file = '/tmp/.rq_sentinel_work_horse_death'
        if os.path.exists(sentinel_file):
            os.remove(sentinel_file)

        job = fooq.enqueue(launch_process_within_worker_and_store_pid, sentinel_file, 100)

        _, queue = w.dequeue_job_and_maintain_ttl(5)
        w.prepare_job_execution(job)
        w.fork_work_horse(job, queue)
        job.timeout = 5
        time.sleep(1)
        with open(sentinel_file) as f:
            subprocess_pid = int(f.read().strip())
        self.assertTrue(psutil.pid_exists(subprocess_pid))

        with mock.patch.object(w, 'handle_work_horse_killed', wraps=w.handle_work_horse_killed) as mocked:
            w.monitor_work_horse(job, queue)
            self.assertEqual(mocked.call_count, 1)
        fudge_factor = 1
        total_time = w.job_monitoring_interval + 65 + fudge_factor

        now = utcnow()
        self.assertTrue((utcnow() - now).total_seconds() < total_time)
        self.assertEqual(job.get_status(), JobStatus.FAILED)
        failed_job_registry = FailedJobRegistry(queue=fooq)
        self.assertTrue(job in failed_job_registry)
        self.assertEqual(fooq.count, 0)
        self.assertFalse(psutil.pid_exists(subprocess_pid))


@pytest.mark.skipif(sys.platform == 'darwin', reason='Fails on OS X')
class TestWorkerSubprocess(RQTestCase):
    def setUp(self):
        super().setUp()
        db_num = self.testconn.connection_pool.connection_kwargs['db']
        self.redis_url = 'redis://127.0.0.1:6379/%d' % db_num

    def test_run_empty_queue(self):
        """Run the worker in its own process with an empty queue"""
        subprocess.check_call(['rqworker', '-u', self.redis_url, '-b'])

    def test_run_access_self(self):
        """Schedule a job, then run the worker as subprocess"""
        q = Queue()
        job = q.enqueue(access_self)
        subprocess.check_call(['rqworker', '-u', self.redis_url, '-b'])
        registry = FinishedJobRegistry(queue=q)
        self.assertTrue(job in registry)
        assert q.count == 0

    @skipIf('pypy' in sys.version.lower(), 'often times out with pypy')
    def test_run_scheduled_access_self(self):
        """Schedule a job that schedules a job, then run the worker as subprocess"""
        q = Queue()
        job = q.enqueue(schedule_access_self)
        subprocess.check_call(['rqworker', '-u', self.redis_url, '-b'])
        registry = FinishedJobRegistry(queue=q)
        self.assertTrue(job in registry)
        assert q.count == 0


class TestExceptionHandlerMessageEncoding(RQTestCase):
    def setUp(self):
        super().setUp()
        self.worker = ForkWorker("foo")
        self.worker._exc_handlers = []
        # Mimic how exception info is actually passed forwards
        try:
            raise Exception(u"💪")
        except Exception:
            self.exc_info = sys.exc_info()

    def test_handle_exception_handles_non_ascii_in_exception_message(self):
        """worker.handle_exception doesn't crash on non-ascii in exception message."""
        self.worker.handle_exception(Mock(), *self.exc_info)


def wait_and_kill_work_horse(pid, time_to_wait=0.0):
    time.sleep(time_to_wait)
    os.kill(pid, signal.SIGKILL)


def schedule_access_self():
    q = Queue('default', connection=get_current_connection())
    q.enqueue(access_self)


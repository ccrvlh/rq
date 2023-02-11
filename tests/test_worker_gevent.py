import json
import os
import sys
import zlib

from datetime import datetime, timedelta
from time import sleep

import redis.exceptions
from unittest import mock

from tests import RQTestCase, slow
from tests.fixtures import (
    create_file, create_file_after_timeout, div_by_zero, do_nothing,
    long_running_job, modify_self, modify_self_and_error,
    say_hello, raise_exc_mock
)

from rq import Queue
from rq.utils import as_text
from rq.job import Job, JobStatus, Retry
from rq.registry import StartedJobRegistry, FailedJobRegistry
from rq.results import Result
from rq.suspension import resume, suspend
from rq.utils import utcnow
from rq.version import VERSION
from rq.worker import GeventWorker, WorkerStatus
from rq.serializers import JSONSerializer


class CustomJob(Job):
    pass


class CustomQueue(Queue):
    pass


class TestGeventWorker(RQTestCase):

    def test_create_worker(self):
        """GeventWorker creation using various inputs."""

        # With single string argument
        w = GeventWorker('foo')
        self.assertEqual(w.queues[0].name, 'foo')

        # With list of strings
        w = GeventWorker(['foo', 'bar'])
        self.assertEqual(w.queues[0].name, 'foo')
        self.assertEqual(w.queues[1].name, 'bar')

        self.assertEqual(w.queue_keys(), [w.queues[0].key, w.queues[1].key])
        self.assertEqual(w.queue_names(), ['foo', 'bar'])

        # With iterable of strings
        w = GeventWorker(iter(['foo', 'bar']))
        self.assertEqual(w.queues[0].name, 'foo')
        self.assertEqual(w.queues[1].name, 'bar')

        # With single Queue
        w = GeventWorker(Queue('foo'))
        self.assertEqual(w.queues[0].name, 'foo')

        # With iterable of Queues
        w = GeventWorker(iter([Queue('foo'), Queue('bar')]))
        self.assertEqual(w.queues[0].name, 'foo')
        self.assertEqual(w.queues[1].name, 'bar')

        # With list of Queues
        w = GeventWorker([Queue('foo'), Queue('bar')])
        self.assertEqual(w.queues[0].name, 'foo')
        self.assertEqual(w.queues[1].name, 'bar')

        # With string and serializer
        w = GeventWorker('foo', serializer=json)
        self.assertEqual(w.queues[0].name, 'foo')

        # With queue having serializer
        w = GeventWorker(Queue('foo'), serializer=json)
        self.assertEqual(w.queues[0].name, 'foo')

    def test_work_and_quit(self):
        """GeventWorker processes work, then quits."""
        fooq, barq = Queue('foo'), Queue('bar')
        w = GeventWorker([fooq, barq])
        self.assertEqual(
            w.work(burst=True), False,
            'Did not expect any work on the queue.'
        )

        fooq.enqueue(say_hello, name='Frank')
        self.assertEqual(
            w.work(burst=True), True,
            'Expected at least some work done.'
        )

    def test_work_and_quit_custom_serializer(self):
        """GeventWorker processes work, then quits."""
        fooq, barq = Queue('foo', serializer=JSONSerializer), Queue('bar', serializer=JSONSerializer)
        w = GeventWorker([fooq, barq], serializer=JSONSerializer)
        self.assertEqual(
            w.work(burst=True), False,
            'Did not expect any work on the queue.'
        )

        fooq.enqueue(say_hello, name='Frank')
        self.assertEqual(
            w.work(burst=True), True,
            'Expected at least some work done.'
        )

    def test_worker_all(self):
        """GeventWorker.all() works properly"""
        foo_queue = Queue('foo')
        bar_queue = Queue('bar')

        w1 = GeventWorker([foo_queue, bar_queue], name='w1')
        w1.register_birth()
        w2 = GeventWorker([foo_queue], name='w2')
        w2.register_birth()

        self.assertEqual(
            set(GeventWorker.all(connection=foo_queue.connection)),
            set([w1, w2])
        )
        self.assertEqual(set(GeventWorker.all(queue=foo_queue)), set([w1, w2]))
        self.assertEqual(set(GeventWorker.all(queue=bar_queue)), set([w1]))

        w1.register_death()
        w2.register_death()

    def test_find_by_key(self):
        """GeventWorker.find_by_key restores queues, state and job_id."""
        queues = [Queue('foo'), Queue('bar')]
        w = GeventWorker(queues)
        w.register_death()
        w.register_birth()
        w.set_state(WorkerStatus.STARTED)
        worker = GeventWorker.find_by_key(w.key)
        self.assertEqual(worker.queues, queues)
        self.assertEqual(worker.get_state(), WorkerStatus.STARTED)
        self.assertEqual(worker._job_id, None)
        self.assertTrue(worker.key in GeventWorker.all_keys(worker.connection))
        self.assertEqual(worker.version, VERSION)

        # If worker is gone, its keys should also be removed
        worker.connection.delete(worker.key)
        GeventWorker.find_by_key(worker.key)
        self.assertFalse(worker.key in GeventWorker.all_keys(worker.connection))

        self.assertRaises(ValueError, GeventWorker.find_by_key, 'foo')

    def test_worker_ttl(self):
        """GeventWorker ttl."""
        w = GeventWorker([])
        w.register_birth()
        [worker_key] = self.testconn.smembers(GeventWorker.redis_workers_keys)
        self.assertIsNotNone(self.testconn.ttl(worker_key))
        w.register_death()

    def test_work_via_string_argument(self):
        """GeventWorker processes work fed via string arguments."""
        q = Queue('foo')
        w = GeventWorker([q])
        job = q.enqueue('tests.fixtures.say_hello', name='Frank')
        self.assertEqual(
            w.work(burst=True), True,
            'Expected at least some work done.'
        )
        expected_result = 'Hi there, Frank!'
        self.assertEqual(job.result, expected_result)
        # Only run if Redis server supports streams
        if job.supports_redis_streams:
            self.assertEqual(Result.fetch_latest(job).return_value, expected_result)
        self.assertIsNone(job.worker_name)

    def test_job_times(self):
        """job times are set correctly."""
        q = Queue('foo')
        w = GeventWorker([q])
        before = utcnow()
        before = before.replace(microsecond=0)
        job = q.enqueue(say_hello)
        self.assertIsNotNone(job.enqueued_at)
        self.assertIsNone(job.started_at)
        self.assertIsNone(job.ended_at)
        self.assertEqual(
            w.work(burst=True), True,
            'Expected at least some work done.'
        )
        self.assertEqual(job.result, 'Hi there, Stranger!')
        after = utcnow()
        job.refresh()
        self.assertTrue(
            before <= job.enqueued_at <= after,
            'Not %s <= %s <= %s' % (before, job.enqueued_at, after)
        )
        self.assertTrue(
            before <= job.started_at <= after,
            'Not %s <= %s <= %s' % (before, job.started_at, after)
        )
        self.assertTrue(
            before <= job.ended_at <= after,
            'Not %s <= %s <= %s' % (before, job.ended_at, after)
        )

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
        w = GeventWorker([q])
        w.work(burst=True)   # should silently pass
        self.assertEqual(q.count, 0)

        failed_job_registry = FailedJobRegistry(queue=q)
        self.assertTrue(job in failed_job_registry)

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
        w = GeventWorker([q])
        job, queue = w.dequeue_job_and_maintain_ttl(10)
        w.perform_job(job, queue)

        # An exception should be logged here at ERROR level
        self.assertIn("Traceback", mock_logger_error.call_args[0][0])

    def test_heartbeat(self):
        """Heartbeat saves last_heartbeat"""
        q = Queue()
        w = GeventWorker([q])
        w.register_birth()

        self.assertEqual(str(w.pid), as_text(self.testconn.hget(w.key, 'pid')))
        self.assertEqual(w.hostname,
                         as_text(self.testconn.hget(w.key, 'hostname')))
        last_heartbeat = self.testconn.hget(w.key, 'last_heartbeat')
        self.assertIsNotNone(self.testconn.hget(w.key, 'birth'))
        self.assertTrue(last_heartbeat is not None)
        w = GeventWorker.find_by_key(w.key)
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
        worker = GeventWorker([queue], connection=self.testconn)
        job = queue.enqueue(say_hello)
        worker.maintain_heartbeats(job)
        self.assertTrue(self.testconn.exists(worker.key))
        self.assertTrue(self.testconn.exists(job.key))

        self.testconn.delete(job.key)

        worker.maintain_heartbeats(job)
        self.assertFalse(self.testconn.exists(job.key))

    @slow
    def test_heartbeat_survives_lost_connection(self):
        with mock.patch.object(GeventWorker, 'heartbeat') as mocked:
            # None -> Heartbeat is first called before the job loop
            mocked.side_effect = [None, redis.exceptions.ConnectionError()]
            q = Queue()
            w = GeventWorker([q])
            w.work(burst=True)
            # First call is prior to job loop, second raises the error,
            # third is successful, after "recovery"
            assert mocked.call_count == 3

    def test_job_timeout_moved_to_failed_job_registry(self):
        """Jobs that run long are moved to FailedJobRegistry"""
        queue = Queue()
        worker = GeventWorker([queue])
        job = queue.enqueue(long_running_job, 5, job_timeout=1)
        worker.work(burst=True)
        self.assertIn(job, job.failed_job_registry)
        job.refresh()
        self.assertIn('rq.timeouts.JobTimeoutException', job.exc_info)

    @slow
    def test_heartbeat_busy(self):
        """Periodic heartbeats while horse is busy with long jobs"""
        q = Queue()
        w = GeventWorker([q], job_monitoring_interval=5)

        for timeout, expected_heartbeats in [(2, 0), (7, 1), (12, 2)]:
            job = q.enqueue(long_running_job,
                            args=(timeout,),
                            job_timeout=30,
                            result_ttl=-1)
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

        w = GeventWorker([q])
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
            self.assertEqual(result.type, Result.Type.FAILED)

    def test_horse_fails(self):
        """Tests that job status is set to FAILED even if horse unexpectedly fails"""
        q = Queue()
        self.assertEqual(q.count, 0)

        # Action
        job = q.enqueue(say_hello)
        self.assertEqual(q.count, 1)

        # keep for later
        enqueued_at_date = str(job.enqueued_at)

        w = GeventWorker([q])
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

    def test_statistics(self):
        """Successful and failed job counts are saved properly"""
        queue = Queue(connection=self.connection)
        job = queue.enqueue(div_by_zero)
        worker = GeventWorker([queue])
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

        worker = GeventWorker([queue])

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
        worker = GeventWorker([queue])
        worker.register_birth()

        worker.perform_job(job, queue)
        worker.refresh()
        # total_working_time should be a little bit more than 0.05 seconds
        self.assertGreaterEqual(worker.total_working_time, 0.05)
        # in multi-user environments delays might be unpredictable,
        # please adjust this magic limit accordingly in case if It takes even longer to run
        self.assertLess(worker.total_working_time, 1)

    def test_max_jobs(self):
        """GeventWorker exits after number of jobs complete."""
        queue = Queue()
        job1 = queue.enqueue(do_nothing)
        job2 = queue.enqueue(do_nothing)
        worker = GeventWorker([queue])
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
        worker = GeventWorker([queue], disable_default_exception_handler=False)
        worker.work(burst=True)

        registry = FailedJobRegistry(queue=queue)
        self.assertTrue(job in registry)

        # Job is not added to FailedJobRegistry if
        # disable_default_exception_handler is True
        job = queue.enqueue(div_by_zero)
        worker = GeventWorker([queue], disable_default_exception_handler=True)
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

        w = GeventWorker([q], exception_handlers=first_handler)
        w.work(burst=True)

        # Check the job
        job.refresh()
        self.assertEqual(job.is_failed, True)
        self.assertTrue(job.meta['first_handler'])

        job = q.enqueue(div_by_zero)
        w = GeventWorker([q], exception_handlers=[first_handler, second_handler])
        w.work(burst=True)

        # Both custom exception handlers are run
        job.refresh()
        self.assertEqual(job.is_failed, True)
        self.assertTrue(job.meta['first_handler'])
        self.assertTrue(job.meta['second_handler'])

        job = q.enqueue(div_by_zero)
        w = GeventWorker([q], exception_handlers=[first_handler, black_hole,
                                            second_handler])
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

        w = GeventWorker([q])
        w.work(burst=True)
        assert q.count == 0

        # Should not have created evidence of execution
        self.assertEqual(os.path.exists(SENTINEL_FILE), False)

    @slow  # noqa
    def test_timeouts(self):
        """GeventWorker kills jobs after timeout."""
        sentinel_file = '/tmp/.rq_sentinel'

        q = Queue()
        w = GeventWorker([q])

        # Put it on the queue with a timeout value
        res = q.enqueue(create_file_after_timeout,
                        args=(sentinel_file, 4),
                        job_timeout=1)

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
        w = GeventWorker([q])

        # Put it on the queue with a timeout value
        self.assertIsNone(w.dequeue_job_and_maintain_ttl(None))

    def test_worker_ttl_param_resolves_timeout(self):
        """Ensures the worker_ttl param is being considered in the dequeue_timeout and connection_timeout params, takes into account 15 seconds gap (hard coded)"""
        q = Queue()
        w = GeventWorker([q])
        self.assertEqual(w.dequeue_timeout, 405)
        self.assertEqual(w.connection_timeout, 415)
        w = GeventWorker([q], default_worker_ttl=500)
        self.assertEqual(w.dequeue_timeout, 485)
        self.assertEqual(w.connection_timeout, 495)

    def test_worker_sets_result_ttl(self):
        """Ensure that GeventWorker properly sets result_ttl for individual jobs."""
        q = Queue()
        job = q.enqueue(say_hello, args=('Frank',), result_ttl=10)
        w = GeventWorker([q])
        self.assertIn(job.get_id().encode(), self.testconn.lrange(q.key, 0, -1))
        w.work(burst=True)
        self.assertNotEqual(self.testconn.ttl(job.key), 0)
        self.assertNotIn(job.get_id().encode(), self.testconn.lrange(q.key, 0, -1))

        # Job with -1 result_ttl don't expire
        job = q.enqueue(say_hello, args=('Frank',), result_ttl=-1)
        w = GeventWorker([q])
        self.assertIn(job.get_id().encode(), self.testconn.lrange(q.key, 0, -1))
        w.work(burst=True)
        self.assertEqual(self.testconn.ttl(job.key), -1)
        self.assertNotIn(job.get_id().encode(), self.testconn.lrange(q.key, 0, -1))

        # Job with result_ttl = 0 gets deleted immediately
        job = q.enqueue(say_hello, args=('Frank',), result_ttl=0)
        w = GeventWorker([q])
        self.assertIn(job.get_id().encode(), self.testconn.lrange(q.key, 0, -1))
        w.work(burst=True)
        self.assertEqual(self.testconn.get(job.key), None)
        self.assertNotIn(job.get_id().encode(), self.testconn.lrange(q.key, 0, -1))

    def test_worker_sets_job_status(self):
        """Ensure that worker correctly sets job status."""
        q = Queue()
        w = GeventWorker([q])

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
        worker = GeventWorker([q])
        job = q.enqueue_call(say_hello)

        self.assertEqual(self.testconn.hget(worker.key, 'current_job'), None)
        worker.set_current_job_id(job.id)
        self.assertEqual(
            worker.get_current_job_id(),
            as_text(self.testconn.hget(worker.key, 'current_job'))
        )
        self.assertEqual(worker.get_current_job(), job)

    def test_custom_job_class(self):
        """Ensure GeventWorker accepts custom job class."""
        q = Queue()
        worker = GeventWorker([q], job_class=CustomJob)
        self.assertEqual(worker.job_class, CustomJob)

    def test_custom_queue_class(self):
        """Ensure GeventWorker accepts custom queue class."""
        q = CustomQueue()
        worker = GeventWorker([q], queue_class=CustomQueue)
        self.assertEqual(worker.queue_class, CustomQueue)

    def test_custom_queue_class_is_not_global(self):
        """Ensure GeventWorker custom queue class is not global."""
        q = CustomQueue()
        worker_custom = GeventWorker([q], queue_class=CustomQueue)
        q_generic = Queue()
        worker_generic = GeventWorker([q_generic])
        self.assertEqual(worker_custom.queue_class, CustomQueue)
        self.assertEqual(worker_generic.queue_class, Queue)
        self.assertEqual(GeventWorker.queue_class, Queue)

    def test_custom_job_class_is_not_global(self):
        """Ensure GeventWorker custom job class is not global."""
        q = Queue()
        worker_custom = GeventWorker([q], job_class=CustomJob)
        q_generic = Queue()
        worker_generic = GeventWorker([q_generic])
        self.assertEqual(worker_custom.job_class, CustomJob)
        self.assertEqual(worker_generic.job_class, Job)
        self.assertEqual(GeventWorker.job_class, Job)

    def test_prepare_job_execution(self):
        """Prepare job execution does the necessary bookkeeping."""
        queue = Queue(connection=self.testconn)
        job = queue.enqueue(say_hello)
        worker = GeventWorker([queue])
        worker.prepare_job_execution(job)

        # Updates working queue
        registry = StartedJobRegistry(connection=self.testconn)
        self.assertEqual(registry.get_job_ids(), [job.id])

        # Updates worker's current job
        self.assertEqual(worker.get_current_job_id(), job.id)

        # job status is also updated
        self.assertEqual(job._status, JobStatus.STARTED)
        self.assertEqual(job.worker_name, worker.name)

    def test_work_unicode_friendly(self):
        """GeventWorker processes work with unicode description, then quits."""
        q = Queue('foo')
        w = GeventWorker([q])
        job = q.enqueue('tests.fixtures.say_hello', name='Adam',
                        description='你好 世界!')
        self.assertEqual(w.work(burst=True), True,
                         'Expected at least some work done.')
        self.assertEqual(job.result, 'Hi there, Adam!')
        self.assertEqual(job.description, '你好 世界!')

    def test_work_log_unicode_friendly(self):
        """GeventWorker process work with unicode or str other than pure ascii content,
        logging work properly"""
        q = Queue("foo")
        w = GeventWorker([q])

        job = q.enqueue('tests.fixtures.say_hello', name='阿达姆',
                        description='你好 世界!')
        w.work(burst=True)
        self.assertEqual(job.get_status(), JobStatus.FINISHED)

        job = q.enqueue('tests.fixtures.say_hello_unicode', name='阿达姆',
                        description='你好 世界!')
        w.work(burst=True)
        self.assertEqual(job.get_status(), JobStatus.FINISHED)

    def test_suspend_worker_execution(self):
        """Test Pause GeventWorker Execution"""

        SENTINEL_FILE = '/tmp/rq-tests.txt'  # noqa

        try:
            # Remove the sentinel if it is leftover from a previous test run
            os.remove(SENTINEL_FILE)
        except OSError as e:
            if e.errno != 2:
                raise

        q = Queue()
        q.enqueue(create_file, SENTINEL_FILE)

        w = GeventWorker([q])

        suspend(self.testconn)

        w.work(burst=True)
        assert q.count == 1

        # Should not have created evidence of execution
        self.assertEqual(os.path.exists(SENTINEL_FILE), False)

        resume(self.testconn)
        w.work(burst=True)
        assert q.count == 0
        self.assertEqual(os.path.exists(SENTINEL_FILE), True)

    @slow
    def test_suspend_with_duration(self):
        q = Queue()
        for _ in range(5):
            q.enqueue(do_nothing)

        w = GeventWorker([q])

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
        w1 = GeventWorker([q], name="worker1")
        w2 = GeventWorker([q], name="worker2")
        w3 = GeventWorker([q], name="worker1")
        worker_set = set([w1, w2, w3])
        self.assertEqual(len(worker_set), 2)

    def test_worker_sets_birth(self):
        """Ensure worker correctly sets worker birth date."""
        q = Queue()
        w = GeventWorker([q])

        w.register_birth()

        birth_date = w.birth_date
        self.assertIsNotNone(birth_date)
        self.assertEqual(type(birth_date).__name__, 'datetime')

    def test_worker_sets_death(self):
        """Ensure worker correctly sets worker death date."""
        q = Queue()
        w = GeventWorker([q])

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

        worker = GeventWorker([foo_queue, bar_queue])
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
        worker = GeventWorker(queue)
        self.assertTrue(worker.should_run_maintenance_tasks)

        worker.last_cleaned_at = utcnow()
        self.assertFalse(worker.should_run_maintenance_tasks)
        worker.last_cleaned_at = utcnow() - timedelta(seconds=3700)
        self.assertTrue(worker.should_run_maintenance_tasks)

    def test_worker_calls_clean_registries(self):
        """GeventWorker calls clean_registries when run."""
        queue = Queue(connection=self.testconn)
        registry = StartedJobRegistry(connection=self.testconn)
        self.testconn.zadd(registry.key, {'foo': 1})

        worker = GeventWorker(queue, connection=self.testconn)
        worker.work(burst=True)
        self.assertEqual(self.testconn.zcard(registry.key), 0)

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
        w = GeventWorker([q])
        with mock.patch.object(GeventWorker, 'execute_job', wraps=w.execute_job) as mocked:
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

    def test_self_modification_persistence(self):
        """Make sure that any meta modification done by
        the job itself persists completely through the
        queue/worker/job stack."""
        q = Queue()
        # Also make sure that previously existing metadata
        # persists properly
        job = q.enqueue(modify_self, meta={'foo': 'bar', 'baz': 42},
                        args=[{'baz': 10, 'newinfo': 'waka'}])

        w = GeventWorker([q])
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
        job = q.enqueue(modify_self_and_error, meta={'foo': 'bar', 'baz': 42},
                        args=[{'baz': 10, 'newinfo': 'waka'}])

        w = GeventWorker([q])
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

        w = GeventWorker([q])
        job = q.enqueue(say_hello, args=('Frank',), result_ttl=10)
        w.perform_job(job, q)
        mock_logger_info.assert_called_with('Result is kept for %s seconds', 10)
        self.assertIn('Result is kept for %s seconds', [c[0][0] for c in mock_logger_info.call_args_list])

    @mock.patch('rq.worker.logger.info')
    def test_log_result_lifespan_false(self, mock_logger_info):
        """Check that log_result_lifespan False causes job lifespan to not be logged."""
        q = Queue()

        class TestWorker(GeventWorker):
            log_result_lifespan = False

        w = TestWorker([q])
        job = q.enqueue(say_hello, args=('Frank',), result_ttl=10)
        w.perform_job(job, q)
        self.assertNotIn('Result is kept for 10 seconds', [c[0][0] for c in mock_logger_info.call_args_list])

    @mock.patch('rq.worker.logger.info')
    def test_log_job_description_true(self, mock_logger_info):
        """Check that log_job_description True causes job lifespan to be logged."""
        q = Queue()
        w = GeventWorker([q])
        q.enqueue(say_hello, args=('Frank',), result_ttl=10)
        w.dequeue_job_and_maintain_ttl(10)
        self.assertIn("Frank", mock_logger_info.call_args[0][2])

    @mock.patch('rq.worker.logger.info')
    def test_log_job_description_false(self, mock_logger_info):
        """Check that log_job_description False causes job lifespan to not be logged."""
        q = Queue()
        w = GeventWorker([q], log_job_description=False)
        q.enqueue(say_hello, args=('Frank',), result_ttl=10)
        w.dequeue_job_and_maintain_ttl(10)
        self.assertNotIn("Frank", mock_logger_info.call_args[0][2])

    def test_worker_configures_socket_timeout(self):
        """Ensures that the worker correctly updates Redis client connection to have a socket_timeout"""
        q = Queue()
        _ = GeventWorker([q])
        connection_kwargs = q.connection.connection_pool.connection_kwargs
        self.assertEqual(connection_kwargs["socket_timeout"], 415)

    def test_worker_version(self):
        q = Queue()
        w = GeventWorker([q])
        w.version = '0.0.0'
        w.register_birth()
        self.assertEqual(w.version, '0.0.0')
        w.refresh()
        self.assertEqual(w.version, '0.0.0')
        # making sure that version is preserved when worker is retrieved by key
        worker = GeventWorker.find_by_key(w.key)
        self.assertEqual(worker.version, '0.0.0')

    def test_python_version(self):
        python_version = sys.version
        q = Queue()
        w = GeventWorker([q])
        w.register_birth()
        self.assertEqual(w.python_version, python_version)
        # now patching version
        python_version = 'X.Y.Z.final'  # dummy version
        self.assertNotEqual(python_version, sys.version)  # otherwise tests are pointless
        w2 = GeventWorker([q])
        w2.python_version = python_version
        w2.register_birth()
        self.assertEqual(w2.python_version, python_version)
        # making sure that version is preserved when worker is retrieved by key
        worker = GeventWorker.find_by_key(w2.key)
        self.assertEqual(worker.python_version, python_version)

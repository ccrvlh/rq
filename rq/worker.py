import contextlib
import errno
import logging
import math
import os
import random
import signal
import socket
import sys
import threading
import time
import traceback
import warnings
import redis.exceptions

from datetime import datetime
from datetime import timedelta
from random import shuffle
from types import FrameType
from typing import TYPE_CHECKING
from typing import Set
from typing import Union
from typing import Type
from typing import Tuple
from typing import Optional
from typing import List
from typing import Callable
from uuid import uuid4
from redis import Redis
from contextlib import suppress
from concurrent.futures import ThreadPoolExecutor

from rq.commands import handle_command
from rq.commands import parse_payload
from rq.commands import PUBSUB_CHANNEL_TEMPLATE
from rq.connections import get_current_connection
from rq.connections import pop_connection
from rq.connections import push_connection
from rq.defaults import (
    DEFAULT_CPU_THREADS,
    DEFAULT_JOB_MONITORING_INTERVAL,
    DEFAULT_LOGGING_DATE_FORMAT,
    DEFAULT_LOGGING_FORMAT,
    DEFAULT_MAINTENANCE_TASK_INTERVAL,
    DEFAULT_RESULT_TTL,
    DEFAULT_WORKER_TTL,
    MAX_KEYS,
    REDIS_WORKER_KEYS,
    WORKERS_BY_QUEUE_KEY,
)
from rq import utils
from rq.version import VERSION
from rq.const import WorkerStatus
from rq.const import DequeueStrategy
from rq.exceptions import DequeueTimeout
from rq.exceptions import DeserializationError
from rq.exceptions import StopRequested
from rq.exceptions import HorseMonitorTimeoutException
from rq.job import Job
from rq.job import JobStatus
from rq.queue import Queue
from rq.logutils import blue, green, setup_loghandlers, yellow
from rq.registries import clean_job_registries
from rq.registries import StartedJobRegistry
from rq.scheduler import Scheduler
from rq.serializers import resolve_serializer
from rq.suspension import is_suspended
from rq.timeouts import DeathPenaltyInterface
from rq.timeouts import TimerDeathPenalty
from rq.timeouts import JobTimeoutException
from rq.timeouts import UnixSignalDeathPenalty

try:
    from signal import SIGKILL
except ImportError:
    from signal import SIGTERM as SIGKILL

try:
    from setproctitle import setproctitle as setprocname
except ImportError:
    def setprocname(*args, **kwargs):  # noqa
        pass

if TYPE_CHECKING:
    try:
        from resource import struct_rusage
    except ImportError:
        pass
    from redis import Redis
    from redis.client import Pipeline
    from redis.client import PubSub


logger = logging.getLogger("rq.worker")


class Worker:
    redis_worker_namespace_prefix = 'rq:worker:'
    """ The Redis key namespace used for all keys set by RQ for a worker. """

    redis_workers_keys = REDIS_WORKER_KEYS
    """ The Redis key namespace used for all keys set by RQ for a worker. """

    death_penalty_class: type[DeathPenaltyInterface] = UnixSignalDeathPenalty
    """ The default death penalty class. """

    queue_class = Queue
    """The Queue class associated with this worker."""

    job_class = Job
    """The Job class associated with this worker."""

    log_result_lifespan = True
    """`log_job_description` is used to toggle logging an entire jobs description.
    Controls whether "Result is kept for XXX seconds" messages are logged after every job, by default they are.
    """

    log_job_description = True
    """ factor to increase connection_wait_time in case of continuous connection failures."""

    exponential_backoff_factor = 2.0
    """ Max Wait time (in seconds) after which exponential_backoff_factor won't be applicable."""

    max_connection_wait_time = 60.0
    """ The default connection timeout for the Redis connection."""

    def __init__(
        self,
        queues,
        name: Optional[str] = None,
        default_result_ttl=DEFAULT_RESULT_TTL,
        connection: Optional['Redis'] = None,
        exc_handler=None,
        exception_handlers=None,
        default_worker_ttl=DEFAULT_WORKER_TTL,
        maintenance_interval: int = DEFAULT_MAINTENANCE_TASK_INTERVAL,
        job_class: Optional[Type['Job']] = None,
        queue_class: Optional[Type['Queue']] = None,
        log_job_description: bool = True,
        job_monitoring_interval=DEFAULT_JOB_MONITORING_INTERVAL,
        disable_default_exception_handler: bool = False,
        prepare_for_work: bool = True,
        serializer=None
    ):  # noqa
        self.default_result_ttl = default_result_ttl
        self.worker_ttl = default_worker_ttl
        self.job_monitoring_interval = job_monitoring_interval
        self.maintenance_interval = maintenance_interval

        connection = self._set_connection(connection)
        self.connection = connection
        self.redis_server_version = None

        self.job_class = utils.backend_class(self, 'job_class', override=job_class)
        self.queue_class = utils.backend_class(self, 'queue_class', override=queue_class)
        self.version = VERSION
        self.python_version = sys.version
        self.serializer = resolve_serializer(serializer)
        self.name: str = name or uuid4().hex
        self.queues = self._parse_queues(queues)
        self._ordered_queues = self.queues[:]
        self._exc_handlers: List[Callable] = []
        self._shutdown_requested_date: Optional[datetime] = None

        self._state: 'WorkerStatus' = WorkerStatus.STARTING
        self._stop_requested: bool = False
        self._stopped_job_id = None

        self.log = logger
        self.log_job_description = log_job_description
        self.last_cleaned_at = None
        self.successful_job_count: int = 0
        self.failed_job_count: int = 0
        self.total_working_time: float = 0
        self.current_job_working_time: float = 0
        self.birth_date = None
        self.scheduler: Optional[Scheduler] = None
        self.pubsub: Optional['PubSub'] = None
        self.pubsub_thread = None
        self._dequeue_strategy: DequeueStrategy = DequeueStrategy.DEFAULT
        self.disable_default_exception_handler = disable_default_exception_handler
        self.hostname: Optional[str] = None
        self.pid: Optional[int] = None
        self.ip_address: str = 'unknown'
        self._set_host_details(prepare_for_work=prepare_for_work)

        if isinstance(exception_handlers, (list, tuple)):
            for handler in exception_handlers:
                self.push_exc_handler(handler)
        elif exception_handlers is not None:
            self.push_exc_handler(exception_handlers)

    @property
    def should_run_maintenance_tasks(self):
        """Maintenance tasks should run on first startup or every 10 minutes."""
        if self.last_cleaned_at is None:
            return True
        if (utils.utcnow() - self.last_cleaned_at) > timedelta(seconds=self.maintenance_interval):
            return True
        return False

    @property
    def dequeue_timeout(self) -> int:
        return max(1, self.worker_ttl - 15)
    
    @property
    def connection_timeout(self) -> int:
        return self.dequeue_timeout + 10

    @property
    def key(self):
        """Returns the worker's Redis hash key."""
        return self.redis_worker_namespace_prefix + self.name

    @property
    def pubsub_channel_name(self):
        """Returns the worker's Redis hash key."""
        return PUBSUB_CHANNEL_TEMPLATE % self.name

    @property
    def supports_redis_streams(self) -> bool:
        """Only supported by Redis server >= 5.0 is required."""
        return self.get_redis_server_version() >= (5, 0, 0)

    @property
    def shutdown_requested_date(self):
        """Fetches shutdown_requested_date from Redis."""
        shutdown_requested_timestamp = self.connection.hget(self.key, 'shutdown_requested_date')
        if shutdown_requested_timestamp is not None:
            return utils.utcparse(utils.as_text(shutdown_requested_timestamp))

    @property
    def death_date(self):
        """Fetches death date from Redis."""
        death_timestamp = self.connection.hget(self.key, 'death')
        if death_timestamp is not None:
            return utils.utcparse(utils.as_text(death_timestamp))

    # Setup Setters

    def _parse_queues(self, queues: Union[str, List[str]]) -> List['Queue']:
        queues_lists = [
            self.queue_class(
                name=q,
                connection=self.connection,
                job_class=self.job_class,
                serializer=self.serializer,
                death_penalty_class=self.death_penalty_class,
            )
            if isinstance(q, str)
            else q
            for q in utils.ensure_list(queues)
        ]
        for queue in queues_lists:
            if not isinstance(queue, self.queue_class):
                raise TypeError('{0} is not of type {1} or string types'.format(queue, self.queue_class))
        return queues_lists

    def _set_host_details(self, prepare_for_work: bool = False):
        """Sets specific host and process information.

        Args:
            should_prepare (bool, optional): Whether to prepare for work. Defaults to False.
        """
        if not prepare_for_work:
            return

        self.hostname = socket.gethostname()
        self.pid = os.getpid()

        try:
            self.connection.client_setname(self.name)
        except redis.exceptions.ResponseError:
            warnings.warn('CLIENT SETNAME command not supported, setting ip_address to unknown', Warning)
            return
        
        try:
            client_list = self.connection.client_list()
        except redis.exceptions.ResponseError:
            warnings.warn('CLIENT LIST command not supported, setting ip_address to unknown', Warning)
            return
        
        client_adresses = [client['addr'] for client in client_list if client['name'] == self.name]
        if len(client_adresses) > 0:
            self.ip_address = client_adresses[0]

    def set_current_job_working_time(self, current_job_working_time: float, pipeline: Optional['Pipeline'] = None):
        """Sets the current job working time in seconds

        Args:
            current_job_working_time (float): The current job working time in seconds
            pipeline (Optional[Pipeline], optional): Pipeline to use. Defaults to None.
        """
        self.current_job_working_time = current_job_working_time
        connection = pipeline if pipeline is not None else self.connection
        connection.hset(self.key, 'current_job_working_time', current_job_working_time)

    def set_current_job_id(self, job_id: Optional[str] = None, pipeline: Optional['Pipeline'] = None):
        """Sets the current job id.
        If `None` is used it will delete the current job key.

        Args:
            job_id (Optional[str], optional): The job id. Defaults to None.
            pipeline (Optional[Pipeline], optional): The pipeline to use. Defaults to None.
        """
        connection = pipeline if pipeline is not None else self.connection
        if job_id is None:
            connection.hdel(self.key, 'current_job')
        else:
            connection.hset(self.key, 'current_job', job_id)

    def get_current_job_id(self, pipeline: Optional['Pipeline'] = None) -> Optional[str]:
        """Retrieves the current job id.

        Args:
            pipeline (Optional[&#39;Pipeline&#39;], optional): The pipeline to use. Defaults to None.

        Returns:
            job_id (Optional[str): The job id
        """
        connection = pipeline if pipeline is not None else self.connection
        result = connection.hget(self.key, 'current_job')
        if result is None:
            return None
        return utils.as_text(result)

    def get_current_job(self) -> Optional['Job']:
        """Returns the currently executing job instance.

        Returns:
            job (Job): The job instance.
        """
        job_id = self.get_current_job_id()
        if job_id is None:
            return None
        return self.job_class.fetch(job_id, self.connection, self.serializer)

    def set_state(self, state: 'WorkerStatus'):
        """Sets the worker's state.

        Args:
            state (str): The state
        """
        self._state = state
        self.connection.hset(self.key, 'state', state)

    def get_state(self) -> 'WorkerStatus':
        return self._state

    def _set_connection(self, connection: Optional['Redis']) -> 'Redis':
        """Configures the Redis connection to have a socket timeout.
        This should timouet the connection in case any specific command hangs at any given time (eg. BLPOP).
        If the connection provided already has a `socket_timeout` defined, skips.

        Args:
            connection (Optional[Redis]): The Redis Connection.
        """
        if connection is None:
            connection = get_current_connection()
        current_socket_timeout = connection.connection_pool.connection_kwargs.get("socket_timeout")
        if current_socket_timeout is None:
            timeout_config = {"socket_timeout": self.connection_timeout}
            connection.connection_pool.connection_kwargs.update(timeout_config)
        return connection

    def _install_signal_handlers(self):
        """Installs signal handlers for handling SIGINT and SIGTERM gracefully."""
        signal.signal(signal.SIGINT, self.request_stop)
        signal.signal(signal.SIGTERM, self.request_stop)

    def _start_scheduler(
        self,
        burst: bool = False,
        logging_level: str = "INFO",
        date_format: str = DEFAULT_LOGGING_DATE_FORMAT,
        log_format: str = DEFAULT_LOGGING_FORMAT,
    ):
        """Starts the scheduler process.
        This is specifically designed to be run by the worker when running the `work()` method.
        Instanciates the Scheduler and tries to acquire a lock.
        If the lock is acquired, start scheduler.
        If worker is on burst mode just enqueues scheduled jobs and quits,
        otherwise, starts the scheduler in a separate process.

        Args:
            burst (bool, optional): Whether to work on burst mode. Defaults to False.
            logging_level (str, optional): Logging level to use. Defaults to "INFO".
            date_format (str, optional): Date Format. Defaults to DEFAULT_LOGGING_DATE_FORMAT.
            log_format (str, optional): Log Format. Defaults to DEFAULT_LOGGING_FORMAT.
        """
        self.scheduler = Scheduler(
            self.queues,
            connection=self.connection,
            logging_level=logging_level,
            date_format=date_format,
            log_format=log_format,
            serializer=self.serializer,
        )
        self.scheduler.acquire_locks()
        if self.scheduler.acquired_locks:
            if burst:
                self.scheduler.enqueue_scheduled_jobs()
                self.scheduler.release_locks()
            else:
                self.scheduler.start()

    # Housekeeping

    def run_maintenance_tasks(self):
        """
        Runs periodic maintenance tasks, these include:
        1. Check if scheduler should be started. This check should not be run
           on first run since worker.work() already calls
           `scheduler.enqueue_scheduled_jobs()` on startup.
        2. Cleaning registries

        No need to try to start scheduler on first run
        """
        if self.last_cleaned_at:
            if self.scheduler and (not self.scheduler._process or not self.scheduler._process.is_alive()):
                self.scheduler.acquire_locks(auto_start=True)
        self.clean_registries()

    def procline(self, message):
        """Changes the current procname for the process.

        This can be used to make `ps -ef` output more readable.
        """
        setprocname(f'rq:worker:{self.name}: {message}')

    def push_exc_handler(self, handler_func):
        """Pushes an exception handler onto the exc handler stack."""
        self._exc_handlers.append(handler_func)

    def get_heartbeat_ttl(self, job: 'Job') -> int:
        """Get's the TTL for the next heartbeat.
        SimpleWorker Code:

        Example::
            ..code-block:: python

                >>> def get_heartbeat_ttl(self, job):
                >>>     if job.timeout == -1:
                >>>         return DEFAULT_WORKER_TTL
                >>>     else:
                >>>         return int((job.timeout or DEFAULT_WORKER_TTL)) + 60

        Args:
            job (Job): The Job

        Returns:
            int: The heartbeat TTL.
        """
        if job.timeout and job.timeout > 0:
            remaining_execution_time = job.timeout - self.current_job_working_time
            return int(min(remaining_execution_time, self.job_monitoring_interval)) + 60
        else:
            return self.job_monitoring_interval + 60
        
    def maintain_heartbeats(self, job: 'Job'):
        """Updates worker and job's last heartbeat field. If job was
        enqueued with `result_ttl=0`, a race condition could happen where this heartbeat
        arrives after job has been deleted, leaving a job key that contains only
        `last_heartbeat` field.

        hset() is used when updating job's timestamp. This command returns 1 if a new
        Redis key is created, 0 otherwise. So in this case we check the return of job's
        heartbeat() command. If a new key was created, this means the job was already
        deleted. In this case, we simply send another delete command to remove the key.

        https://github.com/rq/rq/issues/1450
        """
        with self.connection.pipeline() as pipeline:
            self.heartbeat(self.job_monitoring_interval + 60, pipeline=pipeline)
            ttl = self.get_heartbeat_ttl(job)
            job.heartbeat(utils.utcnow(), ttl, pipeline=pipeline, xx=True)
            results = pipeline.execute()
            if results[2] == 1:
                self.connection.delete(job.key)

    def register_birth(self):
        """Registers its own birth."""
        self.log.debug('Registering birth of worker %s', self.name)
        if self.connection.exists(self.key) and not self.connection.hexists(self.key, 'death'):
            msg = 'There exists an active worker named {0!r} already'
            raise ValueError(msg.format(self.name))
        key = self.key
        queues = ','.join(self.queue_names())
        with self.connection.pipeline() as p:
            p.delete(key)
            now = utils.utcnow()
            now_in_string = utils.utcformat(now)
            self.birth_date = now

            mapping = {
                'birth': now_in_string,
                'last_heartbeat': now_in_string,
                'queues': queues,
                'pid': self.pid,
                'hostname': self.hostname,
                'ip_address': self.ip_address,
                'version': self.version,
                'python_version': self.python_version,
            }

            if self.get_redis_server_version() >= (4, 0, 0):
                p.hset(key, mapping=mapping)
            else:
                p.hmset(key, mapping)

            self.register(self, p)
            p.expire(key, self.worker_ttl + 60)
            p.execute()

    def register_death(self):
        """Registers its own death."""
        self.log.debug('Registering death')
        with self.connection.pipeline() as p:
            # We cannot use self.get_state() = 'dead' here, because that would
            # rollback the pipeline
            Worker.unregister(self, p)
            p.hset(self.key, 'death', utils.utcformat(utils.utcnow()))
            p.expire(self.key, 60)
            p.execute()

    def subscribe(self):
        """Subscribe to this worker's channel"""
        self.log.info('Subscribing to channel %s', self.pubsub_channel_name)
        self.pubsub = self.connection.pubsub()
        self.pubsub.subscribe(**{self.pubsub_channel_name: self.handle_payload})
        self.pubsub_thread = self.pubsub.run_in_thread(sleep_time=0.2, daemon=True)

    def unsubscribe(self):
        """Unsubscribe from pubsub channel"""
        if self.pubsub_thread:
            self.log.info('Unsubscribing from channel %s', self.pubsub_channel_name)
            self.pubsub_thread.stop()
            self.pubsub_thread.join()
            self.pubsub.unsubscribe()
            self.pubsub.close()

    # Utils

    def refresh(self):
        """Refreshes the worker data.
        It will get the data from the datastore and update the Worker's attributes
        """
        data = self.connection.hmget(
            self.key,
            'queues',
            'state',
            'current_job',
            'last_heartbeat',
            'birth',
            'failed_job_count',
            'successful_job_count',
            'total_working_time',
            'current_job_working_time',
            'hostname',
            'ip_address',
            'pid',
            'version',
            'python_version',
        )
        (
            queues,
            state,
            job_id,
            last_heartbeat,
            birth,
            failed_job_count,
            successful_job_count,
            total_working_time,
            current_job_working_time,
            hostname,
            ip_address,
            pid,
            version,
            python_version,
        ) = data
        self.hostname = utils.as_text(hostname) if hostname else None
        self.ip_address = utils.as_text(ip_address) if ip_address else None
        self.pid = int(pid) if pid else None
        self.version = utils.as_text(version) if version else None
        self.python_version = utils.as_text(python_version) if python_version else None

        try:
            _state = WorkerStatus(utils.as_text(state)).value
        except ValueError:
            _state = WorkerStatus.UNKNOWN

        self._state = _state
        self._job_id = job_id or None
        if last_heartbeat:
            self.last_heartbeat = utils.utcparse(utils.as_text(last_heartbeat))
        else:
            self.last_heartbeat = None
        if birth:
            self.birth_date = utils.utcparse(utils.as_text(birth))
        else:
            self.birth_date = None
        if failed_job_count:
            self.failed_job_count = int(utils.as_text(failed_job_count))
        if successful_job_count:
            self.successful_job_count = int(utils.as_text(successful_job_count))
        if total_working_time:
            self.total_working_time = float(utils.as_text(total_working_time))
        if current_job_working_time:
            self.current_job_working_time = float(utils.as_text(current_job_working_time))

        if queues:
            queues = utils.as_text(queues)
            self.queues = [
                self.queue_class(
                    queue, connection=self.connection, job_class=self.job_class, serializer=self.serializer
                )
                for queue in queues.split(',')
            ]

    def clean_registries(self):
        """Runs maintenance jobs on each Queue's registries."""
        for queue in self.queues:
            # If there are multiple workers running, we only want 1 worker
            # to run clean_registries().
            if queue.acquire_maintenance_lock():
                self.log.info('Cleaning registries for queue: %s', queue.name)
                clean_job_registries(queue)
                self.clean_worker_registry(queue)
                self.clean_intermediate_queue(self, queue)
        self.last_cleaned_at = utils.utcnow()

    def get_redis_server_version(self):
        """Return Redis server version of connection"""
        if not self.redis_server_version:
            self.redis_server_version = utils.get_version(self.connection)
        return self.redis_server_version

    def queue_names(self) -> List[str]:
        """Returns the queue names of this worker's queues.

        Returns:
            List[str]: The queue names.
        """
        return [queue.name for queue in self.queues]

    def queue_keys(self) -> List[str]:
        """Returns the Redis keys representing this worker's queues.

        Returns:
            List[str]: The list of strings with queues keys
        """
        return [queue.key for queue in self.queues]

    def reorder_queues(self, reference_queue: 'Queue'):
        """Reorder the queues according to the strategy.
        As this can be defined both in the `Worker` initialization or in the `work` method,
        it doesn't take the strategy directly, but rather uses the private `_dequeue_strategy` attribute.

        Args:
            reference_queue (Union[Queue, str]): The queues to reorder
        """
        if self._dequeue_strategy is None:
            self._dequeue_strategy = DequeueStrategy.DEFAULT

        if self._dequeue_strategy not in ("default", "random", "round_robin"):
            raise ValueError(
                f"Dequeue strategy {self._dequeue_strategy} is not allowed. Use `default`, `random` or `round_robin`."
            )
        if self._dequeue_strategy == DequeueStrategy.DEFAULT:
            return
        if self._dequeue_strategy == DequeueStrategy.ROUND_ROBIN:
            pos = self._ordered_queues.index(reference_queue)
            self._ordered_queues = self._ordered_queues[pos + 1 :] + self._ordered_queues[: pos + 1]
            return
        if self._dequeue_strategy == DequeueStrategy.RANDOM:
            shuffle(self._ordered_queues)
            return

    # Work & Execution

    def dequeue_job_and_maintain_ttl(
        self, timeout: Optional[int], max_idle_time: Optional[int] = None
    ) -> Tuple['Job', 'Queue']:
        """Dequeues a job while maintaining the TTL.

        Returns:
            result (Tuple[Job, Queue]): A tuple with the job and the queue.
        """
        result = None
        qnames = ','.join(self.queue_names())

        self.set_state(WorkerStatus.IDLE)
        self.procline('Listening on ' + qnames)
        self.log.debug('*** Listening on %s...', green(qnames))
        connection_wait_time = 1.0
        idle_since = utils.utcnow()
        idle_time_left = max_idle_time
        while True:
            try:
                self.heartbeat()

                if self.should_run_maintenance_tasks:
                    self.run_maintenance_tasks()

                if timeout is not None and idle_time_left is not None:
                    timeout = min(timeout, idle_time_left)

                self.log.debug('Dequeueing jobs on queues %s and timeout %s', green(qnames), timeout)
                result = self.queue_class.dequeue_any(
                    self._ordered_queues,
                    timeout,
                    connection=self.connection,
                    job_class=self.job_class,
                    serializer=self.serializer,
                    death_penalty_class=self.death_penalty_class,
                )
                if result is not None:
                    job, queue = result
                    self.reorder_queues(reference_queue=queue)
                    self.log.debug('Dequeued job %s from %s', blue(job.id), green(queue.name))
                    job.redis_server_version = self.get_redis_server_version()
                    if self.log_job_description:
                        self.log.info('%s: %s (%s)', green(queue.name), blue(job.description), job.id)
                    else:
                        self.log.info('%s: %s', green(queue.name), job.id)

                break
            except DequeueTimeout:
                if max_idle_time is not None:
                    idle_for = (utils.utcnow() - idle_since).total_seconds()
                    idle_time_left = math.ceil(max_idle_time - idle_for)
                    if idle_time_left <= 0:
                        break
            except redis.exceptions.ConnectionError as conn_err:
                self.log.error(
                    'Could not connect to Redis instance: %s Retrying in %d seconds...', conn_err, connection_wait_time
                )
                time.sleep(connection_wait_time)
                connection_wait_time *= self.exponential_backoff_factor
                connection_wait_time = min(connection_wait_time, self.max_connection_wait_time)
            else:
                connection_wait_time = 1.0

        self.heartbeat()
        return result

    def heartbeat(self, timeout: Optional[int] = None, pipeline: Optional['Pipeline'] = None):
        """Specifies a new worker timeout, typically by extending the
        expiration time of the worker, effectively making this a "heartbeat"
        to not expire the worker until the timeout passes.

        The next heartbeat should come before this time, or the worker will
        die (at least from the monitoring dashboards).

        If no timeout is given, the worker_ttl will be used to update
        the expiration time of the worker.

        Args:
            timeout (Optional[int]): Timeout
            pipeline (Optional[Redis]): A Redis pipeline
        """
        timeout = timeout or self.worker_ttl + 60
        connection: Union[Redis, 'Pipeline'] = pipeline if pipeline is not None else self.connection
        connection.expire(self.key, timeout)
        connection.hset(self.key, 'last_heartbeat', utils.utcformat(utils.utcnow()))
        self.log.debug('Sent heartbeat to prevent worker timeout. Next one should arrive in %s seconds.', timeout)

    def bootstrap(
        self,
        logging_level: str = "INFO",
        date_format: str = DEFAULT_LOGGING_DATE_FORMAT,
        log_format: str = DEFAULT_LOGGING_FORMAT,
    ):
        """Bootstraps the worker.
        Runs the basic tasks that should run when the worker actually starts working.
        Used so that new workers can focus on the work loop implementation rather
        than the full bootstraping process.

        Args:
            logging_level (str, optional): Logging level to use. Defaults to "INFO".
            date_format (str, optional): Date Format. Defaults to DEFAULT_LOGGING_DATE_FORMAT.
            log_format (str, optional): Log Format. Defaults to DEFAULT_LOGGING_FORMAT.
        """
        setup_loghandlers(logging_level, date_format, log_format)
        self.register_birth()
        self.log.info('Worker %s started with PID %d, version %s', self.key, os.getpid(), VERSION)
        self.subscribe()
        self.set_state(WorkerStatus.STARTED)
        qnames = self.queue_names()
        self.log.info('*** Listening on %s...', green(', '.join(qnames)))

    def prepare_job_execution(self, job: 'Job', remove_from_intermediate_queue: bool = False):
        """Performs misc bookkeeping like updating states prior to
        job execution.
        """
        self.log.debug('Preparing for execution of Job ID %s', job.id)
        with self.connection.pipeline() as pipeline:
            self.set_current_job_id(job.id, pipeline=pipeline)
            self.set_current_job_working_time(0, pipeline=pipeline)

            heartbeat_ttl = self.get_heartbeat_ttl(job)
            self.heartbeat(heartbeat_ttl, pipeline=pipeline)
            job.heartbeat(utils.utcnow(), heartbeat_ttl, pipeline=pipeline)

            job.prepare_for_execution(self.name, pipeline=pipeline)
            if remove_from_intermediate_queue:
                from rq.queue import Queue

                queue = Queue(job.origin, connection=self.connection)
                pipeline.lrem(queue.intermediate_queue_key, 1, job.id)
            pipeline.execute()
            self.log.debug('Job preparation finished.')

        msg = 'Processing {0} from {1} since {2}'
        self.procline(msg.format(job.func_name, job.origin, time.time()))

    def handle_payload(self, message):
        """Handle external commands"""
        self.log.debug('Received message: %s', message)
        payload = parse_payload(message)
        handle_command(self, payload)

    def perform_job(self, job: 'Job', queue: 'Queue') -> bool:
        """Performs the actual work of a job.  Will/should only be called
        inside the work horse's process.

        Args:
            job (Job): The Job
            queue (Queue): The Queue

        Returns:
            bool: True after finished.
        """
        push_connection(self.connection)
        started_job_registry = queue.started_job_registry
        self.log.debug('Started Job Registry set.')

        try:
            remove_from_intermediate_queue = len(self.queues) == 1
            self.prepare_job_execution(job, remove_from_intermediate_queue)

            job.started_at = utils.utcnow()
            timeout = job.timeout or self.queue_class.DEFAULT_TIMEOUT
            with self.death_penalty_class(timeout, JobTimeoutException, job_id=job.id):
                self.log.debug('Performing Job...')
                rv = job.perform()
                self.log.debug('Finished performing Job ID %s', job.id)

            job.ended_at = utils.utcnow()

            # Pickle the result in the same try-except block since we need
            # to use the same exc handling when pickling fails
            job._result = rv

            job.heartbeat(utils.utcnow(), job.success_callback_timeout)
            job.execute_success_callback(self.death_penalty_class, rv)

            self.handle_job_success(job=job, queue=queue, started_job_registry=started_job_registry)
        except:  # NOQA
            self.log.debug('Job %s raised an exception.', job.id)
            job.ended_at = utils.utcnow()
            exc_info = sys.exc_info()
            exc_string = ''.join(traceback.format_exception(*exc_info))

            try:
                job.heartbeat(utils.utcnow(), job.failure_callback_timeout)
                job.execute_failure_callback(self.death_penalty_class, *exc_info)
            except:  # noqa
                exc_info = sys.exc_info()
                exc_string = ''.join(traceback.format_exception(*exc_info))

            self.handle_job_failure(
                job=job, exc_string=exc_string, queue=queue, started_job_registry=started_job_registry
            )
            self.handle_exception(job, *exc_info)
            return False

        finally:
            pop_connection()

        self.log.info('%s: %s (%s)', green(job.origin), blue('Job OK'), job.id)
        if rv is not None:
            self.log.debug('Result: %r', yellow(utils.as_text(str(rv))))

        if self.log_result_lifespan:
            result_ttl = job.get_result_ttl(self.default_result_ttl)
            if result_ttl == 0:
                self.log.info('Result discarded immediately')
            elif result_ttl > 0:
                self.log.info('Result is kept for %s seconds', result_ttl)
            else:
                self.log.info('Result will never expire, clean up result key manually')

        return True

    def execute_job(self, job: 'Job', queue: 'Queue'):
        """Execute job in same thread/process, do not fork()"""
        self.set_state(WorkerStatus.BUSY)
        self.perform_job(job, queue)
        self.set_state(WorkerStatus.IDLE)

    def work(
        self,
        burst: bool = False,
        logging_level: str = "INFO",
        date_format: str = DEFAULT_LOGGING_DATE_FORMAT,
        log_format: str = DEFAULT_LOGGING_FORMAT,
        max_jobs: Optional[int] = None,
        max_idle_time: Optional[int] = None,
        with_scheduler: bool = False,
        dequeue_strategy: DequeueStrategy = DequeueStrategy.DEFAULT,
    ) -> bool:
        """Starts the work loop.

        Pops and performs all jobs on the current list of queues.  When all
        queues are empty, block and wait for new jobs to arrive on any of the
        queues, unless `burst` mode is enabled.
        If `max_idle_time` is provided, worker will die when it's idle for more than the provided value.

        The return value indicates whether any jobs were processed.

        Args:
            burst (bool, optional): Whether to work on burst mode. Defaults to False.
            logging_level (str, optional): Logging level to use. Defaults to "INFO".
            date_format (str, optional): Date Format. Defaults to DEFAULT_LOGGING_DATE_FORMAT.
            log_format (str, optional): Log Format. Defaults to DEFAULT_LOGGING_FORMAT.
            max_jobs (Optional[int], optional): Max number of jobs. Defaults to None.
            max_idle_time (Optional[int], optional): Max seconds for worker to be idle. Defaults to None.
            with_scheduler (bool, optional): Whether to run the scheduler in a separate process. Defaults to False.
            dequeue_strategy (DequeueStrategy, optional): Which strategy to use to dequeue jobs.
                Defaults to DequeueStrategy.DEFAULT

        Returns:
            worked (bool): Will return True if any job was processed, False otherwise.
        """
        self.bootstrap(logging_level, date_format, log_format)
        self._dequeue_strategy = dequeue_strategy
        completed_jobs = 0
        if with_scheduler:
            self._start_scheduler(burst, logging_level, date_format, log_format)

        self._install_signal_handlers()
        try:
            while True:
                try:
                    self.check_for_suspension(burst)

                    if self.should_run_maintenance_tasks:
                        self.run_maintenance_tasks()

                    if self._stop_requested:
                        self.log.info('Worker %s: stopping on request', self.key)
                        break

                    timeout = None if burst else self.dequeue_timeout
                    result = self.dequeue_job_and_maintain_ttl(timeout, max_idle_time)
                    if result is None:
                        if burst:
                            self.log.info('Worker %s: done, quitting', self.key)
                        elif max_idle_time is not None:
                            self.log.info('Worker %s: idle for %d seconds, quitting', self.key, max_idle_time)
                        break

                    job, queue = result
                    self.execute_job(job, queue)
                    self.heartbeat()

                    completed_jobs += 1
                    if max_jobs is not None:
                        if completed_jobs >= max_jobs:
                            self.log.info('Worker %s: finished executing %d jobs, quitting', self.key, completed_jobs)
                            break

                except redis.exceptions.TimeoutError:
                    self.log.error('Worker %s: Redis connection timeout, quitting...', self.key)
                    break

                except StopRequested:
                    break

                except SystemExit:
                    # Cold shutdown detected
                    raise

                except:  # noqa
                    self.log.error('Worker %s: found an unhandled exception, quitting...', self.key, exc_info=True)
                    break
        finally:
            self.teardown()
        return bool(completed_jobs)

    def check_for_suspension(self, burst: bool):
        """Check to see if workers have been suspended by `rq suspend`"""
        before_state = None
        notified = False

        while not self._stop_requested and is_suspended(self.connection, self):
            if burst:
                self.log.info('Suspended in burst mode, exiting')
                self.log.info('Note: There could still be unfinished jobs on the queue')
                raise StopRequested

            if not notified:
                self.log.info('Worker suspended, run `rq resume` to resume')
                before_state = self.get_state()
                self.set_state(WorkerStatus.SUSPENDED)
                notified = True
            time.sleep(1)

        if before_state:
            self.set_state(before_state)

    # Shutdown & Cleanup

    def set_shutdown_requested_date(self):
        """Sets the date on which the worker received a (warm) shutdown request"""
        self.connection.hset(self.key, 'shutdown_requested_date', utils.utcformat(self._shutdown_requested_date))

    def teardown(self):
        if self.scheduler:
            self.stop_scheduler()
        self.register_death()
        self.unsubscribe()

    def stop_scheduler(self):
        """Ensure scheduler process is stopped
        Will send the kill signal to scheduler process,
        if there's an OSError, just passes and `join()`'s the scheduler process,
        waiting for the process to finish.
        """
        if self.scheduler._process and self.scheduler._process.pid:
            try:
                os.kill(self.scheduler._process.pid, signal.SIGTERM)
            except OSError:
                pass
            self.scheduler._process.join()

    def pop_exc_handler(self):
        """Pops the latest exception handler off of the exc handler stack."""
        return self._exc_handlers.pop()

    def request_stop(self, signum, frame):
        """Stops the current worker loop but waits for child processes to
        end gracefully (warm shutdown).

        Args:
            signum (Any): Signum
            frame (Any): Frame
        """
        self.log.debug('Got signal %s', utils.signal_name(signum))
        self._shutdown_requested_date = utils.utcnow()

        signal.signal(signal.SIGINT, self.request_force_stop)
        signal.signal(signal.SIGTERM, self.request_force_stop)

        self.handle_warm_shutdown_request()
        self._shutdown()
    
    def request_force_stop(self, signum: int, frame: Optional[FrameType]):
        """Terminates the application (cold shutdown).

        Args:
            signum (Any): Signum
            frame (Any): Frame

        Raises:
            SystemExit: SystemExit
        """
        if (utils.utcnow() - self._shutdown_requested_date) < timedelta(seconds=1):  # type: ignore
            self.log.debug('Shutdown signal ignored, received twice in less than 1 second')
            return

        self.log.warning('Cold shut down')
        self.log.debug('Taking down worker %s with me', self.pid)
        raise SystemExit()

    def _shutdown(self):
        """
        If shutdown is requested in the middle of a job, wait until
        finish before shutting down and save the request in redis
        """
        if self.get_state() == WorkerStatus.BUSY:
            self._stop_requested = True
            self.set_shutdown_requested_date()
            self.log.debug('Stopping after current horse is finished. Press Ctrl+C again for a cold shutdown.')
            if self.scheduler:
                self.stop_scheduler()
        else:
            if self.scheduler:
                self.stop_scheduler()
            raise StopRequested()

    # Handlers

    def handle_warm_shutdown_request(self):
        self.log.info('Worker %s [PID %d]: warm shut down requested', self.name, self.pid)

    def handle_job_failure(self, job: 'Job', queue: 'Queue', started_job_registry=None, exc_string=''):
        """
        Handles the failure or an executing job by:
            1. Setting the job status to failed
            2. Removing the job from StartedJobRegistry
            3. Setting the workers current job to None
            4. Add the job to FailedJobRegistry
        `save_exc_to_job` should only be used for testing purposes
        """
        self.log.debug('Handling failed execution of job %s', job.id)
        with self.connection.pipeline() as pipeline:
            if started_job_registry is None:
                started_job_registry = StartedJobRegistry(
                    job.origin, self.connection, job_class=self.job_class, serializer=self.serializer
                )

            # check whether a job was stopped intentionally and set the job
            # status appropriately if it was this job.
            job_is_stopped = self._stopped_job_id == job.id
            retry = job.retries_left and job.retries_left > 0 and not job_is_stopped

            if job_is_stopped:
                job.set_status(JobStatus.STOPPED, pipeline=pipeline)
                self._stopped_job_id = None
            else:
                # Requeue/reschedule if retry is configured, otherwise
                if not retry:
                    job.set_status(JobStatus.FAILED, pipeline=pipeline)

            started_job_registry.remove(job, pipeline=pipeline)

            if not self.disable_default_exception_handler and not retry:
                job._handle_failure(exc_string, pipeline=pipeline)
                with suppress(redis.exceptions.ConnectionError):
                    pipeline.execute()

            self.set_current_job_id(None, pipeline=pipeline)
            self.increment_failed_job_count(pipeline)
            if job.started_at and job.ended_at:
                self.increment_total_working_time(job.ended_at - job.started_at, pipeline)

            if retry:
                job.retry(queue, pipeline)
                enqueue_dependents = False
            else:
                enqueue_dependents = True

            try:
                pipeline.execute()
                if enqueue_dependents:
                    queue.enqueue_dependents(job)
            except Exception:
                # Ensure that custom exception handlers are called
                # even if Redis is down
                pass

    def handle_job_success(self, job: 'Job', queue: 'Queue', started_job_registry: StartedJobRegistry):
        """Handles the successful execution of certain job.
        It will remove the job from the `StartedJobRegistry`, adding it to the `SuccessfulJobRegistry`,
        and run a few maintenance tasks including:
            - Resting the current job ID
            - Enqueue dependents
            - Incrementing the job count and working time
            - Handling of the job successful execution

        Runs within a loop with the `watch` method so that protects interactions
        with dependents keys.

        Args:
            job (Job): The job that was successful.
            queue (Queue): The queue
            started_job_registry (StartedJobRegistry): The started registry
        """
        self.log.debug('Handling successful execution of job %s', job.id)

        with self.connection.pipeline() as pipeline:
            while True:
                try:
                    # if dependencies are inserted after enqueue_dependents
                    # a WatchError is thrown by execute()
                    pipeline.watch(job.dependents_key)
                    # enqueue_dependents might call multi() on the pipeline
                    queue.enqueue_dependents(job, pipeline=pipeline)

                    if not pipeline.explicit_transaction:
                        # enqueue_dependents didn't call multi after all!
                        # We have to do it ourselves to make sure everything runs in a transaction
                        pipeline.multi()

                    self.set_current_job_id(None, pipeline=pipeline)
                    self.increment_successful_job_count(pipeline=pipeline)
                    self.increment_total_working_time(job.ended_at - job.started_at, pipeline)  # type: ignore

                    result_ttl = job.get_result_ttl(self.default_result_ttl)
                    if result_ttl != 0:
                        self.log.debug('Saving job %s\'s successful execution result', job.id)
                        job._handle_success(result_ttl, pipeline=pipeline)

                    job.cleanup(result_ttl, pipeline=pipeline, remove_from_queue=False)
                    self.log.debug('Removing job %s from StartedJobRegistry', job.id)
                    started_job_registry.remove(job, pipeline=pipeline)

                    pipeline.execute()
                    self.log.debug('Finished handling successful execution of job %s', job.id)
                    break
                except redis.exceptions.WatchError:
                    continue

    def handle_exception(self, job: 'Job', *exc_info):
        """Walks the exception handler stack to delegate exception handling.
        If the job cannot be deserialized, it will raise when func_name or
        the other properties are accessed, which will stop exceptions from
        being properly logged, so we guard against it here.
        """
        self.log.debug('Handling exception for %s.', job.id)
        exc_string = ''.join(traceback.format_exception(*exc_info))
        try:
            extra = {
                'func': job.func_name,
                'arguments': job.args,
                'kwargs': job.kwargs,
            }
            func_name = job.func_name
        except DeserializationError:
            extra = {}
            func_name = '<DeserializationError>'

        # the properties below should be safe however
        extra.update({'queue': job.origin, 'job_id': job.id})

        # func_name
        self.log.error(
            '[Job %s]: exception raised while executing (%s)\n%s', job.id, func_name, exc_string, extra=extra
        )

        for handler in self._exc_handlers:
            self.log.debug('Invoking exception handler %s', handler)
            fallthrough = handler(job, *exc_info)

            # Only handlers with explicit return values should disable further
            # exc handling, so interpret a None return value as True.
            if fallthrough is None:
                fallthrough = True

            if not fallthrough:
                break

    def increment_failed_job_count(self, pipeline: Optional['Pipeline'] = None):
        """Used to keep the worker stats up to date in Redis.
        Increments the failed job count.

        Args:
            pipeline (Optional[Pipeline], optional): A Redis Pipeline. Defaults to None.
        """
        connection = pipeline if pipeline is not None else self.connection
        connection.hincrby(self.key, 'failed_job_count', 1)

    def increment_successful_job_count(self, pipeline: Optional['Pipeline'] = None):
        """Used to keep the worker stats up to date in Redis.
        Increments the successful job count.

        Args:
            pipeline (Optional[Pipeline], optional): A Redis Pipeline. Defaults to None.
        """
        connection = pipeline if pipeline is not None else self.connection
        connection.hincrby(self.key, 'successful_job_count', 1)

    def increment_total_working_time(self, job_execution_time: timedelta, pipeline: 'Pipeline'):
        """Used to keep the worker stats up to date in Redis.
        Increments the time the worker has been workig for (in seconds).

        Args:
            job_execution_time (timedelta): A timedelta object.
            pipeline (Optional[Pipeline], optional): A Redis Pipeline. Defaults to None.
        """
        pipeline.hincrbyfloat(self.key, 'total_working_time', job_execution_time.total_seconds())

    # Class & Dunder

    @staticmethod
    def clean_intermediate_queue(worker: 'Worker', queue: Queue) -> None:
        """
        Check whether there are any jobs stuck in the intermediate queue.

        A job may be stuck in the intermediate queue if a worker has successfully dequeued a job
        but was not able to push it to the StartedJobRegistry. This may happen in rare cases
        of hardware or network failure.

        We consider a job to be stuck in the intermediate queue if it doesn't exist in the StartedJobRegistry.
        """
        job_ids = [utils.as_text(job_id) for job_id in queue.connection.lrange(queue.intermediate_queue_key, 0, -1)]
        for job_id in job_ids:
            if job_id not in queue.started_job_registry:
                job = queue.fetch_job(job_id)
                if job:
                    worker.handle_job_failure(job, queue, exc_string='Job was stuck in intermediate queue.')
                queue.connection.lrem(queue.intermediate_queue_key, 1, job_id)

    @staticmethod
    def register(worker: 'Worker', pipeline: Optional['Pipeline'] = None):
        """
        Store worker key in Redis so we can easily discover active workers.

        Args:
            worker (Worker): The Worker
            pipeline (Optional[Pipeline], optional): The Redis Pipeline. Defaults to None.
        """
        connection = pipeline if pipeline is not None else worker.connection
        connection.sadd(worker.redis_workers_keys, worker.key)
        for name in worker.queue_names():
            redis_key = WORKERS_BY_QUEUE_KEY % name
            connection.sadd(redis_key, worker.key)

    @staticmethod
    def unregister(worker: 'Worker', pipeline: Optional['Pipeline'] = None):
        """Remove Worker key from Redis

        Args:
            worker (Worker): The Worker
            pipeline (Optional[Pipeline], optional): Redis Pipeline. Defaults to None.
        """
        if pipeline is None:
            connection = worker.connection.pipeline()
        else:
            connection = pipeline

        connection.srem(worker.redis_workers_keys, worker.key)
        for name in worker.queue_names():
            redis_key = WORKERS_BY_QUEUE_KEY % name
            connection.srem(redis_key, worker.key)

        if pipeline is None:
            connection.execute()

    @staticmethod
    def get_keys(queue: Optional['Queue'] = None, connection: Optional['Redis'] = None) -> Set[str]:
        """Returns a list of worker keys for a given queue.

        Args:
            queue (Optional[&#39;Queue&#39;], optional): The Queue. Defaults to None.
            connection (Optional[&#39;Redis&#39;], optional): The Redis Connection. Defaults to None.

        Raises:
            ValueError: If no Queue or Connection is provided.

        Returns:
            set: A Set with keys.
        """
        if queue is None and connection is None:
            raise ValueError('"Queue" or "connection" argument is required')

        if queue:
            redis = queue.connection
            redis_key = WORKERS_BY_QUEUE_KEY % queue.name
        else:
            redis = connection  # type: ignore
            redis_key = REDIS_WORKER_KEYS

        return {utils.as_text(key) for key in redis.smembers(redis_key)}

    @staticmethod
    def clean_worker_registry(queue: 'Queue'):
        """Delete invalid worker keys in registry.

        Args:
            queue (Queue): The Queue
        """
        keys = list(Worker.get_keys(queue))

        with queue.connection.pipeline() as pipeline:
            for key in keys:
                pipeline.exists(key)
            results = pipeline.execute()

            invalid_keys = []

            for i, key_exists in enumerate(results):
                if not key_exists:
                    invalid_keys.append(keys[i])

            if invalid_keys:
                for invalid_subset in utils.split_list(invalid_keys, MAX_KEYS):
                    pipeline.srem(WORKERS_BY_QUEUE_KEY % queue.name, *invalid_subset)
                    pipeline.srem(REDIS_WORKER_KEYS, *invalid_subset)
                    pipeline.execute()

    @classmethod
    def load_by_key(
        cls,
        worker_key: str,
        connection: Optional['Redis'] = None,
        job_class: Optional[Type['Job']] = None,
        queue_class: Optional[Type['Queue']] = None,
        serializer=None,
    ) -> Optional['Worker']:
        """Returns a Worker instance, based on the naming conventions for
        naming the internal Redis keys.  Can be used to reverse-lookup Workers
        by their Redis keys.

        Args:
            worker_key (str): The worker key
            connection (Optional[Redis], optional): Redis connection. Defaults to None.
            job_class (Optional[Type[Job]], optional): The job class if custom class is being used. Defaults to None.
            queue_class (Optional[Type[Queue]]): The queue class if a custom class is being used. Defaults to None.
            serializer (Any, optional): The serializer to use. Defaults to None.

        Raises:
            ValueError: If the key doesn't start with `rq:worker:`, the default worker namespace prefix.

        Returns:
            worker (Worker): The Worker instance.
        """
        prefix = cls.redis_worker_namespace_prefix
        if not worker_key.startswith(prefix):
            raise ValueError('Not a valid RQ worker key: %s' % worker_key)

        if connection is None:
            connection = get_current_connection()
        if not connection.exists(worker_key):
            connection.srem(cls.redis_workers_keys, worker_key)
            return None

        name = worker_key[len(prefix) :]
        worker = cls(
            [],
            name,
            connection=connection,
            job_class=job_class,
            queue_class=queue_class,
            prepare_for_work=False,
            serializer=serializer,
        )

        worker.refresh()
        return worker

    @classmethod
    def all(
        cls,
        connection: Optional['Redis'] = None,
        job_class: Optional[Type['Job']] = None,
        queue_class: Optional[Type['Queue']] = None,
        queue: Optional['Queue'] = None,
        serializer=None,
    ) -> List['ForkWorker']:
        """Returns an iterable of all Workers.

        Returns:
            workers (List[Worker]): A list of workers
        """
        warnings.warn(
            "V2 Deprecation Warning: The `all()` method for the Worker class is deprecated. Use the main RQ class to get all workers with `get_workers` instead.",
            DeprecationWarning,
        )

        if queue:
            connection = queue.connection
        elif connection is None:
            connection = get_current_connection()

        worker_keys = Worker.get_keys(queue=queue, connection=connection)
        workers = [
            cls.load_by_key(
                key, connection=connection, job_class=job_class, queue_class=queue_class, serializer=serializer
            )
            for key in worker_keys
        ]
        return utils.compact(workers)

    @classmethod
    def all_keys(cls, connection: Optional['Redis'] = None, queue: Optional['Queue'] = None) -> List[str]:
        """List of worker keys

        Args:
            connection (Optional[Redis], optional): A Redis Connection. Defaults to None.
            queue (Optional[Queue], optional): The Queue. Defaults to None.

        Returns:
            list_keys (List[str]): A list of worker keys
        """
        warnings.warn(
            "V2 Deprecation Warning: The `all_keys()` method for the Worker class is deprecated. Use the main RQ class to query for all workers' keys with `get_workers_keys` instead.",
            DeprecationWarning,
        )
        return [utils.as_text(key) for key in Worker.get_keys(queue=queue, connection=connection)]

    @classmethod
    def count(cls, connection: Optional['Redis'] = None, queue: Optional['Queue'] = None) -> int:
        """Returns the number of workers by queue or connection.

        Args:
            connection (Optional[Redis], optional): Redis connection. Defaults to None.
            queue (Optional[Queue], optional): The queue to use. Defaults to None.

        Returns:
            length (int): The queue length.
        """
        warnings.warn(
            "V2 Deprecation Warning: The `count()` method for the Worker class is deprecated. Use the main RQ class to count workers with `get_workers_count` instead.",
            DeprecationWarning,
        )
        return len(Worker.get_keys(queue=queue, connection=connection))

    def __eq__(self, other):
        """Equality does not take the database/connection into account"""
        if not isinstance(other, self.__class__):
            raise TypeError('Cannot compare workers to other types (of workers)')
        return self.name == other.name

    def __hash__(self):
        """The hash does not take the database/connection into account"""
        return hash(self.name)


class ForkWorker(Worker):
    def __init__(self, queues, name: str | None = None, default_result_ttl=DEFAULT_RESULT_TTL, connection: Redis | None = None, exc_handler=None, exception_handlers=None, default_worker_ttl=DEFAULT_WORKER_TTL, maintenance_interval: int = DEFAULT_MAINTENANCE_TASK_INTERVAL, job_class: type[Job] | None = None, queue_class: type[Queue] | None = None, log_job_description: bool = True, job_monitoring_interval=DEFAULT_JOB_MONITORING_INTERVAL, disable_default_exception_handler: bool = False, prepare_for_work: bool = True, serializer=None, work_horse_killed_handler: Callable[[Job, int, int, 'struct_rusage'], None] | None = None):
        super().__init__(queues, name, default_result_ttl, connection, exc_handler, exception_handlers, default_worker_ttl, maintenance_interval, job_class, queue_class, log_job_description, job_monitoring_interval, disable_default_exception_handler, prepare_for_work, serializer)
        self._is_horse: bool = False
        self._horse_pid: int = 0
        self._work_horse_killed_handler = work_horse_killed_handler

    @property
    def horse_pid(self):
        """The horse's process ID.  Only available in the worker.  Will return
        0 in the horse part of the fork.
        """
        return self._horse_pid

    @property
    def is_horse(self):
        """Returns whether or not this is the worker or the work horse."""
        return self._is_horse

    def setup_work_horse_signals(self):
        """Setup signal handing for the newly spawned work horse

        Always ignore Ctrl+C in the work horse, as it might abort the
        currently running job.

        The main worker catches the Ctrl+C and requests graceful shutdown
        after the current work is done.  When cold shutdown is requested, it
        kills the current job anyway.
        """
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        signal.signal(signal.SIGTERM, signal.SIG_DFL)

    # Work

    def wait_for_horse(self) -> Tuple[Optional[int], Optional[int], Optional['struct_rusage']]:
        """Waits for the horse process to complete.
        Uses `0` as argument as to include "any child in the process group of the current process".
        """
        pid = stat = rusage = None
        with contextlib.suppress(ChildProcessError):  # ChildProcessError: [Errno 10] No child processes
            pid, stat, rusage = os.wait4(self.horse_pid, 0)
        return pid, stat, rusage

    def main_work_horse(self, job: 'Job', queue: 'Queue'):
        """This is the entry point of the newly spawned work horse.
        After fork()'ing, always assure we are generating random sequences
        that are different from the worker.

        os._exit() is the way to exit from childs after a fork(), in
        contrast to the regular sys.exit()
        """
        random.seed()
        self.setup_work_horse_signals()
        self._is_horse = True
        self.log = logger
        try:
            self.perform_job(job, queue)
        except:  # noqa
            os._exit(1)
        os._exit(0)

    def fork_work_horse(self, job: 'Job', queue: 'Queue'):
        """Spawns a work horse to perform the actual work and passes it a job.
        This is where the `fork()` actually happens.

        Args:
            job (Job): The Job that will be ran
            queue (Queue): The queue
        """
        child_pid = os.fork()
        os.environ['RQ_WORKER_ID'] = self.name
        os.environ['RQ_JOB_ID'] = job.id
        if child_pid == 0:
            os.setsid()
            self.main_work_horse(job, queue)
            os._exit(0)  # just in case
        else:
            self._horse_pid = child_pid
            self.procline('Forked {0} at {1}'.format(child_pid, time.time()))

    def execute_job(self, job: 'Job', queue: 'Queue'):
        """Spawns a work horse to perform the actual work and passes it a job.
        The worker will wait for the work horse and make sure it executes
        within the given timeout bounds, or will end the work horse with
        SIGALRM.
        """
        self.set_state(WorkerStatus.BUSY)
        self.fork_work_horse(job, queue)
        self.monitor_work_horse(job, queue)
        self.set_state(WorkerStatus.IDLE)

    def monitor_work_horse(self, job: 'Job', queue: 'Queue'):
        """The worker will monitor the work horse and make sure that it
        either executes successfully or the status of the job is set to
        failed

        Args:
            job (Job): _description_
            queue (Queue): _description_
        """
        retpid = ret_val = rusage = None
        job.started_at = utils.utcnow()
        while True:
            try:
                with self.death_penalty_class(self.job_monitoring_interval, HorseMonitorTimeoutException):
                    retpid, ret_val, rusage = self.wait_for_horse()
                break
            except HorseMonitorTimeoutException:
                # Horse has not exited yet and is still running.
                # Send a heartbeat to keep the worker alive.
                self.set_current_job_working_time((utils.utcnow() - job.started_at).total_seconds())

                # Kill the job from this side if something is really wrong (interpreter lock/etc).
                if job.timeout != -1 and self.current_job_working_time > (job.timeout + 60):  # type: ignore
                    self.heartbeat(self.job_monitoring_interval + 60)
                    self.kill_horse()
                    self.wait_for_horse()
                    break

                self.maintain_heartbeats(job)

            except OSError as e:
                # In case we encountered an OSError due to EINTR (which is
                # caused by a SIGINT or SIGTERM signal during
                # os.waitpid()), we simply ignore it and enter the next
                # iteration of the loop, waiting for the child to end.  In
                # any other case, this is some other unexpected OS error,
                # which we don't want to catch, so we re-raise those ones.
                if e.errno != errno.EINTR:
                    raise
                # Send a heartbeat to keep the worker alive.
                self.heartbeat()

        self.set_current_job_working_time(0)
        self._horse_pid = 0  # Set horse PID to 0, horse has finished working
        if ret_val == os.EX_OK:  # The process exited normally.
            return

        job_status = job.get_status()

        if job_status is None:  # Job completed and its ttl has expired
            return
        elif self._stopped_job_id == job.id:
            # Work-horse killed deliberately
            self.log.warning('Job stopped by user, moving job to FailedJobRegistry')
            if job.stopped_callback:
                job.execute_stopped_callback(self.death_penalty_class)
            self.handle_job_failure(job, queue=queue, exc_string='Job stopped by user, work-horse terminated.')
        elif job_status not in [JobStatus.FINISHED, JobStatus.FAILED]:
            if not job.ended_at:
                job.ended_at = utils.utcnow()

            # Unhandled failure: move the job to the failed queue
            signal_msg = f" (signal {os.WTERMSIG(ret_val)})" if ret_val and os.WIFSIGNALED(ret_val) else ''
            exc_string = f"Work-horse terminated unexpectedly; waitpid returned {ret_val}{signal_msg}; "
            self.log.warning('Moving job to FailedJobRegistry (%s)', exc_string)

            self.handle_work_horse_killed(job, retpid, ret_val, rusage)
            self.handle_job_failure(job, queue=queue, exc_string=exc_string)

    # Cleanups

    def request_force_stop(self, signum: int, frame: Optional[FrameType]):
        """Terminates the application (cold shutdown).

        Args:
            signum (Any): Signum
            frame (Any): Frame

        Raises:
            SystemExit: SystemExit
        """
        # When worker is run through a worker pool, it may receive duplicate signals
        # One is sent by the pool when it calls `pool.stop_worker()` and another is sent by the OS
        # when user hits Ctrl+C. In this case if we receive the second signal within 1 second,
        # we ignore it.
        if (utils.utcnow() - self._shutdown_requested_date) < timedelta(seconds=1):  # type: ignore
            self.log.debug('Shutdown signal ignored, received twice in less than 1 second')
            return

        self.log.warning('Cold shut down')

        # Take down the horse with the worker
        if self.horse_pid:
            self.log.debug('Taking down horse %s with me', self.horse_pid)
            self.kill_horse()
            self.wait_for_horse()
        raise SystemExit()

    def kill_horse(self, sig: signal.Signals = SIGKILL):
        """Kill the horse but catch "No such process" error has the horse could already be dead.

        Args:
            sig (signal.Signals, optional): _description_. Defaults to SIGKILL.
        """
        try:
            os.killpg(os.getpgid(self.horse_pid), sig)
            self.log.info('Killed horse pid %s', self.horse_pid)
        except OSError as e:
            if e.errno == errno.ESRCH:
                # "No such process" is fine with us
                self.log.debug('Horse already dead')
            else:
                raise

    def handle_work_horse_killed(self, job, retpid, ret_val, rusage):
        if self._work_horse_killed_handler is None:
            return

        self._work_horse_killed_handler(job, retpid, ret_val, rusage)

    def teardown(self):
        if not self.is_horse:
            if self.scheduler:
                self.stop_scheduler()
            self.register_death()
            self.unsubscribe()


class ThreadPoolWorker(Worker):
    death_penalty_class = TimerDeathPenalty

    def __init__(self, *args, **kwargs):
        self.threadpool_size = kwargs.pop('pool_size', self.default_pool_size)
        self.executor = ThreadPoolExecutor(max_workers=self.threadpool_size, thread_name_prefix="rq_workers_")
        self._idle_threads = self.threadpool_size
        self._lock = threading.Lock()
        self._current_jobs: List[Tuple['Job', 'Future']] = []  # type: ignore
        super(ThreadPoolWorker, self).__init__(*args, **kwargs)
    
    @property
    def total_horse_count(self) -> int:
        """Returns the number of availble horses.
        In the `SimpleWorker` and `Worker` this will always be 1.

        Returns:
            horse_count (int): The number of horses
        """
        return self.threadpool_size

    @property
    def idle_horses_count(self):
        """Checks whether the thread pool is full.
        Returns True if there are no idle threads, False otherwise

        Returns:
            is_full (bool): True if full, False otherwise.
        """
        return self.threadpool_size - self._idle_threads

    @property
    def is_pool_full(self):
        """Checks whether the thread pool is full.
        Returns True if there are no idle threads, False otherwise

        Returns:
            is_full (bool): True if full, False otherwise.
        """
        if self._idle_threads == 0:
            return True
        return False

    @property
    def default_pool_size(self) -> int:
        """THe default TheadPool size.
        By default, each CPU core should run N Threads,
        where N is the `DEFAULT_CPU_THREADS``

        Returns:
            cpus (int): Number of CPUs
        """
        from multiprocessing import cpu_count
        return cpu_count() * DEFAULT_CPU_THREADS

    def work(
        self,
        burst: bool = False,
        logging_level: str = "INFO",
        date_format: str = DEFAULT_LOGGING_DATE_FORMAT,
        log_format: str = DEFAULT_LOGGING_FORMAT,
        max_jobs: Optional[int] = None,
        max_idle_time: Optional[int] = None,
        with_scheduler: bool = False,
        dequeue_strategy: DequeueStrategy = DequeueStrategy.DEFAULT,
    ):
        """Starts the work loop.

        Pops jobs from the current list of queues, and submits each job to the ThreadPool.
        When all queues are empty, block and wait for new jobs to arrive on any of the
        queues, unless `burst` mode is enabled.

        The return value indicates whether any jobs were processed.
        """
        self.bootstrap(logging_level, date_format, log_format)
        completed_jobs = 0
        self.log.info("ThreadPoolWorker %s: started with %s threads, version %s", self.key, self.threadpool_size, VERSION)
        self.log.warning("*** WARNING: ThreadPoolWorker is in beta and unstable. DO NOT use it in production!")
        if with_scheduler:
            self._start_scheduler(burst, logging_level, date_format, log_format)

        self._install_signal_handlers()
        try:
            while True:
                try:
                    self.check_for_suspension(burst)

                    if self.should_run_maintenance_tasks:
                        self.run_maintenance_tasks()

                    if self._stop_requested:
                        self.log.info('Worker %s: stopping on request', self.key)
                        break

                    if self.is_pool_full:
                        self.log.debug('ThreadPool is full, waiting for idle threads...')
                        self._wait_for_slot()

                    timeout = None if burst else self.dequeue_timeout
                    result = self.dequeue_job_and_maintain_ttl(timeout)
                    if result is None:
                        if not burst:
                            break
                        has_pending_dependents = self._check_pending_dependents()
                        if has_pending_dependents:
                            continue
                        self.log.info("Worker %s: done, quitting", self.key)
                        break

                    job, queue = result
                    self.reorder_queues(reference_queue=queue)
                    self.execute_job(job, queue)
                    self.heartbeat()

                    completed_jobs += 1
                    if max_jobs is not None:
                        if completed_jobs < max_jobs:
                            continue
                        self.log.info("Worker %s: finished executing %d jobs, quitting", self.key, completed_jobs)
                        break

                except redis.exceptions.TimeoutError:
                    self.log.error(f"Worker {self.key}: Redis connection timeout, quitting...")
                    break

                except StopRequested:
                    break

                except SystemExit:
                    # Cold shutdown detected
                    raise

                except:  # noqa
                    self.log.error('Worker %s: found an unhandled exception, quitting...', self.key, exc_info=True)
                    break
        finally:
            self.register_death()
            self.unsubscribe()

        return bool(completed_jobs)

    def execute_job(self, job, queue):
        def job_done(future: Future):
            """Callback function that runs after the job (future) is finished.
            This will update the `idle_counter` object and update the `_current_jobs` array,
            removing the job that just finished from the list.

            Args:
                future (Future): The Future object.
            """
            self._change_idle_counter(+1)
            self.heartbeat()
            job_element = list(filter(lambda x: id(x[1]) == id(future), self._current_jobs))
            for el in job_element:
                self._current_jobs.remove(el)
            if job.get_status() == JobStatus.FINISHED:
                queue.enqueue_dependents(job)

        self.log.info("Executing job %s from %s", blue(job.id), green(queue.name))
        future = self.executor.submit(self.perform_job, job, queue)
        self._current_jobs.append((job, future))
        self._change_idle_counter(-1)
        future.add_done_callback(job_done)

    def wait_all(self, timeout: Optional[int] = None):
        """ Wait all current jobs """
        wait([future for _, future in self._current_jobs])

    def _change_idle_counter(self, operation: int):
        """Updates the idle threads counter using a lock to make it safe.

        Args:
            operation (int): +1 to increment, -1 to decrement.
        """
        with self._lock:
            self._idle_threads += operation

    def _wait_for_slot(self, wait_interval: float = 0.25):
        """Waits for a free slot in the thread pool.
        Sleeps for `wait_interval` seconds to avoid high CPU burden on long jobs.

        Args:
            wait_interval (float, 0.25): How long to wait between each check. Default to 0.25 second.
        """
        while 1:
            if not self.is_pool_full:
                self.log.debug('Found idle thread, ready to work')
                break
            time.sleep(wait_interval)
            continue

    def _check_pending_dependents(self) -> bool:
        """Checks whether any job that's current being executed in the pool has dependents.
        If there are dependents, appends it to a `pending_dependents` array.
        If this array has items (> 0), we know something that's currently running must enqueue dependents
        before we can actually stop a worker (on burst mode, for example).
        If there are dependents returns True, False otherwise.

        Returns:
            pending_dependents (bool): Whether any job currently running has dependents.
        """
        pending_dependents = []
        for job, _ in self._current_jobs:
            if not job.dependents_key:
                continue
            pending_dependents.append(job)
        if len(pending_dependents) > 0:
            return True
        return False

    def _shutdown(self):
        """
        If shutdown is requested in the middle of a job, wait until
        finish before shutting down and save the request in redis
        """
        if self.get_state() != WorkerStatus.BUSY:
            if self.scheduler:
                self.stop_scheduler()
            raise StopRequested()
        self._stop_requested = True
        self.set_shutdown_requested_date()
        self.log.debug('Stopping after current horse is finished. ' 'Press Ctrl+C again for a cold shutdown.')
        self.executor.shutdown()
        if self.scheduler:
            self.stop_scheduler()

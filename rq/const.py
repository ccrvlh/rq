from enum import Enum


class DequeueStrategy(str, Enum):
    DEFAULT = "default"
    ROUND_ROBIN = "round_robin"
    RANDOM = "random"


class WorkerStatus(str, Enum):
    UNKNOWN = 'unknown'
    STARTING = 'starting'
    STARTED = 'started'
    SUSPENDED = 'suspended'
    BUSY = 'busy'
    IDLE = 'idle'


class SchedulerStatus(str, Enum):
    STARTED = 'started'
    WORKING = 'working'
    STOPPED = 'stopped'


class JobStatus(str, Enum):
    """The Status of Job within its lifecycle at any given time."""

    QUEUED = 'queued'
    FINISHED = 'finished'
    FAILED = 'failed'
    STARTED = 'started'
    DEFERRED = 'deferred'
    SCHEDULED = 'scheduled'
    STOPPED = 'stopped'
    CANCELED = 'canceled'


class ResultType(Enum):
    SUCCESSFUL = 1
    FAILED = 2
    STOPPED = 3


class WorkerType(str, Enum):
    """The type of worker to use."""

    DEFAULT = 'default'
    FORK = 'fork'
    THREAD = 'thread'
    ASYNC = 'async'
    GEVENT = 'gevent'
    PROCESS = 'process'

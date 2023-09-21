
from enum import Enum


class DequeueStrategy(str, Enum):
    DEFAULT = "default"
    ROUND_ROBIN = "round_robin"
    RANDOM = "random"


class WorkerStatus(str, Enum):
    STARTED = 'started'
    SUSPENDED = 'suspended'
    BUSY = 'busy'
    IDLE = 'idle'



class SchedulerStatus(str, Enum):
    STARTED = 'started'
    WORKING = 'working'
    STOPPED = 'stopped'
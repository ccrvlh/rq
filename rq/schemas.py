from dataclasses import dataclass
from typing import Any

from redis import Redis

from rq.types import FunctionReferenceType
from rq.types import JobDependencyType


@dataclass
class CallableArgs:
    """Callable arguments."""

    f: FunctionReferenceType
    timeout: int
    description: str
    result_ttl: int
    ttl: int
    failure_ttl: int
    depends_on: list[JobDependencyType]
    job_id: str
    at_front: bool
    meta: dict[Any, Any]
    retry: bool
    on_success: FunctionReferenceType
    on_failure: FunctionReferenceType
    on_stopped: FunctionReferenceType
    pipeline: Redis
    args: tuple[Any]
    kwargs: dict[str, Any]

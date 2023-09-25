from typing import TYPE_CHECKING, Any, Callable, List, Union


if TYPE_CHECKING:
    from rq.queue import Queue
    from rq.job import Dependency
    from rq.job import Job
    from rq.worker import Worker


FunctionReferenceType = Union[str, Callable[..., Any]]
"""Custom type definition for what a `func` is in the context of a job.
A `func` can be a string with the function import path (eg.: `myfile.mymodule.myfunc`)
or a direct callable (function/method).
"""


JobDependencyType = Union['Dependency', 'Job', str, List[Union['Dependency', 'Job']]]
"""Custom type definition for a job dependencies.
A simple helper definition for the `depends_on` parameter when creating a job.
"""

QueueReferenceType = Union[str, 'Queue']
"""_summary_
"""

JobReferenceType = Union[Job, str]
"""_summary_
"""

WorkerReferenceType = Union[Worker, str]
"""_summary_"""

### RQ App Design

The main entrypoint for RQ.
The idea is that this would allow for a single point of contact for the enduser,
easier and simple to work on.

Example:

```python
# This creates a default queue automatically
rq = RQ(connection=Redis())

# You can also create multiple queues
rq = RQ(queues=['high', 'low'], connection=Redis())

# Or even use a queue instance
q = Queue('high')
rq = RQ(queue=q, connection=Redis())
```

As this is a centralized queue, more configuration options are available:

```python
# Setting namespaces makes every Redis key use `mynamespace:...`
rq = RQ(connection=Redis(), namespace="myapp")
```

With a single object, you can use multiple queues from a single place

```python
rq = RQ(queues=['high', 'low'], connection=Redis())

rq.send('high', 'myapp.module.priority_task')
rq.send('low', 'myapp.module.maintenance_task')
```

The well known API is also available, directly from `rq`:

```python
rq.enqueue(...)
rq.enqueue_at(...)
rq.enqueue_in(...)
rq.schedule(...)
```

It should also make it easier to manage resources:

```python
rq = RQ(conneciton=Redis())

# Get all workers
workers = rq.get_workers()

# Get all queues
queues = rq.get_queues()

# Filter jobs
queues = rq.get_jobs(status="failed")

# Or just query all of them
queues = rq.get_jobs()

# Or just query all of them
queues = rq.get_jobs()
```

And allows for more powerful programmatic control over the instance:

```python
# Get a worker
w = rq.get_worker("myworker")

# Kill a worker
rq.kill_worker("myworker")

# Set burst on an existing worker
rq.update_worker("myworker", burst=True)

# Reload it
rq.reload_worker("myworker")
rq.reload_workers(["oldworker", "otherworker"])

# Suspend & Resume
rq.suspend_worker("myworker")
rq.resume_worker("myworker")

# Empty a queue
rq.empty_queue("myqueue")

# Job-related actions are also available
rq.get_job("my-job-id")
rq.cancel_job("my-job-id")
rq.exclude_job("my-job-id")
rq.export_job("my-job-id")
```

Build customized maintenance tasks:

```python
# Requeue stale jobs

```

## RQ Interface First Draft

```python
from typing import List, Optional, Type, Union

from redis import Redis

from rq.job import Job
from rq.queue import Queue


class RQ:
    def __init__(
        self,
        queue: str,
        queues: List[str],
        namespace: str,
        connection: Redis,
        redis_url: str,
        job_id_prefix: str,
        worker_prefix: str,
        serializer: str,
        queue_class: str,
        job_class: str,
        worker_class: str,
        log_level: str,
        debug_mode: bool,
        job_timeout: str,
        result_ttl: str,
        failure_ttl: str,
        success_ttl: str,
        maintenance_interval: str,
        callback_timeout: str,
        scheduler_fallback_interval: str,
    ):
        pass

    # Sends

    def send(self):
        pass

    def send_job(self):
        pass

    def schedule(self):
        pass

    def schedule_job(self):
        pass

    def enqueue(self):
        pass

    def enqueue_at(self):
        pass

    def enqueue_in(self):
        pass

    # Queues

    def create_queue(self):
        pass

    def get_queue(self):
        pass

    def empty_queue(self):
        pass

    # Workers

    def get_worker(self):
        pass

    def get_workers(self):
        pass

    def kill_worker(self):
        pass

    def reload_worker(self):
        pass

    def start_worker(self):
        pass

    # Job

    def get_job(self):
        pass

    def get_job_status(self):
        pass

    def cancel_job(self):
        pass

    def exclude_job(self):
        pass

    def save_job(self):
        pass

    def create_job(self):
        pass
```

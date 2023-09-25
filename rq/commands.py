import json
import os
import signal

from typing import TYPE_CHECKING
from typing import Any
from typing import Dict

from rq.exceptions import InvalidJobOperation
from rq.job import Job

if TYPE_CHECKING:
    from redis import Redis
    from rq.worker import ForkWorker


PUBSUB_CHANNEL_TEMPLATE = 'rq:pubsub:%s'


def send_command(connection: 'Redis', worker_name: str, command: str, **kwargs):
    """
    Sends a command to a worker.
    A command is just a string, availble commands are:
        - `shutdown`: Shuts down a worker
        - `kill-horse`: Command for the worker to kill the current working horse
        - `stop-job`: A command for the worker to stop the currently running job

    The command string will be parsed into a dictionary and send to a PubSub Topic.
    Workers listen to the PubSub, and `handle` the specific command.

    Args:
        connection (Redis): A Redis Connection
        worker_name (str): The Job ID
    """
    payload = {'command': command}
    if kwargs:
        payload.update(kwargs)
    connection.publish(PUBSUB_CHANNEL_TEMPLATE % worker_name, json.dumps(payload))


def parse_payload(payload: Dict[Any, Any]) -> Dict[Any, Any]:
    """
    Returns a dict of command data

    Args:
        payload (dict): Parses the payload dict.
    """
    bytes_data = payload.get('data')
    if not isinstance(bytes_data, bytes):
        raise ValueError('Can only parse bytes data')

    string_data = bytes_data.decode()
    return json.loads(string_data)


def send_shutdown_command(connection: 'Redis', worker_name: str):
    """
    Sends a command to shutdown a worker.

    Args:
        connection (Redis): A Redis Connection
        worker_name (str): The Job ID
    """
    send_command(connection, worker_name, 'shutdown')


def send_kill_horse_command(connection: 'Redis', worker_name: str):
    """
    Tell worker to kill it's horse

    Args:
        connection (Redis): A Redis Connection
        worker_name (str): The Job ID
    """
    send_command(connection, worker_name, 'kill-horse')


def send_stop_job_command(connection: 'Redis', job_id: str, serializer=None):
    """
    Instruct a worker to stop a job

    Args:
        connection (Redis): A Redis Connection
        job_id (str): The Job ID
        serializer (): The serializer
    """
    job = Job.fetch(job_id, connection=connection, serializer=serializer)
    if not job.worker_name:
        raise InvalidJobOperation('Job is not currently executing')
    send_command(connection, job.worker_name, 'stop-job', job_id=job_id)


def handle_command(worker: 'ForkWorker', payload: Dict[Any, Any]):
    """Parses payload and routes commands to the worker.

    Args:
        worker (Worker): The worker to use
        payload (Dict[Any, Any]): The Payload
    """
    received_command = payload.get('command', None)
    if not received_command:
        worker.log.warning('Received empty command, ignoring.')
        return
    
    if received_command == 'stop-job':
        handle_stop_job_command(worker, payload)
    elif received_command == 'shutdown':
        handle_shutdown_command(worker)
    elif received_command == 'kill-horse':
        handle_kill_worker_command(worker, payload)
    else:
        worker.log.warning('Received unknown command %s, ignoring.', received_command)


def handle_shutdown_command(worker: 'ForkWorker'):
    """Perform shutdown command.

    Args:
        worker (Worker): The worker to use.
    """
    worker.log.info('Received shutdown command, sending SIGINT signal.')
    pid = os.getpid()
    os.kill(pid, signal.SIGINT)


def handle_kill_worker_command(worker: 'ForkWorker', payload: Dict[Any, Any]):
    """
    Stops work horse

    Args:
        worker (Worker): The worker to stop
        payload (Dict[Any, Any]): The payload.
    """

    worker.log.info('Received kill horse command.')
    if worker.horse_pid:
        worker.log.info('Kiling horse...')
        worker.kill_horse()
    else:
        worker.log.info('Worker is not working, kill horse command ignored')


def handle_stop_job_command(worker: 'ForkWorker', payload: Dict[Any, Any]):
    """Handles stop job command.

    Args:
        worker (Worker): The worker to use
        payload (Dict[Any, Any]): The payload.
    """
    job_id = payload.get('job_id')
    worker.log.debug('Received command to stop job %s', job_id)
    if job_id and worker.get_current_job_id() == job_id:
        # Sets the '_stopped_job_id' so that the job failure handler knows it
        # was intentional.
        worker._stopped_job_id = job_id
        worker.kill_horse()
    else:
        worker.log.info('Not working on job %s, command ignored.', job_id)

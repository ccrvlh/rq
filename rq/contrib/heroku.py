

from signal import signal
from rq.exceptions import ShutDownImminentException
from rq.utils import signal_name
from rq.worker import ForkWorker


class HerokuWorker(ForkWorker):
    """
    Modified version of rq worker which:
    * stops work horses getting killed with SIGTERM
    * sends SIGRTMIN to work horses on SIGTERM to the main process which in turn
    causes the horse to crash `imminent_shutdown_delay` seconds later
    """

    imminent_shutdown_delay = 6
    frame_properties = ['f_code', 'f_lasti', 'f_lineno', 'f_locals', 'f_trace']

    def setup_work_horse_signals(self):
        """Modified to ignore SIGINT and SIGTERM and only handle SIGRTMIN"""
        signal.signal(signal.SIGRTMIN, self.request_stop_sigrtmin)
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        signal.signal(signal.SIGTERM, signal.SIG_IGN)

    def handle_warm_shutdown_request(self):
        """If horse is alive send it SIGRTMIN"""
        if self.horse_pid != 0:
            self.log.info('Worker %s: warm shut down requested, sending horse SIGRTMIN signal', self.key)
            self.kill_horse(sig=signal.SIGRTMIN)
        else:
            self.log.warning('Warm shut down requested, no horse found')

    def request_stop_sigrtmin(self, signum, frame):
        if self.imminent_shutdown_delay == 0:
            self.log.warning('Imminent shutdown, raising ShutDownImminentException immediately')
            self.request_force_stop_sigrtmin(signum, frame)
        else:
            self.log.warning(
                'Imminent shutdown, raising ShutDownImminentException in %d seconds', self.imminent_shutdown_delay
            )
            signal.signal(signal.SIGRTMIN, self.request_force_stop_sigrtmin)
            signal.signal(signal.SIGALRM, self.request_force_stop_sigrtmin)
            signal.alarm(self.imminent_shutdown_delay)

    def request_force_stop_sigrtmin(self, signum, frame):
        info = dict((attr, getattr(frame, attr)) for attr in self.frame_properties)
        self.log.warning('raising ShutDownImminentException to cancel job...')
        raise ShutDownImminentException('shut down imminent (signal: %s)' % signal_name(signum), info)

import os
import shutil
import signal
import sys
import time
import pytest

from multiprocessing import Process
from unittest import mock, skipIf

from rq.contrib.heroku import HerokuWorker
from tests import RQTestCase
from tests.fixtures import (
    create_file_after_timeout_and_setsid,
    run_dummy_heroku_worker
)
from tests.test_worker import TimeoutTestCase



@pytest.mark.skipif(sys.platform == 'darwin', reason='requires Linux signals')
@skipIf('pypy' in sys.version.lower(), 'these tests often fail on pypy')
class HerokuWorkerShutdownTestCase(TimeoutTestCase, RQTestCase):
    def setUp(self):
        super().setUp()
        self.sandbox = '/tmp/rq_shutdown/'
        os.makedirs(self.sandbox)

    def tearDown(self):
        shutil.rmtree(self.sandbox, ignore_errors=True)

    @pytest.mark.slow
    def test_immediate_shutdown(self):
        """Heroku work horse shutdown with immediate (0 second) kill"""
        p = Process(target=run_dummy_heroku_worker, args=(self.sandbox, 0))
        p.start()
        time.sleep(0.5)

        os.kill(p.pid, signal.SIGRTMIN)

        p.join(2)
        self.assertEqual(p.exitcode, 1)
        self.assertTrue(os.path.exists(os.path.join(self.sandbox, 'started')))
        self.assertFalse(os.path.exists(os.path.join(self.sandbox, 'finished')))

    @pytest.mark.slow
    def test_1_sec_shutdown(self):
        """Heroku work horse shutdown with 1 second kill"""
        p = Process(target=run_dummy_heroku_worker, args=(self.sandbox, 1))
        p.start()
        time.sleep(0.5)

        os.kill(p.pid, signal.SIGRTMIN)
        time.sleep(0.1)
        self.assertEqual(p.exitcode, None)
        p.join(2)
        self.assertEqual(p.exitcode, 1)

        self.assertTrue(os.path.exists(os.path.join(self.sandbox, 'started')))
        self.assertFalse(os.path.exists(os.path.join(self.sandbox, 'finished')))

    @pytest.mark.slow
    def test_shutdown_double_sigrtmin(self):
        """Heroku work horse shutdown with long delay but SIGRTMIN sent twice"""
        p = Process(target=run_dummy_heroku_worker, args=(self.sandbox, 10))
        p.start()
        time.sleep(0.5)

        os.kill(p.pid, signal.SIGRTMIN)
        # we have to wait a short while otherwise the second signal wont bet processed.
        time.sleep(0.1)
        os.kill(p.pid, signal.SIGRTMIN)
        p.join(2)
        self.assertEqual(p.exitcode, 1)

        self.assertTrue(os.path.exists(os.path.join(self.sandbox, 'started')))
        self.assertFalse(os.path.exists(os.path.join(self.sandbox, 'finished')))

    @mock.patch('rq.worker.logger.info')
    def test_handle_shutdown_request(self, mock_logger_info):
        """Mutate HerokuWorker so _horse_pid refers to an artificial process
        and test handle_warm_shutdown_request"""
        w = HerokuWorker('foo')

        path = os.path.join(self.sandbox, 'shouldnt_exist')
        p = Process(target=create_file_after_timeout_and_setsid, args=(path, 2))
        p.start()
        self.assertEqual(p.exitcode, None)
        time.sleep(0.1)

        w._horse_pid = p.pid
        w.handle_warm_shutdown_request()
        p.join(2)
        # would expect p.exitcode to be -34
        self.assertEqual(p.exitcode, -34)
        self.assertFalse(os.path.exists(path))
        mock_logger_info.assert_called_with('Killed horse pid %s', p.pid)

    def test_handle_shutdown_request_no_horse(self):
        """Mutate HerokuWorker so _horse_pid refers to non existent process
        and test handle_warm_shutdown_request"""
        w = HerokuWorker('foo')

        w._horse_pid = 19999
        w.handle_warm_shutdown_request()


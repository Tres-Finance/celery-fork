from time import sleep

import pytest
from pytest_celery import RESULT_TIMEOUT, CeleryTestSetup, CeleryTestWorker, RabbitMQTestBroker

import celery
from celery import Celery
from celery.canvas import chain, group
from t.smoke.conftest import SuiteOperations, WorkerKill, WorkerRestart
from t.smoke.tasks import long_running_task


def assert_container_exited(worker: CeleryTestWorker, attempts: int = RESULT_TIMEOUT):
    """It might take a few moments for the container to exit after the worker is killed."""
    while attempts:
        worker.container.reload()
        if worker.container.status == "exited":
            break
        attempts -= 1
        sleep(1)

    worker.container.reload()
    assert worker.container.status == "exited"


class test_worker_shutdown(SuiteOperations):
    @pytest.fixture
    def default_worker_app(self, default_worker_app: Celery) -> Celery:
        app = default_worker_app
        app.conf.task_acks_late = True
        return app

    def test_warm_shutdown(self, celery_setup: CeleryTestSetup):
        queue = celery_setup.worker.worker_queue
        worker = celery_setup.worker
        sig = long_running_task.si(5, verbose=True).set(queue=queue)
        res = sig.delay()

        worker.wait_for_log("Starting long running task")
        self.kill_worker(worker, WorkerKill.Method.SIGTERM)
        worker.wait_for_log("worker: Warm shutdown (MainProcess)", timeout=5)
        worker.wait_for_log(f"long_running_task[{res.id}] succeeded", timeout=10)

        assert_container_exited(worker)
        assert res.get(RESULT_TIMEOUT)

    def test_cold_shutdown(self, celery_setup: CeleryTestSetup):
        queue = celery_setup.worker.worker_queue
        worker = celery_setup.worker
        sig = long_running_task.si(5, verbose=True).set(queue=queue)
        res = sig.delay()

        worker.wait_for_log("Starting long running task")
        self.kill_worker(worker, WorkerKill.Method.SIGQUIT)
        worker.wait_for_log("worker: Cold shutdown (MainProcess)", timeout=5)
        worker.assert_log_does_not_exist(f"long_running_task[{res.id}] succeeded", timeout=5)

        assert_container_exited(worker)

        with pytest.raises(celery.exceptions.TimeoutError):
            res.get(timeout=5)

    def test_hard_shutdown_from_warm(self, celery_setup: CeleryTestSetup):
        queue = celery_setup.worker.worker_queue
        worker = celery_setup.worker
        sig = long_running_task.si(420, verbose=True).set(queue=queue)
        sig.delay()

        worker.wait_for_log("Starting long running task")
        self.kill_worker(worker, WorkerKill.Method.SIGTERM)
        self.kill_worker(worker, WorkerKill.Method.SIGQUIT)
        self.kill_worker(worker, WorkerKill.Method.SIGQUIT)

        worker.wait_for_log("worker: Warm shutdown (MainProcess)", timeout=5)
        worker.wait_for_log("worker: Cold shutdown (MainProcess)", timeout=5)

        assert_container_exited(worker)

    def test_hard_shutdown_from_cold(self, celery_setup: CeleryTestSetup):
        queue = celery_setup.worker.worker_queue
        worker = celery_setup.worker
        sig = long_running_task.si(420, verbose=True).set(queue=queue)
        sig.delay()

        worker.wait_for_log("Starting long running task")
        self.kill_worker(worker, WorkerKill.Method.SIGQUIT)
        self.kill_worker(worker, WorkerKill.Method.SIGQUIT)

        worker.wait_for_log("worker: Cold shutdown (MainProcess)", timeout=5)

        assert_container_exited(worker)

    class test_REMAP_SIGTERM(SuiteOperations):
        @pytest.fixture
        def default_worker_env(self, default_worker_env: dict) -> dict:
            default_worker_env.update({"REMAP_SIGTERM": "SIGQUIT"})
            return default_worker_env

        def test_cold_shutdown(self, celery_setup: CeleryTestSetup):
            queue = celery_setup.worker.worker_queue
            worker = celery_setup.worker
            sig = long_running_task.si(5, verbose=True).set(queue=queue)
            res = sig.delay()

            worker.wait_for_log("Starting long running task")
            self.kill_worker(worker, WorkerKill.Method.SIGTERM)
            worker.wait_for_log("worker: Cold shutdown (MainProcess)", timeout=5)
            worker.assert_log_does_not_exist(f"long_running_task[{res.id}] succeeded", timeout=5)

            assert_container_exited(worker)

    class test_worker_soft_shutdown_timeout(SuiteOperations):
        @pytest.fixture
        def default_worker_app(self, default_worker_app: Celery) -> Celery:
            app = default_worker_app
            app.conf.worker_soft_shutdown_timeout = 10
            return app

        def test_soft_shutdown(self, celery_setup: CeleryTestSetup):
            app = celery_setup.app
            queue = celery_setup.worker.worker_queue
            worker = celery_setup.worker
            sig = long_running_task.si(5, verbose=True).set(queue=queue)
            res = sig.delay()

            worker.wait_for_log("Starting long running task")
            self.kill_worker(worker, WorkerKill.Method.SIGQUIT)
            worker.wait_for_log(
                f"Initiating Soft Shutdown, terminating in {app.conf.worker_soft_shutdown_timeout} seconds",
                timeout=5,
            )
            worker.wait_for_log(f"long_running_task[{res.id}] succeeded", timeout=10)
            worker.wait_for_log("worker: Cold shutdown (MainProcess)")

            assert_container_exited(worker)
            assert res.get(RESULT_TIMEOUT)

        def test_hard_shutdown_from_soft(self, celery_setup: CeleryTestSetup):
            queue = celery_setup.worker.worker_queue
            worker = celery_setup.worker
            sig = long_running_task.si(420, verbose=True).set(queue=queue)
            sig.delay()

            worker.wait_for_log("Starting long running task")
            self.kill_worker(worker, WorkerKill.Method.SIGQUIT)
            self.kill_worker(worker, WorkerKill.Method.SIGQUIT)
            worker.wait_for_log("Waiting gracefully for cold shutdown to complete...", timeout=5)
            worker.wait_for_log("worker: Cold shutdown (MainProcess)", timeout=5)
            self.kill_worker(worker, WorkerKill.Method.SIGQUIT)

            assert_container_exited(worker)

        class test_reset_visibility_timeout(SuiteOperations):
            @pytest.fixture
            def default_worker_app(self, default_worker_app: Celery) -> Celery:
                app = default_worker_app
                app.conf.prefetch_multiplier = 2
                app.conf.broker_transport_options = {
                    "visibility_timeout": 3600,  # 1 hour
                    "polling_interval": 1,
                }
                return app

            def test_soft_shutdown_reset_visibility_timeout(self, celery_setup: CeleryTestSetup):
                if isinstance(celery_setup.broker, RabbitMQTestBroker):
                    pytest.skip("RabbitMQ does not support visibility timeout")

                app = celery_setup.app
                queue = celery_setup.worker.worker_queue
                worker = celery_setup.worker
                sig = long_running_task.si(15, verbose=True).set(queue=queue)
                res = sig.delay()

                worker.wait_for_log("Starting long running task")
                self.kill_worker(worker, WorkerKill.Method.SIGQUIT)
                worker.wait_for_log(
                    f"Initiating Soft Shutdown, terminating in {app.conf.worker_soft_shutdown_timeout} seconds"
                )
                worker.wait_for_log("worker: Cold shutdown (MainProcess)")
                worker.wait_for_log("Restoring 1 unacknowledged message(s)")
                assert_container_exited(worker)
                worker.restart()
                assert res.get(RESULT_TIMEOUT)

            def test_soft_shutdown_reset_visibility_timeout_group_one_finish(self, celery_setup: CeleryTestSetup):
                if isinstance(celery_setup.broker, RabbitMQTestBroker):
                    pytest.skip("RabbitMQ does not support visibility timeout")

                app = celery_setup.app
                queue = celery_setup.worker.worker_queue
                worker = celery_setup.worker
                short_task = long_running_task.si(3, verbose=True).set(queue=queue)
                short_task_res = short_task.freeze()
                long_task = long_running_task.si(15, verbose=True).set(queue=queue)
                long_task_res = long_task.freeze()
                sig = group(short_task, long_task)
                sig.delay()

                worker.wait_for_log(f"long_running_task[{short_task_res.id}] received")
                worker.wait_for_log(f"long_running_task[{long_task_res.id}] received")
                self.kill_worker(worker, WorkerKill.Method.SIGQUIT)
                worker.wait_for_log(
                    f"Initiating Soft Shutdown, terminating in {app.conf.worker_soft_shutdown_timeout} seconds"
                )
                worker.wait_for_log(f"long_running_task[{short_task_res.id}] succeeded")
                worker.wait_for_log("worker: Cold shutdown (MainProcess)")
                worker.wait_for_log("Restoring 1 unacknowledged message(s)", timeout=5)
                assert_container_exited(worker)
                assert short_task_res.get(RESULT_TIMEOUT)

            def test_soft_shutdown_reset_visibility_timeout_group_none_finish(self, celery_setup: CeleryTestSetup):
                if isinstance(celery_setup.broker, RabbitMQTestBroker):
                    pytest.skip("RabbitMQ does not support visibility timeout")

                app = celery_setup.app
                queue = celery_setup.worker.worker_queue
                worker = celery_setup.worker
                short_task = long_running_task.si(15, verbose=True).set(queue=queue)
                short_task_res = short_task.freeze()
                long_task = long_running_task.si(15, verbose=True).set(queue=queue)
                long_task_res = long_task.freeze()
                sig = group(short_task, long_task)
                res = sig.delay()

                worker.wait_for_log(f"long_running_task[{short_task_res.id}] received")
                worker.wait_for_log(f"long_running_task[{long_task_res.id}] received")
                self.kill_worker(worker, WorkerKill.Method.SIGQUIT)
                worker.wait_for_log(
                    f"Initiating Soft Shutdown, terminating in {app.conf.worker_soft_shutdown_timeout} seconds"
                )
                worker.wait_for_log("worker: Cold shutdown (MainProcess)")
                worker.wait_for_log("Restoring 2 unacknowledged message(s)")
                assert_container_exited(worker)
                worker.restart()
                assert res.get(RESULT_TIMEOUT) == [True, True]
                assert short_task_res.get(RESULT_TIMEOUT)
                assert long_task_res.get(RESULT_TIMEOUT)

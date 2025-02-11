"""Runs local configs"""

import multiprocessing as mp
import queue
import threading
import time

from antz.infrastructure.config.base import Config
from antz.infrastructure.config.local_submitter import LocalSubmitterConfig
from antz.infrastructure.core.manager import run_manager


def run_local_submitter(
    config: LocalSubmitterConfig, start_job: Config
) -> threading.Thread:
    """Start the local submitter to accept jobs

    Args:
        config (LocalSubmitterConfig): user configuration of local submitter

    Returns:
        Callable[[PipelineConfig], None]: callable that accepts a pipeline config
            and places it on the queue
    """
    mp.set_start_method(
        "spawn"
    )  # we have significant threading, so complete isolation is required

    unified_task_queue: mp.Queue = mp.Queue()

    proc_ = LocalProcManager(
        task_queue=unified_task_queue, number_procs=config.num_concurrent_jobs
    )

    def submit_pipeline(config: Config) -> None:
        """Closure for the unified task queue"""
        return unified_task_queue.put(config)

    submit_pipeline(start_job)
    proc_.start()

    return proc_


class LocalProcManager(threading.Thread):
    """Holds the various local runners and issues them a kill command when done"""

    def __init__(self, task_queue: mp.Queue, number_procs: int) -> None:
        """Creates the local proc manager

        Args:
            task_queue (mp.Queue[PipelineConfig]): universal queue for job submission
            number_procs (int): number of parallel processes to start up
        """
        super().__init__()
        self.task_queue = task_queue
        self.number_procs = number_procs

    def run(self) -> None:
        """Run and issue kill command when nothing else to do and the jobs are complete"""

        children = [LocalProc(self.task_queue)]

        for child in children:
            child.start()

        while True:
            if self.task_queue.qsize() == 0 and all(
                not child.get_is_executing() for child in children
            ):
                for child in children:
                    child.set_dead(True)
                break
            time.sleep(1)  # only check every second

        for child in children:
            child.join()


class LocalProc(mp.Process):
    """Local proc is the node that actually runs the code"""

    def __init__(self, task_queue: mp.Queue) -> None:
        """Initialize the process with the universal job queue"""

        super().__init__()

        self._queue = task_queue
        self._is_executing = mp.Value("b")
        with self._is_executing.get_lock():
            self._is_executing.value = 0

        self._is_dead = mp.Value("b")
        with self._is_dead.get_lock():
            self._is_dead.value = 0

    def get_is_executing(self) -> bool:
        """Return if the current process is executing a pipeline"""
        with self._is_executing.get_lock():
            ret = self._is_executing.value
        return ret

    def set_dead(self, new_val) -> None:
        """Tell this process to kill itself"""
        with self._is_dead.get_lock():
            self._is_dead.value = new_val

    def run(self):
        """Infinitely loop waiting for a new job on the queue until the set_dead(True)"""

        def submit_fn(config: Config) -> None:
            """Submit a pipeline to this submitter"""
            self._queue.put(config)

        while not self._is_dead.value:
            try:
                next_config = self._queue.get(timeout=1)
                with self._is_executing.get_lock():
                    self._is_executing.value = True
                try:
                    run_manager(next_config, submit_fn=submit_fn)
                except Exception as _exc:  # pylint: disable=broad-exception-caught
                    pass

                with self._is_executing.get_lock():
                    self._is_executing.value = False
            except queue.Empty as _e:
                pass  # just waiting for another job
            time.sleep(0.5)  # only check every 1/2 second to reduce resource usage
        with self._is_executing.get_lock():
            self._is_executing = False

"""Runs local configs"""

import multiprocessing as mp
import queue
import threading
from typing import Callable

from antz.infrastructure.config.base import PipelineConfig
from antz.infrastructure.config.local_submitter import LocalSubmitterConfig
from antz.infrastructure.core.pipeline import run_pipeline


def run_submitter(config: LocalSubmitterConfig) -> Callable[[PipelineConfig], None]:
    """Start the local submitter to accept jobs

    Args:
        config (LocalSubmitterConfig): user configuration of local submitter

    Returns:
        Callable[[PipelineConfig], None]: callable that accepts a pipeline config
            and places it on the queue
    """

    unified_task_queue: mp.Queue = mp.Queue()

    LocalProcManager(
        task_queue=unified_task_queue, number_procs=config.num_concurrent_jobs
    ).start()

    def submit_pipeline(config: PipelineConfig) -> None:
        """Closure for the unified task queue"""
        return unified_task_queue.put(config)

    return submit_pipeline


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

        while True:
            if self.task_queue.qsize() == 0 and all(
                not child.get_is_executing() for child in children
            ):
                for child in children:
                    child.set_dead(True)
                break

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

        def submit_jobs(config: PipelineConfig) -> None:
            """Submit a pipeline to this submitter"""
            self._queue.put(config)

        while not self._is_dead.value:
            try:
                next_config = self._queue.get(timeout=1)
                with self._is_executing.lock():
                    self._is_executing.value = True
                try:
                    run_pipeline(next_config, submit_jobs)
                except Exception as _exc:  # pylint: disable=broad-exception-caught
                    pass

                with self._is_executing.lock():
                    self._is_executing.value = False
            except queue.Empty as _e:
                pass  # just waiting for another job

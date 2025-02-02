"""A job is the basic unit of execution in this module

Each job performs one user-assigned task and returns its state.
"""

from typing import Callable

from antz.infrastructure.config.base import JobConfig, PipelineConfig
from antz.infrastructure.core.status import Status


def run_job(
    config: JobConfig, submit_pipeline: Callable[[PipelineConfig], None]
) -> Status:
    """Run a job, which is the smallest atomic task of antz"""

    status: Status = Status.STARTING
    func_handle = config.function

    try:
        ret = func_handle(config.parameters, submit_pipeline)
        if isinstance(ret, Status):
            status = ret
        else:
            status = Status.ERROR  # bad return type is an error
    except Exception as _exc:  # pylint: disable=broad-exception-caught
        # logging?
        status = Status.ERROR
    return status

"""A job is the basic unit of execution in this module

Each job performs one user-assigned task and returns its state.
"""
from antz.infrastructure.config.base import JobConfig, PARAMETERS_TYPE, PipelineConfig
from antz.infrastructure.core.status import Status
from typing import Callable
import importlib

def run_job(config: JobConfig, submit_pipeline: Callable[[PipelineConfig], None]) -> Status:


    status: Status = Status.STARTING
    func_handle = config.function

    try:
        ret = func_handle(config.parameters, submit_pipeline)
        if isinstance(ret, Status):
            status = ret
        else:
            status = Status.ERROR # bad return type is an error
    except Exception as _exc:
        # logging?
        status = Status.ERROR
    return status



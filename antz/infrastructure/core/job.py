"""A job is the basic unit of execution in this module

Each job performs one user-assigned task and returns its state.
"""
from antz.infrastructure.config.base import JobConfig, PARAMETERS_TYPE, PipelineConfig
from antz.infrastructure.core.status import Status
from typing import Callable
import importlib

def run_job(config: JobConfig, submit_pipeline: Callable[[PipelineConfig], None]) -> Status:


    status: Status = Status.STARTING

    try:
        func_handle = _get_func(config)
    except ValueError as _exc:
        return Status.ERROR

    try:
        ret = func_handle(config.parameters, submit_pipeline)
        if isinstance(ret, Status):
            status = ret
        else:
            pass # logging?
    except Exception as _exc:
        # logging?
        status = Status.ERROR
    return status

def _get_func(config: JobConfig) -> Callable[[PARAMETERS_TYPE, Callable[[PipelineConfig], None]], Status]:
    """Links to the function described by config

    Args:
        config (JobConfig): configuration of the job to link

    Returns:
        Callable[PARAMETERS_TYPE], Status]: _description_


    """

    name: str = config.function

    components = name.split('.')
    func_name = components[-1]
    mod_name = '.'.join(components[:-1])

    try:
        mod = importlib.import_module(mod_name)
    except ModuleNotFoundError as exc:
        raise ValueError(f"Cannot import {name}") from exc

    if not hasattr(mod, func_name):
        raise ValueError(f'Module {mod_name} has no function {func_name}')

    func = getattr(mod, func_name)

    if not callable(func):
        raise ValueError(f'Function {func_name} is not callable')

    return func




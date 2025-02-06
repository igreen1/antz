"""A job is the basic unit of execution in this module

Each job performs one user-assigned task and returns its state.
"""

import logging
from typing import Callable, Mapping

from antz.infrastructure.config.base import (Config, JobConfig,
                                             MutableJobConfig, PipelineConfig,
                                             PrimitiveType)
from antz.infrastructure.core.status import Status
from antz.infrastructure.core.variables import resolve_variables


def run_job(
    config: JobConfig,
    variables: Mapping[str, PrimitiveType],
    submit_fn: Callable[[Config], None],
    logger: logging.Logger,
) -> Status:
    """Run a job, which is the smallest atomic task of antz"""
    status: Status = Status.STARTING
    func_handle = config.function

    params = resolve_variables(config.parameters, variables)

    try:
        ret = func_handle(params, submit_fn)
        if isinstance(ret, Status):
            status = ret
        else:
            status = Status.ERROR  # bad return type is an error
    except Exception as _exc:  # pylint: disable=broad-exception-caught
        # logging?
        status = Status.ERROR
    return status


def run_mutable_job(
    config: MutableJobConfig,
    variables: Mapping[str, PrimitiveType],
    submit_fn: Callable[[Config], None],
    pipeline_config: PipelineConfig,
    logger: logging.Logger,
) -> tuple[Status, PipelineConfig, Mapping[str, PrimitiveType]]:
    """Like a job, but it will return a new pipeline and variables context

    Can be used to branch the pipeline (conditionals), update the scope,
        or perform other advanced operations

    Args:
        config (MutableJobConfig): configuration of the function to run
        variables (Mapping[str, PrimitiveType]): variables in scope for this job
        submit_fn (Callable[[Config], None]): function to submit new jobs onto the runner system
        pipeline_config (PipelineConfig): configuration of the parent pipeline

    Returns:
        tuple[Status, PipelineConfig, Mapping[str, PrimitiveType]]:
            new configuration of parent pipeline and the new variables to use
    """
    status: Status = Status.STARTING
    func_handle = config.function

    params = resolve_variables(config.parameters, variables)

    try:
        ret = func_handle(params, submit_fn, variables, pipeline_config)
        if (
            isinstance(ret, tuple)
            and len(ret) == 3
            and isinstance(ret[0], Status)
            and isinstance(ret[1], (PipelineConfig, dict))
            and isinstance(ret[2], dict)
        ):
            # check output - don't trust unknown libraries
            status = ret[0]
            if isinstance(ret[1], dict):
                pipeline_config = PipelineConfig.model_validate(ret[1])
            else:
                pipeline_config = ret[1]
            variables = ret[2]
        else:
            status = Status.ERROR  # bad return type is an error
    except Exception as _exc:  # pylint: disable=broad-exception-caught
        # logging?
        status = Status.ERROR
    return status, pipeline_config, variables

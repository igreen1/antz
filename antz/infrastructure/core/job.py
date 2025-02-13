"""A job is the basic unit of execution in this module

Each job performs one user-assigned task and returns its state.
"""

import logging
from typing import Callable, Mapping

from antz.infrastructure.config.base import (
    Config,
    JobConfig,
    PrimitiveType,
)
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


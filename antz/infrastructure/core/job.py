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
    logger.debug('Running job %s, with func handle: %s', config.id, str(func_handle))

    params = resolve_variables(config.parameters, variables)
    logger.debug('Running function with parameters %s', str(params))

    try:
        ret = func_handle(params, submit_fn, variables, logger)
        if isinstance(ret, Status):
            status = ret
        else:
            logger.warning('Return of function was not an ANTZ status, this is an automatic error')
            status = Status.ERROR  # bad return type is an error
    except Exception as exc:  # pylint: disable=broad-exception-caught
        logger.warning('Unexpected error', exc_info=exc)
        status = Status.ERROR
    logger.debug('Finished job %s with status %s', config.id, str(status))
    return status


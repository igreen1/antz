"""Do nothing

Sometimes the syntax requires something to be filled in, but we
    don't want to do anything

In those cases, use a NOP
"""
import logging
from antz.infrastructure.config.base import ParametersType
from antz.infrastructure.config.job_decorators import simple_job

from antz.infrastructure.core.status import Status


@simple_job
def nop(_parameters: ParametersType, _logger: logging.Logger) -> Status:
    """Do nothing"""
    return Status.SUCCESS

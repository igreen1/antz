"""Do nothing

Sometimes the syntax requires something to be filled in, but we
    don't want to do anything

In those cases, use a NOP
"""

from antz.infrastructure.core.status import Status


def nop(*_, **__) -> Status:
    """Do nothing"""
    return Status.SUCCESS

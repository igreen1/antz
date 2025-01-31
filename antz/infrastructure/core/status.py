"""
All tasks have a status, this is the common enum
"""
from enum import IntEnum, auto


class Status(IntEnum):
    ERROR = auto()
    READY = auto()
    STARTING = auto()
    RUNNING = auto()
    SUCCESS = auto()

def is_final(status: Status) -> bool:
    """Return true if this status implies that the task is finished running 

    Tasks with a final status should have completed all their processing
        and not have any open resources

    :param status: status to check
    :type status: Status
    :return: True if the status is final; false otherwise
    :rtype: bool
    """
    return status == Status.ERROR or status == Status.SUCCESS


def is_startable(status: Status) -> bool:
    """Returns true if this status is able to be started"""

    return status == Status.READY

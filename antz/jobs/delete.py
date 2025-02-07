"""Delete a file or directory

params = {
    "path" (str): the path to delete
}
"""

import os
import shutil

from pydantic import BeforeValidator, BaseModel
from typing_extensions import Annotated

from antz.infrastructure.config.base import ParametersType
from antz.infrastructure.core.status import Status


class DeleteParameters(BaseModel, frozen=True):
    """The parameters required for the copy command"""

    path: Annotated[str, BeforeValidator(lambda x: x if os.path.exists(x) else None)]


def delete(parameters: ParametersType, *_, **__) -> Status:
    """Deletes parameters.path

    Args:
        parameters (ParametersType): params of form
            {
                path (str): path/to/delete
            }

    Returns:
        Status: Resulting status of the job after execution
    """

    del_params = DeleteParameters.model_validate(parameters)
    if os.path.isdir(del_params.path):
        try:
            shutil.rmtree(del_params.path)
        except (PermissionError, FileNotFoundError, IOError):
            return Status.ERROR
    elif os.path.isfile(del_params.path):
        try:
            os.remove(del_params.path)
        except (PermissionError, FileNotFoundError, IOError):
            return Status.ERROR
    else:
        return Status.ERROR

    return Status.SUCCESS

"""Copy job will copy a file or directory to another location"""
from antz.infrastructure.config.base import PARAMETERS_TYPE
from antz.infrastructure.core.status import Status

import os
from pydantic import BaseModel
import shutil

class CopyParameters(BaseModel, frozen=True):
    """The parameters required for the copy command"""
    source: str
    destination: str
    infer_name: bool = False


def copy(parameters: PARAMETERS_TYPE) -> Status:
    """Copy file or directory from parameters.soruce to parameters.destination

    Parameters {
        source: path/to/copy/from
        destination: path/to/copy/to
    }

    Args:
        parameters (PARAMETERS_TYPE): parameters for the copy function

    Returns:
        Status: result of the job
    """
    if parameters is None:
        return Status.ERROR
    copy_parameters = CopyParameters.model_validate(parameters)

    source = copy_parameters.source

    if not os.path.exists(source):
        return Status.ERROR
    source_is_file = os.path.isfile(source)

    if source_is_file:
        return _copy_file(copy_parameters)
    
    return _copy_dir(copy_parameters)

def _copy_file(copy_parameters: CopyParameters) -> Status: 
    """Copy a file from source to destination

    Args:
        copy_parameters (CopyParameters): parameters of the copy job

    Returns:
        Status: resulitng status after running the job
    """
    src = copy_parameters.source
    dst = copy_parameters.destination

    if os.path.exists(dst) and os.path.isdir(dst):
        if copy_parameters.infer_name:
            dst = os.path.join(dst, os.path.basename(src))

    if os.path.exists(dst) and os.path.isdir(dst):
        return Status.ERROR
    
    try:
        shutil.copyfile(src, dst)
        return Status.SUCCESS
    except Exception as _exc:
        return Status.ERROR

def _copy_dir(copy_parameters: CopyParameters) -> Status:
    """Copy a directory from a source to destination
    Args:
        copy_parameters (CopyParameters): parameters of the copy job

    Returns:
        Status: resulitng status after running the job
    """

    src = copy_parameters.source
    dst = copy_parameters.destination

    if os.path.exists(dst) and os.path.isfile(dst):
        return Status.ERROR
    
    try:
        shutil.copytree(src, dst)
        return Status.SUCCESS
    except Exception as _exc:
        return Status.ERROR
"""
This is the base level of the configuration for the core components
"""

from __future__ import annotations

import importlib
import uuid
from typing import Any, Callable, Literal, Dict, TypeAlias

from pydantic import BaseModel, BeforeValidator, Field
from typing_extensions import Annotated

from antz.infrastructure.core.status import Status

PrimitiveType: TypeAlias = str | int | float | bool
ParametersType: TypeAlias = Dict[str, PrimitiveType] | None
SubmitFunctionType: TypeAlias = Callable[["Config"], None]
JobFunctionType: TypeAlias = Callable[
    [ParametersType, SubmitFunctionType], Status
]
MutableJobFunctionType: TypeAlias = Callable[
    [ParametersType, SubmitFunctionType, Dict[str, PrimitiveType], "PipelineConfig"], 
    tuple[Status, "PipelineConfig", Dict[str, PrimitiveType]]
]


def _get_job_func(func_name_or_any: Any) -> JobFunctionType | None:
    """Links to the function described by config

    Args:
        config (JobConfig): configuration of the job to link

    Returns:
        Callable[[ParametersType, Callable[[PipelineConfig], None]], Status] } None:
            a function that takes parameters and a
            submitter callable and returns a status after executing
            Returns None if it is unable to find the correct function

    """

    if not isinstance(func_name_or_any, str):
        return None

    name: str = func_name_or_any

    components = name.split(".")
    func_name = components[-1]
    mod_name = ".".join(components[:-1])

    try:
        mod = importlib.import_module(mod_name)
    except ModuleNotFoundError as _:
        return None

    if not hasattr(mod, func_name):
        return None

    func = getattr(mod, func_name)

    if not callable(func):
        return None

    return func

class JobConfig(BaseModel, frozen=True):
    """Configuration of a job"""

    type: Literal["job"]
    name: str = "some job"
    id: uuid.UUID = Field(default_factory=uuid.uuid4, validate_default=True)
    function: Annotated[JobFunctionType, BeforeValidator(_get_job_func)]
    parameters: ParametersType


class MutableJobConfig(BaseModel, frozen=True):
    """A normal job but it can edit its parents configurations
    This can obviously create some issues, but also is useful in quite a few cases. 
    USE WITH CARE"""
    
    type: Literal["mutable_job"]
    name: str = "some job"
    id: uuid.UUID = Field(default_factory=uuid.uuid4, validate_default=True)
    function: Annotated[MutableJobFunctionType, BeforeValidator(_get_job_func)]
    parameters: ParametersType


class PipelineConfig(BaseModel, frozen=True):
    """Configuration of a pipeline, which is a series of jobs or sub-pipelines"""

    type: Literal["pipeline"]
    name: str = "pipeline"
    curr_state: int = 0
    status: int = Status.READY
    max_allowed_restarts: int = 0
    curr_restarts: int = 0
    stages: list[JobConfig | PipelineConfig | MutableJobConfig]


class Config(BaseModel, frozen=True):
    """The global configuration to submit to runner"""

    variables: dict[str, PrimitiveType]
    config: PipelineConfig
    # config: PipelineConfig | JobConfig = Field(discriminator='type')

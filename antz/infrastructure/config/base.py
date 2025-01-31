"""
This is the base level of the configuration which 
"""
import uuid

from antz.infrastructure.core.status import Status
from typing import Literal, TypeAlias
from pydantic import BaseModel, Field

PRIMITIVE_TYPE: TypeAlias = str | int | float | bool
PARAMETERS_TYPE: TypeAlias = (dict[str, PRIMITIVE_TYPE] | None)

class JobConfig(BaseModel, frozen=True):
    type: Literal['job']
    name: str = 'some job'
    id: uuid.UUID = Field(default_factory=uuid.uuid4, validate_default=True)
    function: str
    parameters: PARAMETERS_TYPE

class PipelineConfig(BaseModel, frozen=True):
    type: Literal['pipeline']
    name: str = 'pipeline'
    curr_state: int = 0
    status: int = Status.READY
    max_allowed_restarts: int = 0
    curr_restarts: int = 0
    stages: list[JobConfig]


class Config(BaseModel, frozen=True):
    variables: dict[str, PRIMITIVE_TYPE]
    config: PipelineConfig
    # config: PipelineConfig | JobConfig = Field(discriminator='type')


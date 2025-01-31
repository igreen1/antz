"""
This is the base level of the configuration which 
"""
import uuid

from typing import Literal, TypeAlias
from pydantic import BaseModel, Field

PRIMITIVE_TYPE: TypeAlias = str | int | float | bool

class JobConfig(BaseModel):
    type: Literal['job']
    name: str = 'some job'
    id: uuid.UUID = Field(default_factory=uuid.uuid4, validate_default=True)
    function: str
    parameters: dict[str, PRIMITIVE_TYPE] | None

class PipelineConfig(BaseModel):
    type: Literal['pipeline']
    name: str = 'pipeline'
    curr_state: int = 0
    stages: list[JobConfig]


class Config(BaseModel):
    variables: dict[str, PRIMITIVE_TYPE]
    config: PipelineConfig | JobConfig = Field(discriminator='type')

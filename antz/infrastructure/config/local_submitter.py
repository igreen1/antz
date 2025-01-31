
from typing import Literal
from pydantic import BaseModel

class LocalSubmitterConfig(BaseModel, frozen=True):
    type: Literal['local']
    name: str = 'local submitter'
    num_concurrent_jobs: int = 1

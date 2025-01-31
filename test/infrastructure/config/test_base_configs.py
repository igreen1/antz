
from antz.instrastructure.config.base import JobConfig
import json
import uuid
from pydantic import ValidationError
import pytest

def test_job_uuid_creation() -> None:

    job_config = {
        'type': 'job',
        'name': 'my job',
        'function': 'some func',
        'parameters': {
            's': 1
        }
    }

    j1 = JobConfig.model_validate(job_config)
    j2 = JobConfig.model_validate(job_config)
    assert j1.id != j2.id

    job_config_str: str = json.dumps(job_config)
    j1 = JobConfig.model_validate_json(job_config_str)
    j2 = JobConfig.model_validate_json(job_config_str)
    assert j1.id != j2.id

def test_job_uuid_override() -> None:
    
    
    
    job_config = {
        'type': 'job',
        "id": "cb13bc78-e4a2-4a69-8a54-04ef168d656e",
        'name': 'my job',
        'function': 'some func',
        'parameters': {
            's': 1
        }
    }

    j1 = JobConfig.model_validate(job_config)
    j2 = JobConfig.model_validate(job_config)
    assert j1.id == j2.id # should be the same because we overrode
    assert j1.id == uuid.UUID("cb13bc78-e4a2-4a69-8a54-04ef168d656e")


def test_empty_job_parameters() -> None:
    
    job_config = {
        'type': 'job',
        'name': 'my job',
        'function': 'some func',
        'parameters': {
        }
    }
    _j1 = JobConfig.model_validate(job_config)

    with pytest.raises(ValidationError):
        job_config = {
            'type': 'job',
            'name': 'my job',
            'function': 'some func',
        }
        _j1 = JobConfig.model_validate(job_config)

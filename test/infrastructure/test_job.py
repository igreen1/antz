import pytest
from pydantic import ValidationError

from antz.infrastructure.config.base import JobConfig, PipelineConfig
from antz.infrastructure.core.job import run_job
from antz.infrastructure.core.status import Status


def successful_function(*args):
    return Status.SUCCESS


def failed_function(*args):
    return Status.ERROR


def error_function(*args):
    raise Exception("Some error")


def fake_submission(c: PipelineConfig) -> None:
    pass


def test_getting_functions() -> None:

    job_config: dict = {
        "type": "job",
        "function": "test.infrastructure.test_job.successful_function",
        "parameters": {},
    }
    jc = JobConfig.model_validate(job_config)
    assert jc.function == successful_function
    assert jc.function == successful_function
    assert jc.function(jc.parameters, fake_submission) == Status.SUCCESS

    job_config = {
        "type": "job",
        "function": "test.infrastructure.test_job.failed_function",
        "parameters": {},
    }
    jc = JobConfig.model_validate(job_config)
    assert jc.function == failed_function
    assert jc.function(jc.parameters, fake_submission) == Status.ERROR


def test_running_job_success() -> None:
    job_config: dict = {
        "type": "job",
        "function": "test.infrastructure.test_job.successful_function",
        "parameters": {},
    }
    jc = JobConfig.model_validate(job_config)

    assert run_job(jc, fake_submission) == Status.SUCCESS


def test_running_job_failure() -> None:
    job_config: dict = {
        "type": "job",
        "function": "test.infrastructure.test_job.failed_function",
        "parameters": {},
    }
    jc = JobConfig.model_validate(job_config)

    assert run_job(jc, fake_submission) == Status.ERROR


def test_running_job_exception() -> None:
    job_config: dict = {
        "type": "job",
        "function": "test.infrastructure.test_job.error_function",
        "parameters": {},
    }
    jc = JobConfig.model_validate(job_config)

    assert run_job(jc, fake_submission) == Status.ERROR


def test_no_function_error() -> None:
    job_config: dict = {
        "type": "job",
        "function": "test.infrastructure.test_job.NOSUCHFUNCTION",
        "parameters": {},
    }

    with pytest.raises(ValidationError):
        _ = JobConfig.model_validate(job_config)


def test_not_a_callable() -> None:
    job_config: dict = {
        "type": "job",
        "function": "test.infrastructure.test_job",
        "parameters": {},
    }

    with pytest.raises(ValidationError):
        _ = JobConfig.model_validate(job_config)


def test_not_a_module() -> None:
    job_config: dict = {
        "type": "job",
        "function": "antz.no.such.module",
        "parameters": {},
    }
    with pytest.raises(ValidationError):
        _ = JobConfig.model_validate(job_config)

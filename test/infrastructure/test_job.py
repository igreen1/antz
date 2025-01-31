
from antz.infrastructure.config.base import JobConfig
from antz.infrastructure.status import Status
from antz.infrastructure.job import _get_func, run_job
import pytest

def successful_function(*args):
    return Status.SUCCESS

def failed_function(*args):
    return Status.ERROR

def error_function(*args):
    raise Exception("Some error")

def test_getting_functions() -> None:

    job_config: dict = {
        'type': 'job',
        'function': 'test.infrastructure.test_job.successful_function',
        'parameters': {}
    }
    jc = JobConfig.model_validate(job_config)
    assert _get_func(jc) == successful_function
    assert _get_func(jc)(jc.parameters) == Status.SUCCESS

    job_config = {
        'type': 'job',
        'function': 'test.infrastructure.test_job.failed_function',
        'parameters': {}
    }
    jc = JobConfig.model_validate(job_config)
    assert _get_func(jc) == failed_function
    assert _get_func(jc)(jc.parameters) == Status.ERROR

def test_running_job_success() -> None:
    job_config: dict = {
        'type': 'job',
        'function': 'test.infrastructure.test_job.successful_function',
        'parameters': {}
    }
    jc = JobConfig.model_validate(job_config)

    assert run_job(jc) == Status.SUCCESS

def test_running_job_failure() -> None:
    job_config: dict = {
        'type': 'job',
        'function': 'test.infrastructure.test_job.failed_function',
        'parameters': {}
    }
    jc = JobConfig.model_validate(job_config)

    assert run_job(jc) == Status.ERROR

def test_running_job_exception() -> None:
    job_config: dict = {
        'type': 'job',
        'function': 'test.infrastructure.test_job.error_function',
        'parameters': {}
    }
    jc = JobConfig.model_validate(job_config)

    assert run_job(jc) == Status.ERROR

def test_no_function_error() -> None:
    job_config: dict = {
        'type': 'job',
        'function': 'test.infrastructure.test_job.NOSUCHFUNCTION',
        'parameters': {}
    }
    jc = JobConfig.model_validate(job_config)

    with pytest.raises(ValueError):
        _get_func(jc)
    assert run_job(jc) == Status.ERROR

def test_not_a_callable() -> None:
    job_config: dict = {
        'type': 'job',
        'function': 'test.infrastructure.test_job',
        'parameters': {}
    }
    jc = JobConfig.model_validate(job_config)

    with pytest.raises(ValueError):
        _get_func(jc)
    assert run_job(jc) == Status.ERROR

def test_not_a_module() -> None:
    job_config: dict = {
        'type': 'job',
        'function': 'antz.no.such.module',
        'parameters': {}
    }
    jc = JobConfig.model_validate(job_config)

    with pytest.raises(ValueError):
        _get_func(jc)
    assert run_job(jc) == Status.ERROR
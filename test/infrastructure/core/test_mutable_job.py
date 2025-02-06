"""Test jobs that can mutate the context"""

import queue
from typing import Any

from antz.infrastructure.config.base import (Config, MutableJobConfig,
                                             PipelineConfig, Status)
from antz.infrastructure.core.job import run_mutable_job
from antz.infrastructure.core.pipeline import run_pipeline


def test_mutating_pipeline_and_variables_in_pipeline() -> None:
    job_config: dict = {
        "type": "mutable_job",
        "function": f"{__name__}.update_variables_and_pipeline",
        "parameters": {},
    }
    pipeline_config = {
        "type": "pipeline",
        "stages": [
            job_config,
            job_config,  # need two for the pipeline to submit itself
        ],
    }
    test_queue = queue.Queue()

    def submit_fn(config) -> None:
        test_queue.put(config)

    start_pipeline_config = PipelineConfig.model_validate(pipeline_config)
    assert (
        run_pipeline(
            config=start_pipeline_config, variables={"a": 1}, submit_fn=submit_fn
        )
        == Status.SUCCESS
    )

    assert test_queue.qsize() > 0
    ret = test_queue.get()

    assert isinstance(ret, Config)
    assert ret.variables == {"a": 2}
    assert ret.config.curr_stage == 0  # set to -1 then +1 for finishing the stage
    assert ret.config.max_allowed_restarts == -1


def test_mutating_variables_job() -> None:
    """Change the variables of the parent context"""
    job_config: dict = {
        "type": "mutable_job",
        "function": f"{__name__}.successful_job",
        "parameters": {},
    }
    pipeline_config = {"type": "pipeline", "stages": [job_config]}
    pc = PipelineConfig.model_validate(pipeline_config)
    status, ret_pipeline_config, ret_variables = run_mutable_job(
        MutableJobConfig.model_validate(pc.stages[0]),
        variables={"a": 1},
        submit_fn=lambda *args: None,
        pipeline_config=pc,
    )
    assert status == Status.SUCCESS
    assert ret_pipeline_config == pc


def test_mutable_success_job() -> None:
    job_config: dict = {
        "type": "mutable_job",
        "function": f"{__name__}.successful_job",
        "parameters": {},
    }
    pipeline_config = {"type": "pipeline", "stages": [job_config]}
    pc = PipelineConfig.model_validate(pipeline_config)
    # jc = JobConfig.model_validate(job_config)

    status, ret_pipeline_config, ret_variables = run_mutable_job(
        MutableJobConfig.model_validate(pc.stages[0]),
        variables={},
        submit_fn=lambda *args: None,
        pipeline_config=pc,
    )
    assert status == Status.SUCCESS
    assert ret_pipeline_config == pc
    assert ret_variables == {}


def test_mutable_failure_job() -> None:
    job_config: dict = {
        "type": "mutable_job",
        "function": f"{__name__}.failed_job",
        "parameters": {},
    }
    pipeline_config = {"type": "pipeline", "stages": [job_config]}
    pc = PipelineConfig.model_validate(pipeline_config)
    # jc = JobConfig.model_validate(job_config)

    status, ret_pipeline_config, ret_variables = run_mutable_job(
        MutableJobConfig.model_validate(pc.stages[0]),
        variables={},
        submit_fn=lambda *args: None,
        pipeline_config=pc,
    )
    assert status == Status.ERROR
    assert ret_pipeline_config == pc
    assert ret_variables == {}


def test_mutable_exception_job() -> None:
    job_config: dict = {
        "type": "mutable_job",
        "function": f"{__name__}.error_job",
        "parameters": {},
    }
    pipeline_config = {"type": "pipeline", "stages": [job_config]}
    pc = PipelineConfig.model_validate(pipeline_config)
    # jc = JobConfig.model_validate(job_config)

    status, ret_pipeline_config, ret_variables = run_mutable_job(
        MutableJobConfig.model_validate(pc.stages[0]),
        variables={},
        submit_fn=lambda *args: None,
        pipeline_config=pc,
    )
    assert status == Status.ERROR
    assert ret_pipeline_config == pc
    assert ret_variables == {}


def successful_job(params, submit_fn, variables, pipeline_config) -> Any:
    """Return success"""
    return Status.SUCCESS, pipeline_config, variables


def failed_job(params, submit_fn, variables, pipeline_config) -> Any:
    """Return failure"""
    return Status.ERROR, pipeline_config, variables


def error_job(params, submit_fn, variables, pipeline_config) -> Any:
    """Throws an error"""
    raise Exception("Some error")


def update_variables(params, submit_fn, variables, pipeline_config) -> Any:
    """Change the variables"""
    return Status.SUCCESS, pipeline_config, {"a": 2}


def update_variables_and_pipeline(params, submit_fn, variables, pipeline_config) -> Any:
    """Change the variables and pipeline"""
    config_as_dict = pipeline_config.model_dump()
    config_as_dict["max_allowed_restarts"] = -1
    config_as_dict["curr_stage"] = -1
    return_config = PipelineConfig.model_validate(config_as_dict)
    return Status.SUCCESS, return_config, {"a": 2}

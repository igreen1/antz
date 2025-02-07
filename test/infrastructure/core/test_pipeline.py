"""Test running a pipelin"""

import logging
import queue
from typing import Any

from antz.infrastructure.config.base import Config, PipelineConfig, Status
from antz.infrastructure.core.manager import run_manager
from antz.infrastructure.core.pipeline import run_pipeline

logger = logging.getLogger("test")
logger.setLevel(100000)  # don't log in tests


def test_multiple_restarts_pipeline() -> None:
    """Test that a pipeline can be restarted multiple times"""
    test_queue = queue.Queue()

    def submit_fn(config) -> None:
        test_queue.put(config)

    job_config: dict = {
        "type": "job",
        "function": f"{__name__}.failed_job",
        "parameters": {},
    }
    pipeline_config = {
        "type": "pipeline",
        "max_allowed_restarts": 2,
        "stages": [job_config],
    }
    config_dict = {"config": pipeline_config, "variables": {}}
    config = Config.model_validate(config_dict)

    run_manager(config, submit_fn, logger=logger)
    assert test_queue.qsize() == 1
    ret = test_queue.get()
    assert ret.config.curr_restarts == 1
    assert ret.config.curr_stage == 0

    run_manager(ret, submit_fn, logger=logger)
    assert test_queue.qsize() == 1
    ret = test_queue.get()
    assert ret.config.curr_restarts == 2
    assert ret.config.curr_stage == 0

    run_manager(ret, submit_fn, logger=logger)
    assert test_queue.qsize() == 0


def test_restarting_pipeline() -> None:
    """Test that pipelines can restarte"""

    test_queue = queue.Queue()

    def submit_fn(config) -> None:
        test_queue.put(config)

    job_config: dict = {
        "type": "job",
        "function": f"{__name__}.failed_job",
        "parameters": {},
    }
    pipeline_config = {
        "type": "pipeline",
        "max_allowed_restarts": 1,
        "stages": [job_config],
    }

    pc = PipelineConfig.model_validate(pipeline_config)
    status = run_pipeline(pc, variables={}, submit_fn=submit_fn, logger=logger)
    assert status == Status.ERROR
    assert test_queue.qsize() > 0
    ret = test_queue.get()
    assert ret.config.curr_stage == 0


def test_running_successful_pipeline() -> None:
    test_queue = queue.Queue()

    def submit_fn(config) -> None:
        test_queue.put(config)

    job_config: dict = {
        "type": "job",
        "function": f"{__name__}.successful_job",
        "parameters": {},
    }
    pipeline_config = {"type": "pipeline", "stages": [job_config]}

    pc = PipelineConfig.model_validate(pipeline_config)
    status = run_pipeline(pc, variables={}, submit_fn=submit_fn, logger=logger)
    assert status == Status.SUCCESS
    assert test_queue.qsize() == 0


def test_running_failed_pipeline() -> None:
    test_queue = queue.Queue()

    def submit_fn(config) -> None:
        test_queue.put(config)

    job_config: dict = {
        "type": "job",
        "function": f"{__name__}.failed_job",
        "parameters": {},
    }
    pipeline_config = {"type": "pipeline", "stages": [job_config]}

    pc = PipelineConfig.model_validate(pipeline_config)
    status = run_pipeline(pc, variables={}, submit_fn=submit_fn, logger=logger)
    assert status == Status.ERROR
    assert test_queue.qsize() == 0


def test_running_error_pipeline() -> None:
    """Test that a pipeline with an error quits without restart configuration"""
    test_queue = queue.Queue()

    def submit_fn(config) -> None:
        test_queue.put(config)

    job_config: dict = {
        "type": "job",
        "function": f"{__name__}.error_job",
        "parameters": {},
    }
    pipeline_config = {"type": "pipeline", "stages": [job_config]}

    pc = PipelineConfig.model_validate(pipeline_config)
    status = run_pipeline(pc, variables={}, submit_fn=submit_fn, logger=logger)
    assert status == Status.ERROR
    assert test_queue.qsize() == 0


def successful_job(*args) -> Any:
    """Return success"""
    return Status.SUCCESS


def failed_job(*args) -> Any:
    """Return failure"""
    return Status.ERROR


def error_job(*args) -> Any:
    """Throws an error"""
    raise Exception("Some error")

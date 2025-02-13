"""A pipeline is a set of tasks to perform in series"""

import logging
from typing import Callable, Mapping

from antz.infrastructure.config.base import (
    Config,
    PipelineConfig,
    PrimitiveType,
)
from antz.infrastructure.core.job import run_job
from antz.infrastructure.core.status import Status, is_final


def run_pipeline(
    config: PipelineConfig,
    variables: Mapping[str, PrimitiveType],
    submit_fn: Callable[[Config], None],
    logger: logging.Logger,
) -> Status:
    """Run the provided pipeline

    Args:
        config (PipelineConfig): configuration of the pipeline to run
        submit_fn (Callable[[Config], None]):
            function to submit a next config to the runners

    Returns:
        Status: the status of the pipeline after executing the next job
    """
    logger.debug('Starting pipeline %s', config.id)

    if config.curr_stage < len(config.stages):
        # run the job
        curr_job = config.stages[config.curr_stage]
        if isinstance(curr_job, PipelineConfig):
            logger.debug('Calling run_pipeline for %s', curr_job.id)
            ret_status = run_pipeline(
                curr_job, variables=variables, submit_fn=submit_fn, logger=logger
            )  # allows pipelines of pipelines
        else:
            logger.debug('Calling run_job %s', curr_job.id)
            ret_status = run_job(
                curr_job, variables=variables, submit_fn=submit_fn, logger=logger
            )

        # handle pipeline cleanup/termination
        if ret_status == Status.ERROR:
            logger.warning('Error in stage %d of pipeline %s', config.curr_stage, config.id)
            restart(
                config, variables=variables, submit_fn=submit_fn, logger=logger
            )  # optionally restart if setup for that
        elif not is_final(ret_status):
            logger.error('ERROR: Incomplete status - error in the ANTZ library is likely')
            # error! shouldn't happen
            return Status.ERROR
        else:
            logger.debug('Success in pipeline %s', config.id)
            success(config, variables=variables, submit_fn=submit_fn, logger=logger)
        return ret_status

    return Status.ERROR


def success(
    config: PipelineConfig,
    variables: Mapping[str, PrimitiveType],
    submit_fn: Callable[[Config], None],
    logger: logging.Logger,
) -> None:
    """Resubmit this pipeline setup for the next job after a success"""
    logger.debug('Success in pipeline')
    next_config = config.model_dump()
    next_config["curr_stage"] += 1
    if next_config["curr_stage"] < len(next_config["stages"]):
        next_pipeline_config = PipelineConfig.model_validate(next_config)
        logger.debug('Submittig next pipeline stage: %d', next_pipeline_config.curr_stage)
        submit_fn(
            Config.model_validate(
                {"variables": variables, "config": next_pipeline_config}
            )
        )
    else:
        logger.debug('Pipeline %s completed successfully, exiting this execution line', config.id)


def restart(
    config: PipelineConfig,
    variables: Mapping[str, PrimitiveType],
    submit_fn: Callable[[Config], None],
    logger: logging.Logger,
) -> None:
    """Restart the config provided by updating and submit to submitter"""
    if (
        config.max_allowed_restarts == -1
        or config.curr_restarts < config.max_allowed_restarts
    ):
        logger.debug('Restarting pipeline after failure')
        new_config = config.model_dump()
        new_config["curr_restarts"] += 1
        new_config["curr_stage"] = 0
        new_config["status"] = Status.READY

        new_pipeline_config = PipelineConfig.model_validate(new_config)
        new_config_cls = Config(config=new_pipeline_config, variables=variables)
        logger.debug('Submitting restarted pipeline with id %s', new_config_cls.config.id)
        submit_fn(new_config_cls)
    else:
        logger.debug('Not restarting pipeline; max restarts exceeded')

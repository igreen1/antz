"""Restarts the parent pipeline"""


import logging
from typing import Mapping

from antz.infrastructure.config.base import (Config, ParametersType,
                                             PipelineConfig, PrimitiveType,
                                             SubmitFunctionType)
from antz.infrastructure.core.status import Status



def restart_pipeline(
    parameters: ParametersType,
    submit_fn: SubmitFunctionType,
    variables: Mapping[str, PrimitiveType],
    pipeline_config: PipelineConfig,
    logger: logging.Logger,
    *_,
    **__,
) -> Status:
    """Create a series of parallel pipelines based on user input

    Args:
        parameters (ParametersType): mapping of string names of pipelines to pipeline configurations
        submit_fn (SubmitFunctionType): function to submit the pipeline to for execution
        variables (Mapping[str, PrimitiveType]): variables from the outer context
        logger (logging.Logger): logger to assist with debugging

    Returns:
        Status: SUCCESS if jobs successfully submitted; ERROR otherwise
    """
    logger.debug('Restarting pipeline %s', pipeline_config.id)
    new_pipeline = pipeline_config.model_dump()
    new_pipeline['curr_stage'] = 0

    submit_fn(
        Config.model_validate({
            'variables': variables,
            'config': new_pipeline
        })
    )
    return Status.FINAL
"""Given a template pipeline, create N new pipelines from that template
with a new variables PIPELINE_ID set to a counter


"""

import logging
from typing import Mapping

from pydantic import BaseModel, PositiveInt

from antz.infrastructure.config.base import (
    Config,
    ParametersType,
    PipelineConfig,
    PrimitiveType,
    SubmitFunctionType,
)
from antz.infrastructure.core.status import Status


class ExplodePipelinesParamters(BaseModel, frozen=True):
    """See explode pipeline docs"""

    num_pipelines: PositiveInt
    pipeline_config_template: PipelineConfig


def explode_pipeline(
    parameters: ParametersType,
    submit_fn: SubmitFunctionType,
    variables: Mapping[str, PrimitiveType],
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

    params_parsed = ExplodePipelinesParamters.model_validate(parameters)

    logger.debug("Exploding pipelines into %d pipelines", params_parsed.num_pipelines)

    for i in range(params_parsed.num_pipelines):
        submit_fn(
            Config.model_validate(
                {
                    "variables": {**variables, "PIPELINE_ID": i},
                    "config": params_parsed.pipeline_config_template,
                }
            )
        )

    return Status.SUCCESS

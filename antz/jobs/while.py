"""Restarts the parent pipeline while a conditional is true and update the iterator"""

import logging
import operator
from typing import Any, Callable, Literal, Mapping

from pydantic import BaseModel

from antz.infrastructure.config.base import (Config, ParametersType,
                                             PipelineConfig, PrimitiveType,
                                             SubmitFunctionType)
from antz.infrastructure.core.status import Status

comparators: dict[str, Callable[[Any, Any], bool]] = {
    "==": operator.eq,
    "!=": operator.ne,
    "<": operator.lt,
    "<=": operator.le,
    ">": operator.gt,
    ">=": operator.ge,
}


class RestartPipelineConfig(BaseModel, frozen=True):
    """Provides optional configuration of the restart pipeline job"""

    comparator: Literal["<", ">", "<=", ">=", "==", "!="]
    var_name: str
    right: PrimitiveType


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
    params_parsed = RestartPipelineConfig.model_validate(parameters)
    if not isinstance(variables[params_parsed.var_name], (int, float)):
        raise ValueError("Variable for looping must be a numeric")
    result = comparators[params_parsed.comparator](
        variables[params_parsed.var_name], params_parsed.right
    )

    if not result:
        return Status.FINAL
    logger.debug("Restarting pipeline %s", pipeline_config.id)
    new_pipeline = pipeline_config.model_dump()
    new_pipeline["curr_stage"] = 0

    variables[params_parsed.var_name] += 1  # type: ignore

    submit_fn(Config.model_validate({"variables": variables, "config": new_pipeline}))

    return Status.FINAL

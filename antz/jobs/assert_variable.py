"""Asserts that a variable is of a certain value"""

import logging
from typing import Mapping

from pydantic import BaseModel, ValidationError

from antz.infrastructure.config.base import (ParametersType, PrimitiveType,
                                             SubmitFunctionType)
from antz.infrastructure.core.status import Status
from antz.infrastructure.core.variables import is_variable


class AssertVariableParameters(BaseModel, frozen=True):
    """See assert_variable docstring"""

    var_to_check: str
    expected_value: PrimitiveType


def assert_variable(
    parameters: ParametersType,
    submit_fn: SubmitFunctionType,
    variables: Mapping[str, PrimitiveType],
    logger: logging.Logger,
    *_,
    **__,
) -> Status:
    """Return ERROR if the variable doesn't match expectations

    AssertVariableParameters {
        var_to_check (str): name of the variable to check
        expected_value (PrimitiveType): expected value of the variable
    }

    Args:
        parameters (ParametersType): see above
        submit_fn (SubmitFunctionType): function to submit the pipeline to for execution
        variables (Mapping[str, PrimitiveType]): variables from the outer context
        logger (logging.Logger): logger to assist with debugging

    Returns:
        Status: SUCCESS if the variable matches expected value; ERROR otherwise
    """

    params_parsed = AssertVariableParameters.model_validate(parameters)

    if params_parsed.var_to_check not in variables:
        logger.error(
            "assert_variable: var %s not in variable scope", params_parsed.var_to_check
        )

    if variables[params_parsed.var_to_check] == params_parsed.expected_value:
        return Status.SUCCESS
    return Status.ERROR

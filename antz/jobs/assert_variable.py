"""Asserts that a variable is of a certain value"""

from pydantic import BaseModel

from antz.infrastructure.config.base import ParametersType, PrimitiveType
from antz.infrastructure.core.status import Status


class AssertVariableParameters(BaseModel, frozen=True):
    """See assert_variable docstring"""

    given_val: str
    expected_value: PrimitiveType


def assert_value(
    parameters: ParametersType,
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

    if params_parsed.given_val == params_parsed.expected_value:
        return Status.SUCCESS
    return Status.ERROR

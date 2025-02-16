import logging
from typing import Callable, Mapping

# avoid circular imports
from antz.infrastructure.config.base import (JobFunctionType,
                                             MutableJobFunctionType,
                                             ParametersType, PipelineConfig,
                                             PrimitiveType, Status,
                                             SubmitFunctionType,
                                             SubmitterJobFunctionType)


def mutable_job(
    fn: Callable[
        [ParametersType, Mapping[str, PrimitiveType], logging.Logger],
        tuple[Status, Mapping[str, PrimitiveType]],
    ],
) -> MutableJobFunctionType:
    def _mutable_job(
        parameters: ParametersType,
        variables: Mapping[str, PrimitiveType],
        logger: logging.Logger,
        *_,
        **__,
    ):
        return fn(parameters, variables, logger)

    _mutable_job.__module__ = fn.__module__
    _mutable_job.__name__ = fn.__name__

    return _mutable_job


def submitter_job(fn: SubmitterJobFunctionType) -> SubmitterJobFunctionType:
    def _submitter_job(
        parameters: ParametersType,
        submit_fn: SubmitFunctionType,
        variables: Mapping[str, PrimitiveType],
        pipeline_config: PipelineConfig,
        logger: logging.Logger,
    ):
        return fn(parameters, submit_fn, variables, pipeline_config, logger)

    _submitter_job.__module__ = fn.__module__
    _submitter_job.__name__ = fn.__name__
    return _submitter_job


def simple_job(
    fn: Callable[[ParametersType, logging.Logger], Status],
) -> JobFunctionType:
    def _simple_job(parameters: ParametersType, logger: logging.Logger, *_, **__):
        return fn(parameters, logger)

    _simple_job.__module__ = fn.__module__
    _simple_job.__name__ = fn.__name__
    return _simple_job

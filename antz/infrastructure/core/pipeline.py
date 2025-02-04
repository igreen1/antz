"""A pipeline is a set of tasks to perform in series"""

from typing import Callable, Dict

from antz.infrastructure.config.base import PipelineConfig, Config, PrimitiveType
from antz.infrastructure.core.job import run_job
from antz.infrastructure.core.status import Status, is_final


def run_pipeline(
    config: PipelineConfig, 
    variables: Dict[str, PrimitiveType],
    submit_fn: Callable[[Config], None]
) -> Status:
    """Run the provided pipeline

    Args:
        config (PipelineConfig): configuration of the pipeline to run
        submit_fn (Callable[[Config], None]):
            function to submit a next config to the runners

    Returns:
        Status: the status of the pipeline after executing the next job
    """

    if config.curr_state < len(config.stages):

        curr_job = config.stages[config.curr_state]
        if isinstance(curr_job, PipelineConfig):
            ret = run_pipeline(
                curr_job, variables=variables, submit_fn=submit_fn
            )  # allows pipelines of pipelines
        else:
            ret = run_job(curr_job, variables=variables, submit_fn=submit_fn)
        if ret == Status.ERROR:
            restart(
                config, variables=variables, submit_fn=submit_fn
            )  # optionally restart if setup for that
        elif not is_final(ret):
            # error! shouldn't happen
            return Status.ERROR
        else:
            success(config, variables=variables, submit_fn=submit_fn)
        return ret

    return Status.ERROR


def success(
    config: PipelineConfig, 
    variables: Dict[str, PrimitiveType],
    submit_fn: Callable[[Config], None]
) -> None:
    """Resubmit this pipeline setup for the next job after a success"""

    next_config = config.model_dump()
    next_config["curr_stage"] += 1
    if next_config["curr_stage"] < len(next_config["stages"]):
        next_pipeline_config = PipelineConfig.model_validate(next_config)
        submit_fn(Config.model_validate(dict(variables=variables, config=next_pipeline_config)))


def restart(
    config: PipelineConfig, 
    variables: Dict[str, PrimitiveType],
    submit_fn: Callable[[Config], None]
) -> None:
    """Restart the config provided by updating and submit to submitter"""

    if (
        config.max_allowed_restarts == -1
        or config.curr_restarts < config.max_allowed_restarts
    ):
        new_config = config.model_dump()
        new_config["curr_restarts"] += 1
        new_config["curr_state"] = 0
        new_config["status"] = Status.READY

        new_pipeline_config = PipelineConfig.model_validate(new_config)
        new_config_cls = Config(
            config=new_pipeline_config,
            variables=variables
        )

        submit_fn(new_config_cls)

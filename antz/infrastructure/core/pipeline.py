
from antz.infrastructure.config.base import PipelineConfig
from antz.infrastructure.core.status import Status, is_final
from antz.infrastructure.core.job import run_job
from typing import Callable

def run_pipeline(config: PipelineConfig, submit_pipeline: Callable[[PipelineConfig], None]) -> Status:
    """Run the provided pipeline

    Args:
        config (PipelineConfig): configuration of the pipeline to run
        submit_pipeline (Callable[[PipelineConfig], None]): function to submit a next config to the runners

    Returns:
        Status: the status of the pipeline after executing the next job
    """

    if config.curr_state < len(config.stages):

        curr_job = config.stages[config.curr_state]
        if isinstance(curr_job, PipelineConfig):
            ret = run_pipeline(curr_job, submit_pipeline=submit_pipeline) # allows pipelines of pipelines
        else:
            ret = run_job(curr_job, submit_pipeline=submit_pipeline)
        if ret == Status.ERROR:
            restart(config, submit_pipeline=submit_pipeline) # optionally restart if setup for that
        elif not is_final(ret):
            # error! shouldn't happen
            return Status.ERROR
        else:
            success(config, submit_pipeline=submit_pipeline)
        return ret
    else:
        # state is erroneous - BAD
        pass
        return Status.ERROR



def success(config: PipelineConfig, submit_pipeline: Callable[[PipelineConfig], None]) -> None:
    """Resubmit this pipeline setup for the next job after a success"""

    next_config = config.model_dump()
    next_config['curr_stage'] += 1
    if next_config['curr_stage'] < len(next_config['stages']):
        submit_pipeline(
            PipelineConfig.model_validate(next_config)
        )


def restart(config: PipelineConfig, submit_pipeline: Callable[[PipelineConfig], None]) -> None:
    """Restart the config provided by updating and submit to submitter"""

    if config.max_allowed_restarts == -1 or config.curr_restarts < config.max_allowed_restarts:
        new_config = config.model_dump()
        new_config['curr_restarts'] += 1
        new_config['curr_state'] = 0
        new_config['status'] = Status.READY

        submit_pipeline(
            PipelineConfig.model_validate(new_config)
        )
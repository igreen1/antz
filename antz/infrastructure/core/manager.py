"""Run a general config, which is a pipeline with a scope (variables)"""
from typing import Callable
from antz.infrastructure.config.base import Config
from antz.infrastructure.core.pipeline import run_pipeline

def run_manager(config: Config, submit_fn: Callable[[Config], None]) -> None:
    """Run the configuration"""
    run_pipeline(
        config=config.config,
        variables=config.variables,
        submit_fn=submit_fn
    )

"""Test the create pipelines from matrix job"""

import queue
import os
import pandas as pd
from antz.infrastructure.config.base import Config
from antz.infrastructure.core.manager import run_manager
import random
import string
import logging
 

FILE_LENGTH_MAX: int = 8000
def test_creating_multiple_pipelines_from_job(tmpdir) -> None:
    """Test creating two pipelines from a job"""
    q: queue.Queue = queue.Queue()
    def submit_callable(config: Config) -> None:
        q.put(config)
    
    matrix_path: str | os.PathLike[str] = os.path.join(tmpdir, 'matrix.csv')
    pd.DataFrame({
        'var1': [1,2,3],
        'var3': ['b', 'c', 'd']
    }).to_csv(matrix_path, index=False)
    
    # make a file to copy
    src_dir = os.path.join(tmpdir, "a")
    dst_dir = os.path.join(tmpdir, "b")
    src_file = os.path.join(src_dir, "asdf")
    dst_file = os.path.join(dst_dir, "hjk")
    os.mkdir(src_dir)
    src_length: int = random.randint(0, FILE_LENGTH_MAX)
    with open(src_file, "w") as fh:
        fh.write(
            "".join(
                random.choice(string.ascii_uppercase + string.digits)
                for _ in range(src_length)
            )
        )

    input_config = {
        'variables': {
            'a': 1
        },
        'config': {
            'type': 'pipeline',
            'stages': [
                {
                    'type': 'job',
                    'function': 'antz.jobs.create_pipelines_from_matrix.create_pipelines_from_matrix',
                    'parameters': {
                        'matrix_path': matrix_path,
                        'pipeline_config_template': {
                            'type': 'pipeline',
                            'stages': [
                                {
                                    'type': 'job',
                                    'function': 'antz.jobs.copy.copy',
                                    'parameters': {
                                        'source': os.fspath(src_file),
                                        'destination': os.fspath(dst_file)
                                    }
                                }
                            ]
                        }
                    }
                }
            ]
        }
    }

    run_manager(
        Config.model_validate(input_config), 
        submit_callable,
        logging.getLogger('test')
    )

    ret = q.get(timeout=1) 

    expected_config = {
        'variables': {
            'a': 1,
            'var1': 1,
            'var3': 'b'
        },
        'config': {
            'type': 'pipeline',
            'name': 'pipeline_0',
            'id': ret.config.id,
            'stages': [
                {
                    'type': 'job',
                    'id': ret.config.stages[0].id,
                    'function': 'antz.jobs.copy.copy',
                    'parameters': {
                        'source': src_file,
                        'destination': dst_file
                    }
                }
            ]
        }
    }

    assert ret == Config.model_validate(expected_config)

    ret = q.get(timeout=1) 
    expected_config = {
        'variables': {
            'a': 1,
            'var1': 2,
            'var3': 'c'
        },
        'config': {
            'type': 'pipeline',
            'name': 'pipeline_1',
            'id': ret.config.id,
            'stages': [
                {
                    'type': 'job',
                    'id': ret.config.stages[0].id,
                    'function': 'antz.jobs.copy.copy',
                    'parameters': {
                        'source': src_file,
                        'destination': dst_file
                    }
                }
            ]
        }
    }

    assert ret == Config.model_validate(expected_config)

    ret = q.get(timeout=1) 
    expected_config = {
        'variables': {
            'a': 1,
            'var1': 3,
            'var3': 'd'
        },
        'config': {
            'type': 'pipeline',
            'name': 'pipeline_2',
            'id': ret.config.id,
            'stages': [
                {
                    'type': 'job',
                    'id': ret.config.stages[0].id,
                    'function': 'antz.jobs.copy.copy',
                    'parameters': {
                        'source': src_file,
                        'destination': dst_file
                    }
                }
            ]
        }
    }

    assert ret == Config.model_validate(expected_config)
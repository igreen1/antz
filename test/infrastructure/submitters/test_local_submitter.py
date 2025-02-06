"""Test that the local submitter runner works"""

import os
import antz.run


def test_local_submitter(tmpdir) -> None:
    tmpdir = "/home/dominus/tmp"
    dst_file = os.path.join(tmpdir, "end.txt")

    src_file = os.path.join(tmpdir, "start.txt")

    test_text: str = "Hello there general kenobi"

    with open(src_file, "w") as fh:
        fh.write(test_text)

    test_config = {
        "submitter_config": {"type": "local"},
        "analysis_config": {
            "variables": {},
            "config": {
                "type": "pipeline",
                "stages": [
                    {
                        "type": "job",
                        "function": "antz.jobs.copy.copy",
                        "parameters": {
                            "source": os.fspath(src_file),
                            "destination": os.fspath(dst_file),
                        },
                    }
                ],
            },
        },
    }

    antz.run.run(test_config)

    assert os.path.exists(dst_file)
    with open(dst_file, "r") as fh:
        ret = fh.read()
    assert ret == test_text

import os
import sys
import pytest

# Ensure the project root is in sys.path for absolute imports
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from dags.etl_pipeline import extract, create_schema, clean, load, match

@pytest.mark.parametrize("func,expected_calls", [
    (extract, 2),        # extract should call subprocess.run twice
    (create_schema, 1),  # create_schema should call subprocess.run once
    (clean, 2),          # clean should call subprocess.run twice
    (load, 1),           # load should call subprocess.run once
    (match, 1),          # match should call subprocess.run once
])
def test_etl_pipeline_steps_with_mocks(mocker, func, expected_calls):
    # Mock subprocess.run so no real scripts are executed
    mock_run = mocker.patch("subprocess.run")
    # Mock logger to avoid file writes
    mocker.patch("dags.etl_pipeline.get_stage_logger", return_value=mocker.Mock())
    # Mock os.path.exists for extract to always return False (so scripts run)
    if func is extract:
        mocker.patch("os.path.exists", return_value=False)
    func()
    assert mock_run.call_count == expected_calls 
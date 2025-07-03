import os
import sys
import pytest

# Ensure the project root is in sys.path for absolute imports
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from dags.etl_pipeline import extract

def test_extract_with_mocks(mocker):
    # Mock os.path.exists to always return False (simulate files not existing)
    mocker.patch("os.path.exists", return_value=False)
    # Mock subprocess.run to do nothing (simulate script execution)
    mock_run = mocker.patch("subprocess.run")
    # Mock logger to avoid file writes
    mocker.patch("dags.etl_pipeline.get_stage_logger", return_value=mocker.Mock())

    # Call the extract function
    extract()

    # Check that subprocess.run was called for both scripts
    assert mock_run.call_count == 2
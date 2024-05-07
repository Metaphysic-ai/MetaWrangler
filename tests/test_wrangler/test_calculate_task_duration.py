import pytest
from hypothesis import given
import hypothesis.strategies as st
from datetime import datetime
from MetaWrangler import MetaWrangler

# Example-based tests for calculate_task_duration method
def test_calculate_task_duration_valid_input():
    wrangler = MetaWrangler()
    task = {'StartRen': '2024-05-07T12:00:00.000+02:00', 'Comp': '2024-05-07T12:10:00.000+02:00'}
    assert wrangler.calculate_task_duration(task) == 600

def test_calculate_task_duration_invalid_input():
    wrangler = MetaWrangler()
    task = {'StartRen': 'invalid_start_time', 'Comp': 'invalid_completion_time'}
    assert wrangler.calculate_task_duration(task) is None
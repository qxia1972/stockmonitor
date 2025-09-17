#!/usr/bin/env python3
"""
Dagster Repository for Stock Monitor Jobs
"""

from dagster import repository
from modules.orchestration.pipeline_definitions import (
    daily_data_sync_job,
    daily_full_pipeline_job,
    daily_processing_success_monitor,
    daily_processing_failure_monitor,
    daily_full_pipeline_schedule
)

@repository
def stock_monitor_repository():
    """Stock Monitor Repository containing all jobs, schedules, and sensors"""
    return [
        # Jobs
        daily_data_sync_job,
        daily_full_pipeline_job,

        # Schedules
        daily_full_pipeline_schedule,

        # Sensors
        daily_processing_success_monitor,
        daily_processing_failure_monitor,
    ]

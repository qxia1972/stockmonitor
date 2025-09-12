"""Unified configuration for stockmonitor

This file holds tunable defaults for data fetching behaviors such as
minute-chunking and cache refresh flags. Values can be overridden by
environment variables if needed.
"""
from __future__ import annotations

import os

# Chunk size in calendar days when fetching minute-level data in parts
# Default: 30 days per chunk
CHUNK_DAYS = int(os.environ.get('STOCKMONITOR_CHUNK_DAYS', '30'))

# Number of retries for each chunk fetch
CHUNK_RETRIES = int(os.environ.get('STOCKMONITOR_CHUNK_RETRIES', '2'))

# Backoff seconds between chunk retry attempts (float)
CHUNK_BACKOFF_SECONDS = float(os.environ.get('STOCKMONITOR_CHUNK_BACKOFF_SECONDS', '0.5'))

# Global default for forcing refresh from remote even if cache exists
# Can be enabled via environment variable or overridden in code
FORCE_REFRESH_CACHE = os.environ.get('STOCKMONITOR_FORCE_REFRESH_CACHE', 'false').lower() in ('1', 'true', 'yes')

# Export list
__all__ = [
    'CHUNK_DAYS',
    'CHUNK_RETRIES',
    'CHUNK_BACKOFF_SECONDS',
    'FORCE_REFRESH_CACHE',
]

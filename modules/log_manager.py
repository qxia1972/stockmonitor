#!/usr/bin/env python3
"""
Unified Log Manager

Provides consistent logging format and configuration management for all project files.
Offers pre-configured logger instances to ensure unified log output format across the project.
"""

import logging
import sys
from pathlib import Path
from typing import Optional

def setup_logger(
    name: str = __name__,
    log_level: int = logging.INFO,
    log_file: Optional[str] = None,
    console_output: bool = True
) -> logging.Logger:
    """
    Setup logger with unified format
    
    Args:
        name: Logger name, typically use __name__
        log_level: Log level, default INFO
        log_file: Log file name, default no file output
        console_output: Whether to output to console, default True
        
    Returns:
        Configured logger instance
    """
    # Create logs subdirectory
    logs_dir = Path('logs')
    logs_dir.mkdir(exist_ok=True)
    
    # Get or create logger
    logger = logging.getLogger(name)
    
    # Avoid duplicate handler addition
    if logger.handlers:
        return logger
        
    logger.setLevel(log_level)
    
    # Unified log format - aligned format
    formatter = logging.Formatter(
        '%(asctime)s - %(name)-12s - %(levelname)-8s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # Console handler
    if console_output:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        console_handler.setLevel(logging.INFO)
        logger.addHandler(console_handler)
    
    # File handler
    if log_file:
        try:
            file_handler = logging.FileHandler(
                logs_dir / log_file,
                encoding='utf-8-sig',
                mode='a'
            )
            file_handler.setFormatter(formatter)
            file_handler.setLevel(logging.DEBUG)
            logger.addHandler(file_handler)
        except Exception as e:
            # Fallback to standard UTF-8
            file_handler = logging.FileHandler(
                logs_dir / log_file,
                encoding='utf-8',
                mode='a'
            )
            file_handler.setFormatter(formatter)
            file_handler.setLevel(logging.DEBUG)
            logger.addHandler(file_handler)
    
    return logger

# Pre-defined logger configurations
def get_stockpool_logger() -> logging.Logger:
    """Get stock pool manager logger"""
    return setup_logger('stockpool', logging.DEBUG, 'stockpool.log')

def get_monitor_logger() -> logging.Logger:
    """Get monitoring tool logger"""
    return setup_logger('monitor', logging.INFO, 'monitor.log')

def get_system_logger() -> logging.Logger:
    """Get system startup logger"""
    return setup_logger('system', logging.INFO, 'system.log')

def get_datastore_logger() -> logging.Logger:
    """Get data store logger"""
    return setup_logger('datastore', logging.INFO, 'datastore.log')

def get_tool_logger() -> logging.Logger:
    """Get tool class logger"""
    return setup_logger('tool', logging.INFO, 'tool.log')

# General-purpose logger for simple scenarios
def get_default_logger(name: str = 'default') -> logging.Logger:
    """Get default logger, console output only"""
    return setup_logger(name, logging.INFO, None, True)

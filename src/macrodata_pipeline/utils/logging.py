"""Logging utilities for the Macroeconomic Data Pipeline.

This module provides standardized logging configuration and utilities
for consistent logging across the pipeline.
"""

import os
import logging
from pathlib import Path
from typing import Optional

def get_logger(name: str, log_file: Optional[str] = None) -> logging.Logger:
    """Get a logger with standardized configuration.
    
    Args:
        name: Name of the logger (typically __name__)
        log_file: Optional path to log file. If not provided, logs to console only.
        
    Returns:
        Configured logger instance
    """
    logger = logging.getLogger(name)
    
    # Only configure if not already configured
    if not logger.handlers:
        logger.setLevel(logging.INFO)
        
        # Create formatters
        console_formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        file_formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        
        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(console_formatter)
        logger.addHandler(console_handler)
        
        # File handler if log_file is provided
        if log_file:
            # Create log directory if it doesn't exist
            log_dir = Path(log_file).parent
            log_dir.mkdir(parents=True, exist_ok=True)
            
            file_handler = logging.FileHandler(log_file)
            file_handler.setFormatter(file_formatter)
            logger.addHandler(file_handler)
    
    return logger

def set_log_level(logger: logging.Logger, level: int) -> None:
    """Set the logging level for a logger.
    
    Args:
        logger: Logger instance to configure
        level: Logging level to set
    """
    logger.setLevel(level)
    for handler in logger.handlers:
        handler.setLevel(level) 
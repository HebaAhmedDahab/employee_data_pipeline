"""
Logging Utility Module
Provides consistent logging configuration across the pipeline
"""

import logging
import os
from datetime import datetime
from pathlib import Path

class PipelineLogger:
    """Custom logger for the data pipeline"""
    
    def __init__(self, name, log_level=logging.INFO):
        self.logger = logging.getLogger(name)
        self.logger.setLevel(log_level)
        
        # Create logs directory if it doesn't exist
        log_dir = Path(__file__).parent.parent.parent / 'logs'
        log_dir.mkdir(exist_ok=True)
        
        # Create log file with timestamp
        log_file = log_dir / f"pipeline_{datetime.now().strftime('%Y%m%d')}.log"
        
        # File handler
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(log_level)
        
        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(log_level)
        
        # Formatter
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)
        
        # Add handlers if not already added
        if not self.logger.handlers:
            self.logger.addHandler(file_handler)
            self.logger.addHandler(console_handler)
    
    def get_logger(self):
        """Return the configured logger"""
        return self.logger


def get_logger(name):
    """
    Convenience function to get a logger
    
    Args:
        name (str): Name of the logger (usually __name__)
    
    Returns:
        logging.Logger: Configured logger instance
    """
    pipeline_logger = PipelineLogger(name)
    return pipeline_logger.get_logger()


if __name__ == "__main__":
    # Test the logger
    logger = get_logger(__name__)
    logger.info("This is an info message")
    logger.warning("This is a warning message")
    logger.error("This is an error message")
    print("✅ Logger test complete. Check logs/ directory")

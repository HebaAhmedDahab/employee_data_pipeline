"""
Data Quality Checker Module
Validates data quality at each pipeline stage
"""

import pandas as pd
from pathlib import Path
import sys

# Add parent directory to path
sys.path.append(str(Path(__file__).parent.parent.parent))
from src.utils.logger import get_logger

logger = get_logger(__name__)


class DataQualityChecker:
    """Class for performing data quality checks"""
    
    def __init__(self, dataframe, table_name):
        self.df = dataframe
        self.table_name = table_name
        self.issues = []
    
    def check_null_values(self):
        """Check for null values in the dataframe"""
        null_counts = self.df.isnull().sum()
        null_columns = null_counts[null_counts > 0]
        
        if not null_columns.empty:
            for col, count in null_columns.items():
                percentage = (count / len(self.df)) * 100
                message = f"Column '{col}' has {count} null values ({percentage:.2f}%)"
                self.issues.append(message)
                logger.warning(f"[{self.table_name}] {message}")
        
        return null_columns
    
    def check_duplicates(self):
        """Check for duplicate rows"""
        duplicate_count = self.df.duplicated().sum()
        
        if duplicate_count > 0:
            message = f"Found {duplicate_count} duplicate rows"
            self.issues.append(message)
            logger.warning(f"[{self.table_name}] {message}")
        
        return duplicate_count
    
    def check_row_count(self, expected_min=1):
        """Check if dataframe has minimum expected rows"""
        row_count = len(self.df)
        
        if row_count < expected_min:
            message = f"Row count ({row_count}) is below expected minimum ({expected_min})"
            self.issues.append(message)
            logger.error(f"[{self.table_name}] {message}")
            return False
        
        logger.info(f"[{self.table_name}] Row count: {row_count} ✅")
        return True
    
    def check_data_types(self):
        """Log data types of all columns"""
        logger.info(f"[{self.table_name}] Data types:")
        for col, dtype in self.df.dtypes.items():
            logger.info(f"  - {col}: {dtype}")
    
    def run_all_checks(self):
        """Run all quality checks and return summary"""
        logger.info(f"Starting data quality checks for {self.table_name}")
        
        # Run checks
        self.check_row_count()
        self.check_null_values()
        self.check_duplicates()
        self.check_data_types()
        
        # Summary
        if not self.issues:
            logger.info(f"[{self.table_name}] ✅ All quality checks passed!")
            return True
        else:
            logger.warning(f"[{self.table_name}] ⚠️  Found {len(self.issues)} quality issues")
            return False
    
    def get_summary(self):
        """Return a summary dictionary"""
        return {
            'table': self.table_name,
            'row_count': len(self.df),
            'column_count': len(self.df.columns),
            'null_count': self.df.isnull().sum().sum(),
            'duplicate_count': self.df.duplicated().sum(),
            'issues': self.issues
        }


def validate_dataframe(df, table_name):
    """
    Convenience function to validate a dataframe
    
    Args:
        df (pd.DataFrame): DataFrame to validate
        table_name (str): Name of the table for logging
    
    Returns:
        dict: Quality check summary
    """
    checker = DataQualityChecker(df, table_name)
    checker.run_all_checks()
    return checker.get_summary()


if __name__ == "__main__":
    # Test with sample data
    test_df = pd.DataFrame({
        'id': [1, 2, 3, 2],
        'name': ['John', 'Jane', None, 'Jane'],
        'salary': [50000, 60000, 70000, 60000]
    })
    
    summary = validate_dataframe(test_df, 'test_table')
    print("\n=== Quality Check Summary ===")
    for key, value in summary.items():
        print(f"{key}: {value}")

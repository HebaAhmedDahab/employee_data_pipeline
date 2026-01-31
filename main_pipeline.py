"""
Main Pipeline Orchestrator
Executes the complete ETL pipeline: Extract -> Transform -> Load
"""

import sys
from pathlib import Path
from datetime import datetime

# Add parent directory to path
sys.path.append(str(Path(__file__).parent.parent))

from config.db_config import db_config
from src.extract.extract_employees import EmployeeExtractor
from src.extract.extract_departments import DepartmentExtractor
from src.transform.transform_employees import EmployeeTransformer
from src.load.load_to_gold import EmployeeAnalyticsLoader
from src.utils.logger import get_logger

logger = get_logger(__name__)


class DataPipeline:
    """Main pipeline orchestrator class"""
    
    def __init__(self):
        self.pipeline_name = "Employee Data Pipeline"
        self.start_time = None
        self.end_time = None
        self.status = "Not Started"
        self.errors = []
    
    def test_connection(self):
        """
        Test database connection before starting pipeline
        
        Returns:
            bool: True if connection successful
        """
        logger.info("Testing database connection...")
        
        try:
            if db_config.test_connection():
                logger.info("Database connection successful")
                return True
            else:
                logger.error("Database connection failed")
                return False
        except Exception as e:
            logger.error(f"Connection test error: {str(e)}")
            return False
    
    def extract_phase(self):
        """
        Execute extraction phase
        
        Returns:
            bool: True if successful
        """
        logger.info("\n" + "="*60)
        logger.info("PHASE 1: EXTRACTION")
        logger.info("="*60)
        
        try:
            # Extract employees
            employee_extractor = EmployeeExtractor()
            emp_df, emp_file = employee_extractor.run()
            logger.info(f"Employees extracted: {len(emp_df)} rows")
            
            # Extract departments
            dept_extractor = DepartmentExtractor()
            dept_df, dept_file = dept_extractor.run()
            logger.info(f"Departments extracted: {len(dept_df)} rows")
            
            logger.info("="*60)
            logger.info("EXTRACTION PHASE COMPLETED SUCCESSFULLY")
            logger.info("="*60 + "\n")
            
            return True
            
        except Exception as e:
            logger.error(f"Extraction phase failed: {str(e)}")
            self.errors.append(f"Extraction: {str(e)}")
            return False
    
    def transform_phase(self):
        """
        Execute transformation phase
        
        Returns:
            bool: True if successful
        """
        logger.info("\n" + "="*60)
        logger.info("PHASE 2: TRANSFORMATION")
        logger.info("="*60)
        
        try:
            # Transform employees
            transformer = EmployeeTransformer()
            transformed_df, transformed_file = transformer.run(filter_active=False)
            logger.info(f"Employees transformed: {len(transformed_df)} rows")
            
            logger.info("="*60)
            logger.info("TRANSFORMATION PHASE COMPLETED SUCCESSFULLY")
            logger.info("="*60 + "\n")
            
            return True
            
        except Exception as e:
            logger.error(f"Transformation phase failed: {str(e)}")
            self.errors.append(f"Transformation: {str(e)}")
            return False
    
    def load_phase(self):
        """
        Execute load phase
        
        Returns:
            bool: True if successful
        """
        logger.info("\n" + "="*60)
        logger.info("PHASE 3: LOAD TO GOLD LAYER")
        logger.info("="*60)
        
        try:
            # Load to gold layer
            loader = EmployeeAnalyticsLoader()
            saved_files, analytics = loader.run()
            logger.info(f"Analytics tables created: {len(saved_files)}")
            
            logger.info("="*60)
            logger.info("LOAD PHASE COMPLETED SUCCESSFULLY")
            logger.info("="*60 + "\n")
            
            return True
            
        except Exception as e:
            logger.error(f"Load phase failed: {str(e)}")
            self.errors.append(f"Load: {str(e)}")
            return False
    
    def run(self):
        """
        Execute the complete pipeline
        
        Returns:
            dict: Pipeline execution summary
        """
        self.start_time = datetime.now()
        self.status = "Running"
        
        logger.info("\n" + "="*60)
        logger.info(f"STARTING PIPELINE: {self.pipeline_name}")
        logger.info(f"Start Time: {self.start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info("="*60)
        
        # Test connection
        if not self.test_connection():
            self.status = "Failed"
            logger.error("Pipeline aborted: Database connection failed")
            return self.get_summary()
        
        # Execute phases
        phases = [
            ("Extract", self.extract_phase),
            ("Transform", self.transform_phase),
            ("Load", self.load_phase)
        ]
        
        for phase_name, phase_func in phases:
            success = phase_func()
            
            if not success:
                self.status = "Failed"
                logger.error(f"Pipeline failed at {phase_name} phase")
                break
        else:
            self.status = "Completed Successfully"
        
        self.end_time = datetime.now()
        
        # Print summary
        self.print_summary()
        
        return self.get_summary()
    
    def get_summary(self):
        """
        Get pipeline execution summary
        
        Returns:
            dict: Summary dictionary
        """
        duration = None
        if self.start_time and self.end_time:
            duration = (self.end_time - self.start_time).total_seconds()
        
        return {
            'pipeline_name': self.pipeline_name,
            'status': self.status,
            'start_time': self.start_time,
            'end_time': self.end_time,
            'duration_seconds': duration,
            'errors': self.errors
        }
    
    def print_summary(self):
        """Print pipeline execution summary"""
        summary = self.get_summary()
        
        logger.info("\n" + "="*60)
        logger.info("PIPELINE EXECUTION SUMMARY")
        logger.info("="*60)
        logger.info(f"Pipeline Name: {summary['pipeline_name']}")
        logger.info(f"Status: {summary['status']}")
        
        if summary['start_time']:
            logger.info(f"Start Time: {summary['start_time'].strftime('%Y-%m-%d %H:%M:%S')}")
        
        if summary['end_time']:
            logger.info(f"End Time: {summary['end_time'].strftime('%Y-%m-%d %H:%M:%S')}")
        
        if summary['duration_seconds']:
            logger.info(f"Duration: {summary['duration_seconds']:.2f} seconds")
        
        if summary['errors']:
            logger.info("\nErrors encountered:")
            for error in summary['errors']:
                logger.info(f"  - {error}")
        else:
            logger.info("\n No errors encountered")
        
        logger.info("="*60 + "\n")
        
        # Print success message with emoji
        if summary['status'] == "Completed Successfully":
            logger.info("PIPELINE COMPLETED SUCCESSFULLY!")
            logger.info("\nYou can find your data in:")
            logger.info("Bronze Layer: data/bronze/")
            logger.info("Silver Layer: data/silver/")
            logger.info("Gold Layer: data/gold/")
        else:
            logger.error("PIPELINE FAILED")
            logger.info("\nCheck the logs above for error details")


def main():
    """Main execution function"""
    pipeline = DataPipeline()
    summary = pipeline.run()
    
    # Return exit code based on status
    if summary['status'] == "Completed Successfully":
        return 0
    else:
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)

"""
Extract Module - DimEmployee
Extracts employee data from SQL Server to Bronze layer
"""

import pandas as pd
from pathlib import Path
import sys
from datetime import datetime

# Add parent directory to path
sys.path.append(str(Path(__file__).parent.parent.parent))
from config.db_config import db_config
from src.utils.logger import get_logger
from src.utils.data_quality import validate_dataframe

logger = get_logger(__name__)


class EmployeeExtractor:
    """Class to handle employee data extraction"""
    
    def __init__(self):
        self.table_name = 'DimEmployee'
        self.output_dir = Path(__file__).parent.parent.parent / 'data' / 'bronze'
        self.output_dir.mkdir(parents=True, exist_ok=True)
    
    def extract(self):
        """
        Extract employee data from SQL Server
        
        Returns:
            pd.DataFrame: Extracted employee data
        """
        logger.info(f"Starting extraction of {self.table_name}")
        
        try:
            # Get database engine
            engine = db_config.get_sqlalchemy_engine()
            
            # SQL Query
            query = f"""
            SELECT 
                EmployeeKey,
                ParentEmployeeKey,
                EmployeeNationalIDAlternateKey,
                ParentEmployeeNationalIDAlternateKey,
                SalesTerritoryKey,
                FirstName,
                LastName,
                MiddleName,
                NameStyle,
                Title,
                HireDate,
                BirthDate,
                LoginID,
                EmailAddress,
                Phone,
                MaritalStatus,
                EmergencyContactName,
                EmergencyContactPhone,
                SalariedFlag,
                Gender,
                PayFrequency,
                BaseRate,
                VacationHours,
                SickLeaveHours,
                CurrentFlag,
                SalesPersonFlag,
                DepartmentName,
                StartDate,
                EndDate,
                Status
            FROM dbo.{self.table_name}
            """
            
            # Execute query
            df = pd.read_sql(query, engine)
            
            logger.info(f"✅ Successfully extracted {len(df)} rows from {self.table_name}")
            
            # Validate data quality
            validate_dataframe(df, self.table_name)
            
            return df
            
        except Exception as e:
            logger.error(f"❌ Failed to extract {self.table_name}: {str(e)}")
            raise
    
    def save_to_bronze(self, df):
        """
        Save extracted data to bronze layer
        
        Args:
            df (pd.DataFrame): Data to save
        
        Returns:
            str: Path to saved file
        """
        try:
            # Add extraction metadata
            df['extraction_timestamp'] = datetime.now()
            
            # File path with timestamp
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"{self.table_name.lower()}_{timestamp}.csv"
            filepath = self.output_dir / filename
            
            # Also save a "latest" version without timestamp
            latest_filepath = self.output_dir / f"{self.table_name.lower()}_latest.csv"
            
            # Save files
            df.to_csv(filepath, index=False)
            df.to_csv(latest_filepath, index=False)
            
            logger.info(f"✅ Saved to bronze layer: {filepath}")
            logger.info(f"✅ Saved latest version: {latest_filepath}")
            
            return str(filepath)
            
        except Exception as e:
            logger.error(f"❌ Failed to save to bronze layer: {str(e)}")
            raise
    
    def run(self):
        """
        Execute the complete extraction process
        
        Returns:
            tuple: (DataFrame, file_path)
        """
        logger.info("="*50)
        logger.info(f"EXTRACTION STARTED: {self.table_name}")
        logger.info("="*50)
        
        try:
            # Extract data
            df = self.extract()
            
            # Save to bronze
            filepath = self.save_to_bronze(df)
            
            logger.info("="*50)
            logger.info(f"EXTRACTION COMPLETED: {self.table_name}")
            logger.info(f"Rows extracted: {len(df)}")
            logger.info(f"File saved: {filepath}")
            logger.info("="*50)
            
            return df, filepath
            
        except Exception as e:
            logger.error("="*50)
            logger.error(f"EXTRACTION FAILED: {self.table_name}")
            logger.error(f"Error: {str(e)}")
            logger.error("="*50)
            raise


def main():
    """Main execution function"""
    extractor = EmployeeExtractor()
    df, filepath = extractor.run()
    
    # Display sample data
    print("\n=== Sample Data (First 5 rows) ===")
    print(df.head())
    print(f"\n=== Data Shape: {df.shape} ===")


if __name__ == "__main__":
    main()

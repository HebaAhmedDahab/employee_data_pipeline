"""
Transform Module - Employee Data
Transforms employee data from Bronze to Silver layer
Performs data cleaning, type conversion, and standardization
"""

import pandas as pd
from pathlib import Path
import sys
from datetime import datetime

# Add parent directory to path
sys.path.append(str(Path(__file__).parent.parent.parent))
from src.utils.logger import get_logger
from src.utils.data_quality import validate_dataframe

logger = get_logger(__name__)


class EmployeeTransformer:
    """Class to handle employee data transformation"""
    
    def __init__(self):
        self.table_name = 'employees'
        self.bronze_dir = Path(__file__).parent.parent.parent / 'data' / 'bronze'
        self.silver_dir = Path(__file__).parent.parent.parent / 'data' / 'silver'
        self.silver_dir.mkdir(parents=True, exist_ok=True)
    
    def load_from_bronze(self):
        """
        Load latest employee data from bronze layer
        
        Returns:
            pd.DataFrame: Raw employee data
        """
        try:
            filepath = self.bronze_dir / 'dimemployee_latest.csv'
            
            if not filepath.exists():
                raise FileNotFoundError(f"Bronze file not found: {filepath}")
            
            df = pd.read_csv(filepath)
            logger.info(f"✅ Loaded {len(df)} rows from bronze layer")
            
            return df
            
        except Exception as e:
            logger.error(f"❌ Failed to load from bronze: {str(e)}")
            raise
    
    def clean_data(self, df):
        """
        Clean and standardize employee data
        
        Args:
            df (pd.DataFrame): Raw data
        
        Returns:
            pd.DataFrame: Cleaned data
        """
        logger.info("Starting data cleaning...")
        
        df_clean = df.copy()
        
        # 1. Remove extraction timestamp (metadata column)
        if 'extraction_timestamp' in df_clean.columns:
            df_clean = df_clean.drop('extraction_timestamp', axis=1)
        
        # 2. Handle null values in name fields
        df_clean['MiddleName'] = df_clean['MiddleName'].fillna('')
        df_clean['Title'] = df_clean['Title'].fillna('Not Specified')
        
        # 3. Standardize text fields (trim whitespace, title case)
        text_columns = ['FirstName', 'LastName', 'MiddleName', 'Title', 
                       'DepartmentName', 'EmergencyContactName']
        
        for col in text_columns:
            if col in df_clean.columns:
                df_clean[col] = df_clean[col].astype(str).str.strip()
        
        # 4. Create full name field
        df_clean['FullName'] = (
            df_clean['FirstName'] + ' ' + 
            df_clean['MiddleName'].apply(lambda x: x + ' ' if x else '') + 
            df_clean['LastName']
        ).str.strip()
        
        # 5. Convert date columns to datetime
        date_columns = ['HireDate', 'BirthDate', 'StartDate', 'EndDate']
        for col in date_columns:
            if col in df_clean.columns:
                df_clean[col] = pd.to_datetime(df_clean[col], errors='coerce')
        
        # 6. Calculate derived fields
        if 'BirthDate' in df_clean.columns:
            today = pd.Timestamp.now()
            df_clean['Age'] = (today - df_clean['BirthDate']).dt.days // 365
        
        if 'HireDate' in df_clean.columns:
            today = pd.Timestamp.now()
            df_clean['YearsOfService'] = (today - df_clean['HireDate']).dt.days // 365
        
        # 7. Standardize boolean fields
        boolean_columns = ['SalariedFlag', 'CurrentFlag', 'SalesPersonFlag']
        for col in boolean_columns:
            if col in df_clean.columns:
                df_clean[col] = df_clean[col].astype(bool)
        
        # 8. Handle numeric fields
        numeric_columns = ['BaseRate', 'VacationHours', 'SickLeaveHours']
        for col in numeric_columns:
            if col in df_clean.columns:
                df_clean[col] = pd.to_numeric(df_clean[col], errors='coerce').fillna(0)
        
        # 9. Standardize gender and marital status
        if 'Gender' in df_clean.columns:
            gender_map = {'M': 'Male', 'F': 'Female'}
            df_clean['Gender'] = df_clean['Gender'].map(gender_map)
        
        if 'MaritalStatus' in df_clean.columns:
            marital_map = {'M': 'Married', 'S': 'Single'}
            df_clean['MaritalStatus'] = df_clean['MaritalStatus'].map(marital_map)
        
        # 10. Add data quality flag
        df_clean['data_quality_score'] = 100
        
        # Reduce score for missing critical fields
        critical_fields = ['EmailAddress', 'Phone', 'DepartmentName']
        for field in critical_fields:
            if field in df_clean.columns:
                df_clean.loc[df_clean[field].isna(), 'data_quality_score'] -= 10
        
        logger.info("✅ Data cleaning completed")
        
        return df_clean
    
    def remove_duplicates(self, df):
        """
        Remove duplicate records
        
        Args:
            df (pd.DataFrame): Data to deduplicate
        
        Returns:
            pd.DataFrame: Deduplicated data
        """
        initial_count = len(df)
        
        # Remove duplicates based on EmployeeKey
        df_dedup = df.drop_duplicates(subset=['EmployeeKey'], keep='last')
        
        removed_count = initial_count - len(df_dedup)
        
        if removed_count > 0:
            logger.warning(f"⚠️  Removed {removed_count} duplicate records")
        else:
            logger.info("✅ No duplicates found")
        
        return df_dedup
    
    def filter_active_employees(self, df):
        """
        Filter for active employees only (optional step)
        
        Args:
            df (pd.DataFrame): Employee data
        
        Returns:
            pd.DataFrame: Filtered data
        """
        if 'CurrentFlag' in df.columns:
            active_df = df[df['CurrentFlag'] == True].copy()
            logger.info(f"Filtered to {len(active_df)} active employees (from {len(df)} total)")
            return active_df
        else:
            logger.warning("CurrentFlag column not found, skipping active filter")
            return df
    
    def save_to_silver(self, df):
        """
        Save transformed data to silver layer
        
        Args:
            df (pd.DataFrame): Transformed data
        
        Returns:
            str: Path to saved file
        """
        try:
            # Add transformation metadata
            df['transformation_timestamp'] = datetime.now()
            
            # File path with timestamp
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"{self.table_name}_{timestamp}.csv"
            filepath = self.silver_dir / filename
            
            # Also save a "latest" version
            latest_filepath = self.silver_dir / f"{self.table_name}_latest.csv"
            
            # Save files
            df.to_csv(filepath, index=False)
            df.to_csv(latest_filepath, index=False)
            
            logger.info(f"✅ Saved to silver layer: {filepath}")
            logger.info(f"✅ Saved latest version: {latest_filepath}")
            
            return str(filepath)
            
        except Exception as e:
            logger.error(f"❌ Failed to save to silver layer: {str(e)}")
            raise
    
    def run(self, filter_active=False):
        """
        Execute the complete transformation process
        
        Args:
            filter_active (bool): Whether to filter for active employees only
        
        Returns:
            tuple: (DataFrame, file_path)
        """
        logger.info("="*50)
        logger.info(f"TRANSFORMATION STARTED: {self.table_name}")
        logger.info("="*50)
        
        try:
            # Load from bronze
            df = self.load_from_bronze()
            
            # Clean data
            df_clean = self.clean_data(df)
            
            # Remove duplicates
            df_dedup = self.remove_duplicates(df_clean)
            
            # Optional: Filter active employees
            if filter_active:
                df_final = self.filter_active_employees(df_dedup)
            else:
                df_final = df_dedup
            
            # Validate quality
            validate_dataframe(df_final, self.table_name)
            
            # Save to silver
            filepath = self.save_to_silver(df_final)
            
            logger.info("="*50)
            logger.info(f"TRANSFORMATION COMPLETED: {self.table_name}")
            logger.info(f"Input rows: {len(df)}")
            logger.info(f"Output rows: {len(df_final)}")
            logger.info(f"File saved: {filepath}")
            logger.info("="*50)
            
            return df_final, filepath
            
        except Exception as e:
            logger.error("="*50)
            logger.error(f"TRANSFORMATION FAILED: {self.table_name}")
            logger.error(f"Error: {str(e)}")
            logger.error("="*50)
            raise


def main():
    """Main execution function"""
    transformer = EmployeeTransformer()
    df, filepath = transformer.run(filter_active=False)
    
    # Display sample data
    print("\n=== Transformed Data (First 5 rows) ===")
    print(df[['EmployeeKey', 'FullName', 'DepartmentName', 'Age', 
              'YearsOfService', 'CurrentFlag']].head())
    print(f"\n=== Data Shape: {df.shape} ===")


if __name__ == "__main__":
    main()

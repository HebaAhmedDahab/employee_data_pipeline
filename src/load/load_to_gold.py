"""
Load Module - Employee Analytics
Aggregates employee data from Silver to Gold layer
Creates business-ready analytics tables
"""

import pandas as pd
from pathlib import Path
import sys
from datetime import datetime

# Add parent directory to path
sys.path.append(str(Path(__file__).parent.parent.parent))
from src.utils.logger import get_logger

logger = get_logger(__name__)


class EmployeeAnalyticsLoader:
    """Class to create analytics-ready employee data in Gold layer"""
    
    def __init__(self):
        self.silver_dir = Path(__file__).parent.parent.parent / 'data' / 'silver'
        self.gold_dir = Path(__file__).parent.parent.parent / 'data' / 'gold'
        self.gold_dir.mkdir(parents=True, exist_ok=True)
    
    def load_from_silver(self):
        """
        Load transformed employee data from silver layer
        
        Returns:
            pd.DataFrame: Transformed employee data
        """
        try:
            filepath = self.silver_dir / 'employees_latest.csv'
            
            if not filepath.exists():
                raise FileNotFoundError(f"Silver file not found: {filepath}")
            
            df = pd.read_csv(filepath)
            
            # Convert date columns back to datetime
            date_columns = ['HireDate', 'BirthDate', 'StartDate', 'EndDate', 
                          'transformation_timestamp']
            for col in date_columns:
                if col in df.columns:
                    df[col] = pd.to_datetime(df[col], errors='coerce')
            
            logger.info(f"✅ Loaded {len(df)} rows from silver layer")
            
            return df
            
        except Exception as e:
            logger.error(f"❌ Failed to load from silver: {str(e)}")
            raise
    
    def create_department_summary(self, df):
        """
        Create department-level summary statistics
        
        Args:
            df (pd.DataFrame): Employee data
        
        Returns:
            pd.DataFrame: Department summary
        """
        logger.info("Creating department summary...")
        
        summary = df.groupby('DepartmentName').agg({
            'EmployeeKey': 'count',
            'BaseRate': ['mean', 'median', 'min', 'max'],
            'YearsOfService': 'mean',
            'Age': 'mean',
            'VacationHours': 'mean',
            'SickLeaveHours': 'mean'
        }).round(2)
        
        # Flatten column names
        summary.columns = ['_'.join(col).strip() for col in summary.columns.values]
        summary = summary.rename(columns={'EmployeeKey_count': 'total_employees'})
        summary = summary.reset_index()
        
        # Calculate additional metrics
        summary['avg_base_rate'] = summary['BaseRate_mean']
        summary['avg_years_of_service'] = summary['YearsOfService_mean'].round(1)
        summary['avg_age'] = summary['Age_mean'].round(1)
        
        # Keep only relevant columns
        summary = summary[[
            'DepartmentName', 'total_employees', 'avg_base_rate',
            'avg_years_of_service', 'avg_age', 'BaseRate_min', 'BaseRate_max'
        ]]
        
        logger.info(f"✅ Created summary for {len(summary)} departments")
        
        return summary
    
    def create_gender_diversity_report(self, df):
        """
        Create gender diversity analysis
        
        Args:
            df (pd.DataFrame): Employee data
        
        Returns:
            pd.DataFrame: Gender diversity report
        """
        logger.info("Creating gender diversity report...")
        
        if 'Gender' not in df.columns:
            logger.warning("Gender column not found, skipping diversity report")
            return None
        
        diversity = df.groupby(['DepartmentName', 'Gender']).agg({
            'EmployeeKey': 'count'
        }).reset_index()
        
        diversity = diversity.rename(columns={'EmployeeKey': 'employee_count'})
        
        # Calculate percentages within each department
        total_by_dept = diversity.groupby('DepartmentName')['employee_count'].sum()
        diversity['percentage'] = diversity.apply(
            lambda row: (row['employee_count'] / total_by_dept[row['DepartmentName']] * 100).round(2),
            axis=1
        )
        
        logger.info(f"✅ Created diversity report with {len(diversity)} records")
        
        return diversity
    
    def create_tenure_analysis(self, df):
        """
        Create employee tenure analysis
        
        Args:
            df (pd.DataFrame): Employee data
        
        Returns:
            pd.DataFrame: Tenure analysis
        """
        logger.info("Creating tenure analysis...")
        
        if 'YearsOfService' not in df.columns:
            logger.warning("YearsOfService column not found, skipping tenure analysis")
            return None
        
        # Create tenure bands
        df_tenure = df.copy()
        df_tenure['tenure_band'] = pd.cut(
            df_tenure['YearsOfService'],
            bins=[-1, 1, 3, 5, 10, 100],
            labels=['0-1 years', '1-3 years', '3-5 years', '5-10 years', '10+ years']
        )
        
        tenure_summary = df_tenure.groupby(['DepartmentName', 'tenure_band']).agg({
            'EmployeeKey': 'count'
        }).reset_index()
        
        tenure_summary = tenure_summary.rename(columns={'EmployeeKey': 'employee_count'})
        
        logger.info(f"✅ Created tenure analysis with {len(tenure_summary)} records")
        
        return tenure_summary
    
    def create_hiring_trends(self, df):
        """
        Create hiring trends by year
        
        Args:
            df (pd.DataFrame): Employee data
        
        Returns:
            pd.DataFrame: Hiring trends
        """
        logger.info("Creating hiring trends...")
        
        if 'HireDate' not in df.columns:
            logger.warning("HireDate column not found, skipping hiring trends")
            return None
        
        df_hire = df.copy()
        df_hire['hire_year'] = df_hire['HireDate'].dt.year
        
        hiring_trends = df_hire.groupby(['hire_year', 'DepartmentName']).agg({
            'EmployeeKey': 'count'
        }).reset_index()
        
        hiring_trends = hiring_trends.rename(columns={'EmployeeKey': 'new_hires'})
        hiring_trends = hiring_trends.sort_values(['hire_year', 'new_hires'], ascending=[False, False])
        
        logger.info(f"✅ Created hiring trends with {len(hiring_trends)} records")
        
        return hiring_trends
    
    def save_to_gold(self, dataframes):
        """
        Save all analytics tables to gold layer
        
        Args:
            dataframes (dict): Dictionary of dataframe name -> dataframe
        
        Returns:
            dict: Paths to saved files
        """
        saved_files = {}
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        for name, df in dataframes.items():
            if df is not None and not df.empty:
                try:
                    # Save with timestamp
                    filename = f"{name}_{timestamp}.csv"
                    filepath = self.gold_dir / filename
                    df.to_csv(filepath, index=False)
                    
                    # Save latest version
                    latest_filepath = self.gold_dir / f"{name}_latest.csv"
                    df.to_csv(latest_filepath, index=False)
                    
                    logger.info(f"✅ Saved {name} to gold layer: {filepath}")
                    saved_files[name] = str(filepath)
                    
                except Exception as e:
                    logger.error(f"❌ Failed to save {name}: {str(e)}")
        
        return saved_files
    
    def run(self):
        """
        Execute the complete load process
        
        Returns:
            dict: Paths to saved analytics files
        """
        logger.info("="*50)
        logger.info("LOAD TO GOLD LAYER STARTED")
        logger.info("="*50)
        
        try:
            # Load from silver
            df = self.load_from_silver()
            
            # Create analytics tables
            analytics = {
                'department_summary': self.create_department_summary(df),
                'gender_diversity': self.create_gender_diversity_report(df),
                'tenure_analysis': self.create_tenure_analysis(df),
                'hiring_trends': self.create_hiring_trends(df)
            }
            
            # Save to gold
            saved_files = self.save_to_gold(analytics)
            
            logger.info("="*50)
            logger.info("LOAD TO GOLD LAYER COMPLETED")
            logger.info(f"Analytics tables created: {len(saved_files)}")
            for name, path in saved_files.items():
                logger.info(f"  - {name}: {path}")
            logger.info("="*50)
            
            return saved_files, analytics
            
        except Exception as e:
            logger.error("="*50)
            logger.error("LOAD TO GOLD LAYER FAILED")
            logger.error(f"Error: {str(e)}")
            logger.error("="*50)
            raise


def main():
    """Main execution function"""
    loader = EmployeeAnalyticsLoader()
    saved_files, analytics = loader.run()
    
    # Display summaries
    print("\n=== DEPARTMENT SUMMARY ===")
    if 'department_summary' in analytics and analytics['department_summary'] is not None:
        print(analytics['department_summary'])
    
    print("\n=== GENDER DIVERSITY ===")
    if 'gender_diversity' in analytics and analytics['gender_diversity'] is not None:
        print(analytics['gender_diversity'].head(10))
    
    print("\n=== Analytics files created ===")
    for name in saved_files:
        print(f"✅ {name}")


if __name__ == "__main__":
    main()

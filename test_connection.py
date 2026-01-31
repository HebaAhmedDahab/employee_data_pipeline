"""
Test Connection Script
Quick script to verify SQL Server connectivity
"""

import sys
from pathlib import Path

# Add parent directory to path
sys.path.append(str(Path(__file__).parent))

from config.db_config import db_config


def main():
    """Test database connection and display basic information"""
    
    print("="*60)
    print("SQL SERVER CONNECTION TEST")
    print("="*60)
    print()
    
    # Display configuration
    print("Configuration:")
    print(f"  Server: {db_config.server}")
    print(f"  Database: {db_config.database}")
    print(f"  Driver: {db_config.driver}")
    print(f"  Authentication: Windows Authentication")
    print()
    
    # Test connection
    print("Testing connection...")
    print()
    
    try:
        conn = db_config.get_pyodbc_connection()
        cursor = conn.cursor()
        
        # Get SQL Server version
        cursor.execute("SELECT @@VERSION")
        version = cursor.fetchone()[0]
        print("CONNECTION SUCCESSFUL!")
        print()
        print(f"SQL Server Version: {version[:80]}...")
        print()
        
        # Get database info
        cursor.execute("""
            SELECT 
                COUNT(*) as table_count
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_TYPE = 'BASE TABLE'
        """)
        table_count = cursor.fetchone()[0]
        print(f"Tables in database: {table_count}")
        
        # Get DimEmployee count
        cursor.execute("SELECT COUNT(*) FROM dbo.DimEmployee")
        emp_count = cursor.fetchone()[0]
        print(f"Employee records: {emp_count}")
        
        # Get DimDepartmentGroup count
        cursor.execute("SELECT COUNT(*) FROM dbo.DimDepartmentGroup")
        dept_count = cursor.fetchone()[0]
        print(f"Department records: {dept_count}")
        
        cursor.close()
        conn.close()
        
        print()
        print("="*60)
        print("ALL CHECKS PASSED - READY TO RUN PIPELINE")
        print("="*60)
        print()
        print("Next steps:")
        print("  1. Run extraction: python src/extract/extract_employees.py")
        print("  2. Or run full pipeline: python main_pipeline.py")
        print()
        
        return 0
        
    except Exception as e:
        print("CONNECTION FAILED!")
        print()
        print(f"Error: {str(e)}")
        print()
        print("="*60)
        print("TROUBLESHOOTING TIPS:")
        print("="*60)
        print("1. Verify SQL Server is running")
        print("2. Check server name in .env file")
        print("3. Ensure Windows Authentication is enabled")
        print("4. Install ODBC Driver 17 for SQL Server")
        print()
        
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)

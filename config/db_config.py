"""
Database Configuration Module
Handles connection to SQL Server using Windows Authentication
"""

import os
import pyodbc
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.engine import URL

# Load environment variables
load_dotenv()

class DatabaseConfig:
    """Configuration class for database connections"""
    
    def __init__(self):
        self.server = os.getenv('SQL_SERVER', 'BEBA')
        self.database = os.getenv('SQL_DATABASE', 'AdventureWorksDW2022')
        self.driver = os.getenv('SQL_DRIVER', 'ODBC Driver 17 for SQL Server')
        self.username = os.getenv('SQL_USERNAME', '')
        self.password = os.getenv('SQL_PASSWORD', '')
    
    def get_connection_string(self):
        """
        Generate connection string for pyodbc
        Uses Windows Authentication if username/password are empty
        """
        if self.username and self.password:
            # SQL Server Authentication
            conn_str = (
                f"DRIVER={{{self.driver}}};"
                f"SERVER={self.server};"
                f"DATABASE={self.database};"
                f"UID={self.username};"
                f"PWD={self.password}"
            )
        else:
            # Windows Authentication
            conn_str = (
                f"DRIVER={{{self.driver}}};"
                f"SERVER={self.server};"
                f"DATABASE={self.database};"
                f"Trusted_Connection=yes;"
            )
        return conn_str
    
    def get_pyodbc_connection(self):
        """
        Create and return a pyodbc connection
        """
        try:
            conn = pyodbc.connect(self.get_connection_string())
            return conn
        except Exception as e:
            raise Exception(f"Failed to connect to SQL Server: {str(e)}")
    
    def get_sqlalchemy_engine(self):
        """
        Create and return a SQLAlchemy engine
        Useful for pandas read_sql operations
        """
        try:
            connection_url = URL.create(
                "mssql+pyodbc",
                query={"odbc_connect": self.get_connection_string()}
            )
            engine = create_engine(connection_url)
            return engine
        except Exception as e:
            raise Exception(f"Failed to create SQLAlchemy engine: {str(e)}")
    
    def test_connection(self):
        """
        Test the database connection
        Returns True if successful, False otherwise
        """
        try:
            conn = self.get_pyodbc_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            cursor.close()
            conn.close()
            return True
        except Exception as e:
            print(f"Connection test failed: {str(e)}")
            return False


# Create a singleton instance
db_config = DatabaseConfig()


if __name__ == "__main__":
    # Test the connection
    print("Testing SQL Server connection...")
    print(f"Server: {db_config.server}")
    print(f"Database: {db_config.database}")
    print(f"Driver: {db_config.driver}")
    
    if db_config.test_connection():
        print("✅ Connection successful!")
    else:
        print("❌ Connection failed!")

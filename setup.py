"""
Setup Script
Helps set up the project environment and validate configuration
"""

import os
import sys
from pathlib import Path
import subprocess


def print_header(text):
    """Print a formatted header"""
    print("\n" + "="*60)
    print(text)
    print("="*60 + "\n")


def check_python_version():
    """Check if Python version is compatible"""
    print("Checking Python version...")
    version = sys.version_info
    
    if version.major >= 3 and version.minor >= 8:
        print(f"✅ Python {version.major}.{version.minor}.{version.micro} (Compatible)")
        return True
    else:
        print(f"❌ Python {version.major}.{version.minor}.{version.micro} (Requires 3.8+)")
        return False


def check_env_file():
    """Check if .env file exists"""
    print("Checking environment configuration...")
    
    base_dir = Path(__file__).parent
    env_file = base_dir / ".env"
    env_example = base_dir / ".env.example"
    
    if env_file.exists():
        print("✅ .env file found")
        return True
    elif env_example.exists():
        print("⚠️  .env file not found")
        print("   Creating .env from .env.example...")
        
        # Copy .env.example to .env
        with open(env_example, 'r') as src:
            content = src.read()
        with open(env_file, 'w') as dst:
            dst.write(content)
        
        print("✅ .env file created")
        print("   ⚠️  Please edit .env with your database settings")
        return False
    else:
        print("❌ .env.example not found")
        return False


def check_directories():
    """Create necessary directories"""
    print("Creating project directories...")
    
    directories = [
        'data/bronze',
        'data/silver',
        'data/gold',
        'logs',
        'tests',
        'notebooks'
    ]
    
    for directory in directories:
        Path(directory).mkdir(parents=True, exist_ok=True)
    
    print(f"✅ Created {len(directories)} directories")


def install_dependencies():
    """Install Python dependencies"""
    print("Installing Python dependencies...")
    print("This may take a few minutes...")
    print()
    
    try:
        subprocess.check_call([
            sys.executable, 
            "-m", 
            "pip", 
            "install", 
            "-r", 
            "requirements.txt"
        ])
        print()
        print("✅ Dependencies installed successfully")
        return True
    except subprocess.CalledProcessError:
        print()
        print("❌ Failed to install dependencies")
        print("   Try manually: pip install -r requirements.txt")
        return False


def test_imports():
    """Test if critical packages can be imported"""
    print("Testing package imports...")
    
    packages = [
        ('pandas', 'pandas'),
        ('pyodbc', 'pyodbc'),
        ('sqlalchemy', 'SQLAlchemy'),
        ('dotenv', 'python-dotenv')
    ]
    
    all_ok = True
    for package, display_name in packages:
        try:
            __import__(package)
            print(f"  ✅ {display_name}")
        except ImportError:
            print(f"  ❌ {display_name} (not installed)")
            all_ok = False
    
    return all_ok


def main():
    """Main setup function"""
    print_header("EMPLOYEE DATA PIPELINE - SETUP")
    
    print("Welcome! This script will help you set up the project.\n")
    
    # Step 1: Check Python version
    print_header("STEP 1: Python Version Check")
    if not check_python_version():
        print("\n❌ Setup failed: Python 3.8+ required")
        print("Please upgrade Python and try again")
        return 1
    
    # Step 2: Check/create .env file
    print_header("STEP 2: Environment Configuration")
    env_configured = check_env_file()
    
    # Step 3: Create directories
    print_header("STEP 3: Directory Structure")
    check_directories()
    
    # Step 4: Install dependencies
    print_header("STEP 4: Install Dependencies")
    print("Do you want to install Python dependencies? (y/n): ", end="")
    
    response = input().strip().lower()
    if response == 'y':
        if not install_dependencies():
            print("\n⚠️  Dependency installation failed")
            print("Please install manually: pip install -r requirements.txt")
    else:
        print("Skipped dependency installation")
    
    # Step 5: Test imports
    print_header("STEP 5: Verify Installation")
    if test_imports():
        print("\n✅ All packages available")
    else:
        print("\n⚠️  Some packages missing - install with: pip install -r requirements.txt")
    
    # Final summary
    print_header("SETUP COMPLETE")
    
    if not env_configured:
        print("⚠️  IMPORTANT: Edit .env file with your database settings")
        print()
        print("Required settings:")
        print("  - SQL_SERVER (your server name)")
        print("  - SQL_DATABASE (usually AdventureWorksDW2022)")
        print()
    
    print("Next steps:")
    print("  1. Configure .env file (if not done)")
    print("  2. Test connection: python test_connection.py")
    print("  3. Run pipeline: python main_pipeline.py")
    print()
    print("For detailed instructions, see README.md")
    print()
    
    return 0


if __name__ == "__main__":
    try:
        exit_code = main()
        sys.exit(exit_code)
    except KeyboardInterrupt:
        print("\n\n⚠️  Setup cancelled by user")
        sys.exit(1)

# 🚀 QUICK START GUIDE

## Get Up and Running in 5 Minutes!

### Step 1: Open VS Code
```bash
# Navigate to your project folder
cd employee-data-pipeline

# Open in VS Code
code .
```

### Step 2: Create Virtual Environment
Open VS Code Terminal (Ctrl + `) and run:

```bash
# Create virtual environment
python -m venv venv

# Activate it
# On Windows:
venv\Scripts\activate

# You should see (venv) in your terminal now
```

### Step 3: Setup Project
```bash
# Run the setup script
python setup.py

# Follow the prompts
# When asked to install dependencies, type: y
```

### Step 4: Configure Database
1. Create your `.env` file (if setup didn't do it):
   ```bash
   copy .env.example .env
   ```

2. Your `.env` should look like this:
   ```env
   SQL_SERVER=BEBA
   SQL_DATABASE=AdventureWorksDW2022
   SQL_DRIVER=ODBC Driver 17 for SQL Server
   SQL_USERNAME=
   SQL_PASSWORD=
   ```

### Step 5: Test Connection
```bash
python test_connection.py
```

Expected output:
```
✅ CONNECTION SUCCESSFUL!
Employee records: 290
Department records: 5
✅ ALL CHECKS PASSED - READY TO RUN PIPELINE
```

### Step 6: Run the Pipeline! 🎉
```bash
python main_pipeline.py
```

Watch the magic happen! You should see:
- ✅ Extraction phase complete
- ✅ Transformation phase complete  
- ✅ Load phase complete
- 🎉 PIPELINE COMPLETED SUCCESSFULLY!

### Step 7: Check Your Results
Your data will be in:
```
📁 data/
  ├── 📁 bronze/    (raw data from SQL Server)
  ├── 📁 silver/    (cleaned data)
  └── 📁 gold/      (analytics tables)
```

---

## Testing with New Data

### Add a Test Employee
1. Open SQL Server Management Studio
2. Go to AdventureWorksDW2022 database
3. Run this query:
   ```sql
   -- View existing employees first
   SELECT TOP 5 * FROM dbo.DimEmployee
   ORDER BY EmployeeKey DESC;
   
   -- Add a test employee (modify as needed)
   INSERT INTO dbo.DimEmployee (
       FirstName, LastName, HireDate, BirthDate,
       EmailAddress, Gender, MaritalStatus,
       CurrentFlag, SalesPersonFlag
   )
   VALUES (
       'Test', 'Employee', GETDATE(), '1990-01-01',
       'test@example.com', 'M', 'S',
       1, 0
   );
   ```

4. Run the pipeline again:
   ```bash
   python main_pipeline.py
   ```

5. Check if the new employee appears in `data/gold/department_summary_latest.csv`

---

## Common Commands

```bash
# Test connection only
python test_connection.py

# Extract employees only
python src/extract/extract_employees.py

# Transform only
python src/transform/transform_employees.py

# Load to gold only
python src/load/load_to_gold.py

# Run full pipeline
python main_pipeline.py

# Check logs
cat logs/pipeline_20240130.log  # Mac/Linux
type logs\pipeline_20240130.log # Windows
```

---

## Troubleshooting

### ❌ Connection Failed
**Problem**: Can't connect to SQL Server

**Solutions**:
1. Make sure SQL Server is running
2. Verify server name in `.env` is correct (`BEBA`)
3. Check that Windows Authentication is enabled
4. Install ODBC Driver 17:
   - Download from: https://docs.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server

### ❌ Module Not Found
**Problem**: `ModuleNotFoundError: No module named 'pandas'`

**Solution**:
```bash
# Make sure virtual environment is activated
venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

### ❌ Bronze File Not Found
**Problem**: `FileNotFoundError: Bronze file not found`

**Solution**: Run extraction first
```bash
python src/extract/extract_employees.py
```

---

## VS Code Tips

### Recommended Extensions
1. Python (Microsoft)
2. Pylance
3. GitLens
4. Rainbow CSV (for viewing CSV files)

### View CSV Files in VS Code
- Click on any CSV file in the `data/` folders
- Install "Rainbow CSV" extension for better viewing
- Or use Excel to open the files

### Terminal Shortcuts
- `Ctrl + `` - Open/close terminal
- `Ctrl + Shift + `` - New terminal
- `Ctrl + C` - Stop running process

---

## What Each Layer Contains

### 🥉 Bronze Layer (`data/bronze/`)
- **Raw data** directly from SQL Server
- Unchanged from source
- Includes extraction timestamp
- Files: `dimemployee_latest.csv`, `dimdepartmentgroup_latest.csv`

### 🥈 Silver Layer (`data/silver/`)
- **Cleaned** data
- Standardized formats
- Duplicates removed
- New calculated fields (Age, YearsOfService, FullName)
- Files: `employees_latest.csv`

### 🥇 Gold Layer (`data/gold/`)
- **Analytics-ready** tables
- Aggregated metrics
- Business KPIs
- Files:
  - `department_summary_latest.csv` - Metrics by department
  - `gender_diversity_latest.csv` - Gender distribution
  - `tenure_analysis_latest.csv` - Years of service breakdown
  - `hiring_trends_latest.csv` - Hiring patterns over time

---

## Next Steps

After successfully running the pipeline:

1. ✅ **Explore the data** in Excel or VS Code
2. ✅ **Try adding new records** to test incremental loads
3. ✅ **Read the full README.md** for advanced features
4. ✅ **Push to GitHub** to showcase your work!

### Pushing to GitHub
```bash
# Initialize git (if not done)
git init

# Add all files
git add .

# Commit
git commit -m "Initial commit: Employee data pipeline"

# Add remote (create repo on GitHub first)
git remote add origin https://github.com/yourusername/employee-data-pipeline.git

# Push
git push -u origin main
```

---

**Questions?** Check the full README.md or review the logs in `logs/` directory!

**Happy Data Engineering! 🚀📊**

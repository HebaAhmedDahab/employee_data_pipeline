# Employee Data Pipeline 🚀

A production-ready data engineering pipeline that extracts employee data from SQL Server, transforms it through bronze/silver/gold layers, and creates analytics-ready datasets.

## 📋 Project Overview

This project demonstrates a modern data engineering workflow using the **Medallion Architecture** (Bronze → Silver → Gold) to process employee data from the AdventureWorksDW2022 database.

### Architecture

```
SQL Server (Source)
    ↓
BRONZE LAYER (Raw Data)
    ↓
SILVER LAYER (Cleaned & Validated)
    ↓
GOLD LAYER (Analytics-Ready)
```

### What This Pipeline Does

1. **Extract**: Pulls employee and department data from SQL Server
2. **Transform**: Cleans, standardizes, and enriches the data
3. **Load**: Creates business-ready analytics tables
4. **Monitor**: Logs all operations and validates data quality

---

## 🛠️ Tech Stack

- **Language**: Python 3.8+
- **Database**: SQL Server (AdventureWorksDW2022)
- **Libraries**:
  - `pandas` - Data manipulation
  - `pyodbc` - SQL Server connectivity
  - `SQLAlchemy` - Database abstraction
  - `python-dotenv` - Environment management
  - `great-expectations` - Data validation (optional)

---

## 📁 Project Structure

```
employee-data-pipeline/
│
├── config/
│   └── db_config.py          # Database connection configuration
│
├── src/
│   ├── extract/              # Data extraction modules
│   │   ├── extract_employees.py
│   │   └── extract_departments.py
│   │
│   ├── transform/            # Data transformation modules
│   │   └── transform_employees.py
│   │
│   ├── load/                 # Data loading modules
│   │   └── load_to_gold.py
│   │
│   └── utils/                # Utility modules
│       ├── logger.py         # Logging configuration
│       └── data_quality.py   # Data quality checks
│
├── data/                     # Data storage (not in git)
│   ├── bronze/               # Raw extracted data
│   ├── silver/               # Cleaned data
│   └── gold/                 # Analytics-ready data
│
├── logs/                     # Pipeline logs (not in git)
├── tests/                    # Unit tests
├── notebooks/                # Jupyter notebooks for exploration
│
├── main_pipeline.py          # Main pipeline orchestrator
├── requirements.txt          # Python dependencies
├── .env.example              # Environment variables template
├── .gitignore               # Git ignore rules
└── README.md                # This file
```

---

## 🚀 Getting Started

### Prerequisites

- Python 3.8 or higher
- SQL Server with AdventureWorksDW2022 database
- SQL Server ODBC Driver 17

### Installation

1. **Clone the repository**
   ```bash
   git clone <your-repo-url>
   cd employee-data-pipeline
   ```

2. **Create virtual environment**
   ```bash
   python -m venv venv
   
   # On Windows
   venv\Scripts\activate
   
3. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

4. **Configure environment variables**
   ```bash
   # Copy the example file
   copy .env.example .env  # Windows
      
   # Edit .env with your database settings
   # For Windows Authentication, leave USERNAME and PASSWORD empty
   ```

### Configuration

Edit your `.env` file:

---

## 🏃 Running the Pipeline

### Test Database Connection

```bash
python config/db_config.py
```

Expected output:
```
Testing SQL Server connection...
Server: BEBA
Database: AdventureWorksDW2022
✅ Connection successful!
```

### Run Individual Components

**Extract Only:**
```bash
python src/extract/extract_employees.py
python src/extract/extract_departments.py
```

**Transform Only:**
```bash
python src/transform/transform_employees.py
```

**Load Only:**
```bash
python src/load/load_to_gold.py
```

### Run Complete Pipeline

```bash
python main_pipeline.py
```

Expected output:
```
============================================================
🚀 STARTING PIPELINE: Employee Data Pipeline
Start Time: 2024-01-30 10:30:15
============================================================

PHASE 1: EXTRACTION
============================================================
✅ Employees extracted: 290 rows
✅ Departments extracted: 5 rows

PHASE 2: TRANSFORMATION
============================================================
✅ Employees transformed: 290 rows

PHASE 3: LOAD TO GOLD LAYER
============================================================
✅ Analytics tables created: 4

🎉 PIPELINE COMPLETED SUCCESSFULLY! 🎉
```

---

## 📊 Output Data

### Bronze Layer (`data/bronze/`)
- **Raw data** directly from SQL Server
- No transformations applied
- Includes extraction timestamp

**Files:**
- `dimemployee_latest.csv` - Employee data
- `dimdepartmentgroup_latest.csv` - Department data

### Silver Layer (`data/silver/`)
- **Cleaned and standardized** data
- Duplicates removed
- Data types corrected
- Derived fields added (Age, YearsOfService, FullName)

**Files:**
- `employees_latest.csv` - Cleaned employee data

### Gold Layer (`data/gold/`)
- **Analytics-ready** aggregated tables
- Business metrics and KPIs

**Files:**
- `department_summary_latest.csv` - Department-level metrics
- `gender_diversity_latest.csv` - Gender distribution by department
- `tenure_analysis_latest.csv` - Employee tenure breakdown
- `hiring_trends_latest.csv` - Hiring patterns by year

---

## 🧪 Testing the Pipeline

### Test with New Data

1. Open SQL Server Management Studio
2. Add a new employee record:
   ```sql
   -- Example: Add a test employee
   INSERT INTO DimEmployee (...)
   VALUES (...)
   ```
3. Run the pipeline again:
   ```bash
   python main_pipeline.py
   ```
4. Check if the new record appears in the output

### Validate Data Quality

The pipeline automatically performs quality checks:
- ✅ Row count validation
- ✅ Null value detection
- ✅ Duplicate detection
- ✅ Data type validation

Check logs in `logs/pipeline_YYYYMMDD.log`

---

## 📝 Logging

All pipeline operations are logged to:
- **Console** - Real-time output
- **Log files** - `logs/pipeline_YYYYMMDD.log`

Log levels:
- `INFO` - Normal operations
- `WARNING` - Data quality issues
- `ERROR` - Pipeline failures

---

## 🔍 Data Quality Checks

The pipeline includes comprehensive quality checks:

```python
# Example quality check output
[DimEmployee] Row count: 290 ✅
[DimEmployee] ⚠️ Column 'EmailAddress' has 5 null values (1.72%)
[DimEmployee] ✅ No duplicates found
```

---

## 📈 Analytics Examples

### Department Summary
```
DepartmentName  | total_employees | avg_base_rate | avg_years_of_service
----------------|-----------------|---------------|---------------------
Engineering     | 75              | 32.50         | 8.5
Production      | 120             | 28.75         | 6.2
Marketing       | 45              | 35.00         | 5.8
```

### Gender Diversity
```
DepartmentName  | Gender | employee_count | percentage
----------------|--------|----------------|------------
Engineering     | Male   | 60             | 80.00
Engineering     | Female | 15             | 20.00
```

---

## 🔄 Next Steps & Improvements

### Phase 2 Enhancements
- [ ] Add Apache Airflow for scheduling
- [ ] Implement incremental loads
- [ ] Add email notifications
- [ ] Create Power BI dashboards

### Phase 3 Enhancements
- [ ] Deploy to cloud (AWS/Azure/GCP)
- [ ] Add CI/CD pipeline
- [ ] Implement data versioning
- [ ] Add real-time streaming

---

## 🤝 Contributing

This is a learning project. Feel free to:
1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Submit a pull request

---

## 🐛 Troubleshooting

### Connection Issues

**Error**: `pyodbc.InterfaceError: ('IM002'...)`
- **Solution**: Install ODBC Driver 17 for SQL Server

**Error**: `Login failed for user`
- **Solution**: Verify Windows Authentication is enabled

### Import Errors

**Error**: `ModuleNotFoundError: No module named 'pandas'`
- **Solution**: Install requirements: `pip install -r requirements.txt`

### Data Issues

**Error**: `FileNotFoundError: Bronze file not found`
- **Solution**: Run extraction first: `python src/extract/extract_employees.py`

---

## 🎯 Project Goals Achieved

✅ Extract data from SQL Server using Python
✅ Implement Medallion Architecture (Bronze/Silver/Gold)
✅ Perform data quality checks
✅ Create analytics-ready datasets
✅ Implement comprehensive logging
✅ Follow software engineering best practices
✅ Make GitHub-ready with proper documentation

---

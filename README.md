# TPC-H Data and Query Generator

A comprehensive toolkit for generating TPC-H benchmark datasets and queries in optimized Parquet format with cross-platform compatibility.

## Overview

This project provides a complete TPC-H benchmark environment generator with two main components:

1. **Data Generation** - Creates TPC-H benchmark tables in optimized Parquet format using PySpark
2. **Query Generation** - Generates all 22 standard TPC-H queries with proper parameter substitution

**Key Features:**
- **Cross-platform compatibility** - Works on Windows, macOS, and Linux
- **Self-contained** - No external binary dependencies for query generation
- **TPC-H compliant** - Follows official specification for parameter generation
- **Optimized output** - Parquet format with compression and partitioning
- **Reproducible** - Seed-based parameter generation for consistent results

## Prerequisites

Before using this toolkit, ensure you have the following installed:

- **Python 3.7 or higher** - Required for all scripts
- **Java 8 or higher** - Required for PySpark data generation
- **Git** - For cloning this repository

## Installation

### Step 1: Clone the Repository
```bash
git clone https://github.com/agold-spd/speedata-tpc-generator.git
cd speedata-tpc-generator
```

### Step 2: Install Python Dependencies
```bash
pip install -r requirements.txt
```
This will install:
- `pyspark` - For distributed data processing
- `tpchgen-cli` - For generating raw TPC-H data
- `pyyaml` - For configuration file parsing

### Step 3: Verify Installation
Check that all required tools are available:
```bash
# Verify Python version
python3 --version

# Verify Java installation
java -version

# Verify tpchgen-cli installation
tpchgen --help
```

## Usage Guide

### Data Generation

The data generation process creates TPC-H benchmark tables in Parquet format with automatic optimization.

#### Basic Usage
```bash
# Generate 1GB dataset (scale factor 1) - good for testing
python3 scripts/generate_tpch.py /path/to/output/data --scale 1

# Generate 10GB dataset (scale factor 10) - typical development size
python3 scripts/generate_tpch.py /path/to/output/data --scale 10

# Generate 1TB dataset (scale factor 1000) - production benchmark size
python3 scripts/generate_tpch.py /path/to/output/data --scale 1000
```

#### Advanced Options
```bash
# Generate specific tables only
python3 scripts/generate_tpch.py /path/to/output/data --scale 1 --tables customer,orders,lineitem

# Disable dictionary encoding (for compatibility with some systems)
python3 scripts/generate_tpch.py /path/to/output/data --scale 1 --no-dict

# Use different compression (default is SNAPPY)
python3 scripts/generate_tpch.py /path/to/output/data --scale 1 --compression GZIP
```

#### What This Creates
The data generation process will create a directory structure like this:
```
/path/to/output/data/
├── customer/           # Customer table (150K * SF rows)
├── lineitem/          # Line item table (6M * SF rows) 
├── nation/            # Nation table (25 rows)
├── orders/            # Orders table (1.5M * SF rows)
├── part/              # Part table (200K * SF rows)
├── partsupp/          # Part-supplier table (800K * SF rows)
├── region/            # Region table (5 rows)
└── supplier/          # Supplier table (10K * SF rows)
```

Each table directory contains Parquet files optimized for analytics workloads.

### Query Generation

The query generation creates all 22 TPC-H benchmark queries with randomized parameters according to the official TPC-H specification.

#### Generate All Queries
```bash
# Generate all 22 queries with default parameters
python3 scripts/generate_tpch_queries.py /path/to/output/queries

# Generate queries with a specific seed for reproducible results
python3 scripts/generate_tpch_queries.py /path/to/output/queries --seed 12345
```

#### Generate Individual Queries
```bash
# Generate only query 1 and print to console
python3 scripts/generate_tpch_queries.py /path/to/output/queries --query 1

# Generate query 15 with specific seed
python3 scripts/generate_tpch_queries.py /path/to/output/queries --query 15 --seed 999
```

#### What This Creates
The query generation will create SQL files:
```
/path/to/output/queries/
├── q01.sql            # Pricing Summary Report
├── q02.sql            # Minimum Cost Supplier
├── q03.sql            # Shipping Priority
├── q04.sql            # Order Priority Checking
├── q05.sql            # Local Supplier Volume
├── q06.sql            # Forecasting Revenue Change
├── q07.sql            # Volume Shipping
├── q08.sql            # National Market Share
├── q09.sql            # Product Type Profit Measure
├── q10.sql            # Returned Item Reporting
├── q11.sql            # Important Stock Identification
├── q12.sql            # Shipping Modes and Order Priority
├── q13.sql            # Customer Distribution
├── q14.sql            # Promotion Effect
├── q15.sql            # Top Supplier (with CREATE VIEW)
├── q16.sql            # Parts/Supplier Relationship
├── q17.sql            # Small-Quantity-Order Revenue
├── q18.sql            # Large Volume Customer
├── q19.sql            # Discounted Revenue
├── q20.sql            # Potential Part Promotion
├── q21.sql            # Suppliers Who Kept Orders Waiting
└── q22.sql            # Global Sales Opportunity
```

Each query file contains:
- Query metadata (name, description)
- Generation parameters used
- Complete SQL query with substituted parameters

## Configuration

### Spark Configuration (`config/spark.yaml`)
Adjust Spark settings based on your system resources:
```yaml
driver_memory: "4g"        # Increase for larger datasets
executor_memory: "4g"      # Increase for better performance
executor_cores: 2          # Match your CPU cores
```

### Partitioning Configuration (`config/partitions.yaml`)
Control how data is partitioned for optimal query performance:
```yaml
customer: 4               # Number of partitions for customer table
lineitem: 16              # Lineitem is largest, needs more partitions
orders: 8                 # Balance between parallelism and file count
```

## Understanding TPC-H

TPC-H (Transaction Processing Performance Council - H) is a decision support benchmark that:

- **Simulates business scenarios** with 22 complex analytical queries
- **Tests database performance** on large datasets with joins, aggregations, and subqueries
- **Provides industry standard** for comparing database and analytics systems
- **Scales linearly** - scale factor 1 = 1GB, scale factor 1000 = 1TB

The benchmark includes 8 tables representing a wholesale supplier business:
- **Customer** - Customer information
- **Orders** - Order headers
- **Lineitem** - Order line items (largest table)
- **Part** - Part information
- **Supplier** - Supplier information
- **Partsupp** - Part-supplier relationships
- **Nation** - Nation reference data
- **Region** - Region reference data

## Examples

### Complete Workflow Example
```bash
# 1. Create a small test dataset (1GB)
python3 scripts/generate_tpch.py ./data --scale 1

# 2. Generate queries for testing
python3 scripts/generate_tpch_queries.py ./queries --seed 42

# 3. Test a single query
python3 scripts/generate_tpch_queries.py ./queries --query 1 --seed 42

# 4. Use the generated data and queries with your database system
# (Examples for different systems would go here)
```

### Production Benchmark Example
```bash
# Generate 100GB dataset for production benchmarking
python3 scripts/generate_tpch.py /data/tpch-sf100 --scale 100

# Generate official query suite
python3 scripts/generate_tpch_queries.py /queries/tpch-sf100 --seed 1

# Verify generation completed successfully
ls -la /data/tpch-sf100/     # Should show 8 table directories
ls -la /queries/tpch-sf100/  # Should show 22 .sql files
```

## Troubleshooting

### Common Issues and Solutions

#### 1. Installation Issues

**Problem:** `tpchgen-cli` command not found
```bash
tpchgen: command not found
```
**Solution:**
```bash
# Reinstall tpchgen-cli
pip install --upgrade tpchgen-cli

# Verify installation
pip list | grep tpchgen

# If still not working, try with explicit path
python3 -m pip install tpchgen-cli
```

**Problem:** Java not found or wrong version
```bash
java: command not found
# or
Java version is below 8
```
**Solution:**
```bash
# On macOS with Homebrew
brew install openjdk@11

# On Ubuntu/Debian
sudo apt-get update
sudo apt-get install openjdk-11-jdk

# On Windows - download from Oracle or use OpenJDK
# Verify installation
java -version
```

#### 2. Data Generation Issues

**Problem:** Out of disk space during generation
```bash
No space left on device
```
**Solution:**
- Scale factor 1 needs ~1GB free space
- Scale factor 10 needs ~10GB free space  
- Scale factor 1000 needs ~1TB free space
- Check available space: `df -h`
- Use smaller scale factor or clean up disk space

**Problem:** Memory errors during Spark processing
```bash
Java heap space error
OutOfMemoryError
```
**Solution:**
Edit `config/spark.yaml` to reduce memory usage:
```yaml
driver_memory: "2g"      # Reduce from 4g
executor_memory: "2g"    # Reduce from 4g
executor_cores: 1        # Reduce parallelism
```

**Problem:** Permission denied errors
```bash
Permission denied: '/path/to/output'
```
**Solution:**
```bash
# Ensure output directory exists and is writable
mkdir -p /path/to/output
chmod 755 /path/to/output

# Or use a directory you own
python3 scripts/generate_tpch.py ./my-data --scale 1
```

#### 3. Query Generation Issues

**Problem:** Query templates not found
```bash
Error: Query templates not found in templates/ directory
```
**Solution:**
```bash
# Verify templates directory exists
ls -la templates/

# Should show files like 1.sql, 2.sql, etc.
# If missing, re-clone the repository:
git clone https://github.com/agold-spd/speedata-tpc-generator.git
```

**Problem:** Invalid query number
```bash
Error: Query 23 not supported
```
**Solution:**
TPC-H has exactly 22 queries (1-22). Use a valid query number:
```bash
python3 scripts/generate_tpch_queries.py queries --query 15  # Valid
```

#### 4. Performance Optimization

**Slow data generation:**
- Increase Spark executor memory in `config/spark.yaml`
- Use SSD storage instead of HDD
- Increase the number of executor cores (but don't exceed your CPU cores)

**Large file sizes:**
- Enable compression: `--compression GZIP` or `--compression SNAPPY`
- Adjust partitioning in `config/partitions.yaml`
- Use dictionary encoding (enabled by default)

#### 5. Verification Steps

**Verify data generation completed successfully:**
```bash
# Check all 8 tables were created
ls /path/to/data/
# Should show: customer lineitem nation orders part partsupp region supplier

# Check table sizes are reasonable
du -sh /path/to/data/*
# Lineitem should be the largest table
```

**Verify query generation completed successfully:**
```bash
# Check all 22 queries were created
ls /path/to/queries/*.sql | wc -l
# Should output: 22

# Verify queries have content
head -5 /path/to/queries/q01.sql
# Should show query header and SQL
```

## Advanced Usage

### Custom Configurations

#### Creating Custom Partitioning
Edit `config/partitions.yaml` for your specific use case:
```yaml
# For small datasets (SF 1-10)
customer: 2
lineitem: 4
orders: 2

# For medium datasets (SF 100)
customer: 8
lineitem: 32
orders: 16

# For large datasets (SF 1000+)
customer: 16
lineitem: 64
orders: 32
```

#### Optimizing for Your Hardware
Edit `config/spark.yaml` based on your system:
```yaml
# For systems with 8GB RAM
driver_memory: "2g"
executor_memory: "2g"
executor_cores: 2

# For systems with 32GB RAM
driver_memory: "8g"
executor_memory: "8g"
executor_cores: 4

# For systems with 64GB+ RAM
driver_memory: "16g"
executor_memory: "16g" 
executor_cores: 8
```

### Integration Examples

#### Using with Apache Spark
```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("TPC-H").getOrCreate()

# Load generated data
customer = spark.read.parquet("./data/customer")
lineitem = spark.read.parquet("./data/lineitem")

# Register as temporary views
customer.createOrReplaceTempView("customer")
lineitem.createOrReplaceTempView("lineitem")

# Run generated queries
query1 = open("./queries/q01.sql").read()
result = spark.sql(query1)
result.show()
```

#### Using with DuckDB
```python
import duckdb

conn = duckdb.connect()

# Load generated data
conn.execute("CREATE VIEW customer AS SELECT * FROM './data/customer/*.parquet'")
conn.execute("CREATE VIEW lineitem AS SELECT * FROM './data/lineitem/*.parquet'")

# Run generated queries
with open("./queries/q01.sql") as f:
    query = f.read()
    result = conn.execute(query).fetchall()
    print(result)
```

## Project Structure

```
speedata-tpc-generator/
├── README.md                      # This file
├── LICENSE                        # MIT license
├── requirements.txt               # Python dependencies
├── config/                        # Configuration files
│   ├── partitions.yaml           # Table partitioning settings
│   └── spark.yaml                # Spark configuration
├── scripts/                       # Main scripts
│   ├── generate_tpch.py          # Data generation script
│   └── generate_tpch_queries.py  # Query generation script
└── templates/                     # TPC-H query templates
    ├── 1.sql                     # Query 1 template
    ├── 2.sql                     # Query 2 template
    └── ...                       # Queries 3-22
```

## Support

- **Issues:** Report bugs or request features via GitHub Issues
- **Documentation:** Refer to this README for comprehensive usage instructions
- **TPC-H Specification:** [Official TPC-H Documentation](http://www.tpc.org/tpch/)

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Acknowledgments

- Transaction Processing Performance Council (TPC) for the TPC-H specification
- The open-source community for tools and libraries used in this project

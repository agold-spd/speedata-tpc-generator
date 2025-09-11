# TPC-H Data Generator

A tool to generate TPC-H benchmark data at configurable scale in Parquet format.

## Overview

This project creates TPC-H benchmark datasets using the `tpchgen-cli` tool. It generates data in Parquet format with automatic data type optimization and optional dictionary encoding control. The default scale factor is 1000 (approximately 1TB), but this can be adjusted.

## Requirements

- Python 3.7 or higher
- Java 8 or higher (required by PySpark)
- `tpchgen-cli` command line tool
- Sufficient disk space (varies by scale factor: ~1GB for SF 1, ~1TB for SF 1000)

## Installation

Install dependencies directly or use a virtual environment:

```bash
# Option 1: Install directly
pip install -r requirements.txt

# Option 2: Use virtual environment (recommended)
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt
```

Make sure the `tpchgen-cli` command is available in your system PATH after installation.

The requirements file includes:
- `tpchgen-cli` - the core TPC-H data generator
- `pyyaml` - for reading configuration files
- `pyspark` - for data type optimization and Parquet rewriting

## Usage

### Generate TPC-H Data

Generate the complete TPC-H dataset:

```bash
python scripts/generate_tpch.py /path/to/output
```

Generate with a specific scale factor:

```bash
# Generate 1GB dataset (scale factor 1)
python scripts/generate_tpch.py /path/to/output --scale 1

# Generate 10GB dataset (scale factor 10)  
python scripts/generate_tpch.py /path/to/output --scale 10

# Generate 1TB dataset (scale factor 1000) - default
python scripts/generate_tpch.py /path/to/output --scale 1000
```

Generate specific tables only:

```bash
# Generate only customer and orders tables
python scripts/generate_tpch.py /path/to/output --tables customer,orders

# Generate just the largest table for testing
python scripts/generate_tpch.py /path/to/output --tables lineitem --scale 1
```

This command will:
- Generate all TPC-H tables at scale factor 1000 (1TB total size) by default
- Use partition configurations from `config/partitions.yaml`
- Create separate directories for each table under `/path/to/output/`
- Save data in Parquet format
- Automatically optimize data types and apply chosen compression

### Data Type Optimization and Dictionary Encoding

This tool automatically optimizes data types for better performance and storage efficiency. Dictionary encoding can be optionally controlled:

#### Generate with dictionary encoding enabled (default):
```bash
python scripts/generate_tpch.py /path/to/output
```

#### Generate with dictionary encoding disabled:
```bash
python scripts/generate_tpch.py /path/to/output --no-dict
```

#### Generate with custom compression:
```bash
python scripts/generate_tpch.py /path/to/output --compression GZIP
python scripts/generate_tpch.py /path/to/output --no-dict --compression UNCOMPRESSED
```

**Available compression options**: SNAPPY (default), GZIP, UNCOMPRESSED

The script will always:
1. First generate the data using `tpchgen-cli`
2. Then automatically rewrite the files using Apache Spark with data type optimization
3. Apply dictionary encoding based on the `--no-dict` flag (enabled by default)
4. Apply the chosen compression codec during rewrite
5. Use Spark's intelligent auto-partitioning for optimal performance
6. Replace the original files with optimized versions (saves disk space)

**Data Type Optimizations Applied:**
- Convert `DecimalType(38,0)` columns to appropriate integer types based on TPC-H spec (`IntegerType` or `LongType`)
- Special handling for `partsupp` table with optimized decimal precision
- Improved query performance and reduced storage footprint

## Configuration

The project uses separate configuration files for different aspects:

- `config/partitions.yaml` - controls how many files tpchgen-cli generates per table (optimized for 1TB)
- `config/spark.yaml` - Spark performance settings for the optimization phase  
- `requirements.txt` - Python dependencies

**Note:** Partition counts only affect initial file generation. Spark uses auto-partitioning during optimization.

### Spark Performance Tuning

Spark settings are in `config/spark.yaml`, organized by category for easy customization:

#### Quick Setup Examples:

**For smaller systems (16GB RAM, 8 cores):**
```yaml
memory:
  driver: "4g"
  executor: "8g"
performance:
  shuffle_partitions: 100
  default_parallelism: 50
```

**For larger systems (64GB+ RAM, 32+ cores):**
```yaml
memory:
  driver: "16g"
  executor: "32g"
performance:
  shuffle_partitions: 400
  default_parallelism: 200
```

**Adding custom Spark settings:**
```yaml
custom:
  "spark.executor.instances": "4"
  "spark.sql.adaptive.advisoryPartitionSizeInBytes": "256MB"
```

The configuration includes sensible defaults and is well-documented with examples for different system sizes.

## Output Structure

After running the generator, your output directory will contain:

```
/path/to/output/
├── customer/
├── lineitem/
├── nation/
├── orders/
├── part/
├── partsupp/
├── region/
└── supplier/
```

Each directory contains Parquet files for the corresponding TPC-H table.

## About TPC-H

TPC-H is a decision support benchmark that consists of business-oriented queries and concurrent data modifications. The benchmark provides a representative evaluation of performance for decision support systems that examine large volumes of data and execute complex queries.

## Troubleshooting

### Common Issues

1. **Command not found: tpchgen-cli**
   - Ensure `tpchgen-cli` is installed and in your PATH
   - Try reinstalling with `pip install --upgrade tpchgen-cli`

2. **Out of disk space**
   - The complete dataset requires approximately 1TB of storage
   - Ensure you have sufficient free disk space before starting

3. **Memory errors during Spark optimization**
   - The Spark optimization process requires adequate memory
   - Adjust memory settings in `config/spark.yaml` based on your system
   - Reduce `driver_memory` and `executor_memory` for systems with less RAM

4. **PySpark import errors**
   - Ensure PySpark is installed: `pip install pyspark`
   - Check that Java 8+ is installed and available

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

#!/usr/bin/env python3
"""Generate TPC-H Parquet data using tpchgen-cli with optional dictionary encoding control.

This script can:
1. Generate TPC-H data using tpchgen-cli 
2. Optionally rewrite the generated Parquet files with specific dictionary encoding settings using PySpark

The script uses the tpchgen-cli '--parts/--part' mechanism to produce multiple
part-files per table. For each logical "partition" we generate the part and then
move the generated files into the final table directory.

If dictionary encoding control is requested (--dict or --no-dict), the script will
automatically rewrite the generated files using PySpark after generation.
"""
import argparse
import os
import shutil
import subprocess
import sys
import tempfile
import yaml

TABLE_ORDER = ["region", "nation", "supplier", "customer", "part", "partsupp", "orders", "lineitem"]

def load_config(path):
    """Load the full configuration including partitions and Spark settings."""
    with open(path, 'r') as f:
        return yaml.safe_load(f)

def load_spark_config(path):
    """Load Spark configuration from separate YAML file."""
    with open(path, 'r') as f:
        return yaml.safe_load(f)

def find_parquet_files(path):
    matches = []
    for root, _, files in os.walk(path):
        for fn in files:
            if fn.endswith('.parquet'):
                matches.append(os.path.join(root, fn))
    return matches

def rewrite_parquet_with_dict_encoding(input_dir, output_dir, use_dict, tables, compression="SNAPPY", spark_config_path=None):
    """Rewrite Parquet files with specified dictionary encoding, compression, and optimized data types using PySpark."""
    try:
        from pyspark.sql import SparkSession
        from pyspark.sql.functions import col
        from pyspark.sql.types import IntegerType, LongType
    except ImportError:
        print("Error: PySpark is required for dictionary encoding control. Install with: pip install pyspark")
        sys.exit(1)
    
    # Load Spark configuration from separate file
    if spark_config_path and os.path.exists(spark_config_path):
        spark_yaml_config = load_spark_config(spark_config_path)
    else:
        spark_yaml_config = {}
    
    # Build Spark configuration with defaults
    memory_config = spark_yaml_config.get('memory', {})
    performance_config = spark_yaml_config.get('performance', {})
    adaptive_config = spark_yaml_config.get('adaptive', {})
    parquet_config = spark_yaml_config.get('parquet', {})
    custom_config = spark_yaml_config.get('custom', {})
    
    spark_config = {
        "spark.driver.memory": memory_config.get('driver', '8g'),
        "spark.driver.maxResultSize": memory_config.get('driver_max_result', '4g'),
        "spark.executor.memory": memory_config.get('executor', '12g'),
        "spark.executor.memoryFraction": str(memory_config.get('executor_fraction', 0.8)),
        "spark.sql.shuffle.partitions": str(performance_config.get('shuffle_partitions', 200)),
        "spark.default.parallelism": str(performance_config.get('default_parallelism', 100)),
        "spark.sql.adaptive.enabled": str(adaptive_config.get('enabled', True)).lower(),
        "spark.sql.adaptive.coalescePartitions.enabled": str(adaptive_config.get('coalesce_partitions', True)).lower(),
        "spark.sql.adaptive.coalescePartitions.minPartitionSize": adaptive_config.get('min_partition_size', '64MB'),
        "spark.sql.adaptive.advisoryPartitionSizeInBytes": adaptive_config.get('advisory_partition_size', '128MB'),
        "spark.sql.adaptive.skewJoin.enabled": str(adaptive_config.get('skew_join', True)).lower(),
        "spark.sql.parquet.columnarReaderBatchSize": str(parquet_config.get('batch_size', 8192)),
        "spark.sql.parquet.vectorizedReader.enabled": str(parquet_config.get('vectorized_reader', True)).lower(),
        "spark.sql.parquet.enableVectorizedReader": str(parquet_config.get('vectorized_reader', True)).lower(),
        "spark.sql.parquet.compression.codec": compression.lower(),
    }
    
    # Add custom configurations
    spark_config.update(custom_config)
    
    # Build Spark session with configuration
    builder = SparkSession.builder.appName("tpch-rewrite-parquet")
    for key, value in spark_config.items():
        builder = builder.config(key, value)
    
    spark = builder.getOrCreate()
    
    dict_flag_val = "true" if use_dict else "false"
    print(f"Starting optimization: dictionary={dict_flag_val}, compression={compression}")
    
    # Define columns that should be cast to specific integer types
    int_list = ['L_LINENUMBER', 'O_SHIPPRIORITY', 'PS_AVAILQTY', 'P_SIZE']
    
    for table in tables:
        in_table = os.path.join(input_dir, table)
        out_table = os.path.join(output_dir, table)
        if not os.path.exists(in_table):
            continue
        print(f"Optimizing {table}...")
        df = spark.read.parquet(in_table)
        
        # Cache the DataFrame to avoid file path issues later
        df = df.cache()
        # Trigger caching by counting rows
        row_count = df.count()
        
        # Let Spark decide on optimal partitioning
        current_partitions = df.rdd.getNumPartitions()
        
        # Optimize data types - convert DecimalType(38,0) to appropriate integer types
        integer_columns = [field.name for field in df.schema.fields 
                          if str(field.dataType) == "DecimalType(38,0)" and field.name in int_list]
        decimal_columns = [field.name for field in df.schema.fields 
                          if str(field.dataType) == "DecimalType(38,0)" and field.name not in int_list]
        
        # Cast decimal columns to LongType for better performance
        for column in decimal_columns:
            df = df.withColumn(column, col(column).cast(LongType()))
            
        # Cast specific integer columns to IntegerType
        for column in integer_columns:
            df = df.withColumn(column, col(column).cast(IntegerType()))
        
        # Special handling for partsupp table - only if PS_SUPPLYCOST exists and isn't already decimal
        if table == 'partsupp':
            # Find PS_SUPPLYCOST column (case-insensitive search)
            ps_supplycost_fields = [field for field in df.schema.fields 
                                  if field.name.upper() == "PS_SUPPLYCOST"]
            if ps_supplycost_fields:
                ps_supplycost_field = ps_supplycost_fields[0]
                ps_supplycost_type = ps_supplycost_field.dataType
                if "DecimalType" not in str(ps_supplycost_type):
                    df = df.withColumn(ps_supplycost_field.name, col(ps_supplycost_field.name).cast("decimal(12,2)"))
        
        # Ensure output dir exists (Spark will create it, but be explicit)
        if os.path.exists(out_table):
            shutil.rmtree(out_table)
        
        # Write to temporary location first to avoid conflicts
        temp_table = f"{out_table}_temp"
        if os.path.exists(temp_table):
            shutil.rmtree(temp_table)
            
        (df.write
           .mode("overwrite")
           .option("parquet.enable.dictionary", dict_flag_val)
           .option("parquet.compression", compression)
           .parquet(temp_table))
        
        # Unpersist the cached DataFrame to free memory
        df.unpersist()
        
        # Remove original and replace with optimized version
        if os.path.exists(out_table):
            shutil.rmtree(out_table)
        shutil.move(temp_table, out_table)
    
    spark.stop()
    print("Optimization complete.")

def main():
    p = argparse.ArgumentParser(description="Generate TPC-H Parquet with tpchgen-cli and optional dictionary encoding control")
    p.add_argument("output_dir", help="Output directory")
    p.add_argument("--tpchbin", default="tpchgen-cli", help="Path to tpchgen-cli binary (default: tpchgen-cli in PATH)")
    p.add_argument("--scale", type=int, default=1000, help="Scale factor (default: 1000 = 1TB)")
    p.add_argument("--config", default=os.path.join(os.path.dirname(__file__), "../config/partitions.yaml"),
                   help="Partitions config YAML")
    p.add_argument("--spark-config", default=os.path.join(os.path.dirname(__file__), "../config/spark.yaml"),
                   help="Spark configuration YAML")
    p.add_argument("--tables", default=None, help="Comma-separated list of tables to generate (default: all)")
    
    # Dictionary encoding options
    p.add_argument("--no-dict", dest="use_dict", action="store_false", default=True,
                   help="Disable dictionary encoding (default: enabled). Data type optimization is always applied.")
    
    # Compression option
    p.add_argument("--compression", default="SNAPPY", 
                   choices=["SNAPPY", "GZIP", "UNCOMPRESSED"],
                   help="Compression codec for optimized Parquet files (default: SNAPPY)")
    
    args = p.parse_args()

    output_dir = os.path.abspath(args.output_dir)
    os.makedirs(output_dir, exist_ok=True)

    config = load_config(os.path.abspath(args.config))

    if args.tables:
        tables = [t.strip() for t in args.tables.split(',')]
    else:
        tables = TABLE_ORDER

    for table in tables:
        parts = int(config.get(table, 1))
        table_out = os.path.join(output_dir, table)
        os.makedirs(table_out, exist_ok=True)
        print(f"Generating {table}...")

        # Use --parts + --part loop to produce 'parts' pieces and move them into table_out
        for part_idx in range(1, parts + 1):
            tmpdir = tempfile.mkdtemp(prefix=f"tpchgen_{table}_part{part_idx}_")
            try:
                cmd = [
                    args.tpchbin,
                    "--scale-factor", str(args.scale),
                    "--format", "parquet",
                    "--tables", table,
                    "--parts", str(parts),
                    "--part", str(part_idx),
                    "--output-dir", tmpdir
                ]
                subprocess.run(cmd, check=True)

                # Move generated parquet files into final table directory.
                pfiles = find_parquet_files(tmpdir)
                if not pfiles:
                    print(f"Warning: no parquet files produced for {table} part {part_idx}", file=sys.stderr)
                for src in pfiles:
                    # Rename to include part index to avoid collisions
                    base = os.path.basename(src)
                    dst_name = f"part-{part_idx:05d}-{base}"
                    dst = os.path.join(table_out, dst_name)
                    shutil.move(src, dst)
            finally:
                shutil.rmtree(tmpdir, ignore_errors=True)

    print("Generation complete.")
    
    # Always perform data type optimization and rewrite with PySpark
    print("Starting optimization...")
    
    rewrite_parquet_with_dict_encoding(output_dir, output_dir, args.use_dict, tables, args.compression, args.spark_config)
    
    print(f"Complete! Optimized files are in: {output_dir}")

if __name__ == '__main__':
    main()

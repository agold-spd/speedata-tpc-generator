#!/usr/bin/env python3
"""Complete TPC-H benchmark setup script.

This script generates both TPC-H data and queries in one command,
creating a complete benchmark environment ready for execution.
"""
import argparse
import os
import subprocess
import sys

def run_command(cmd, description):
    """Run a command and handle errors."""
    print(f"Starting {description}...")
    
    try:
        result = subprocess.run(cmd, check=True, capture_output=True, text=True)
        print(f"{description} completed successfully")
        if result.stdout:
            print(result.stdout)
        return True
    except subprocess.CalledProcessError as e:
        print(f"{description} failed!")
        print(f"Error: {e.stderr}")
        return False

def main():
    parser = argparse.ArgumentParser(description="Generate complete TPC-H benchmark environment")
    parser.add_argument("output_dir", help="Base output directory for data and queries")
    parser.add_argument("--scale", type=int, default=1, 
                       help="TPC-H scale factor (default: 1)")
    parser.add_argument("--tables", help="Comma-separated list of tables to generate")
    parser.add_argument("--streams", type=int, default=1,
                       help="Number of query streams to generate (default: 1)")
    parser.add_argument("--compression", default="SNAPPY",
                       choices=["SNAPPY", "GZIP", "UNCOMPRESSED"],
                       help="Parquet compression codec (default: SNAPPY)")
    parser.add_argument("--no-dict", action="store_true",
                       help="Disable dictionary encoding")
    parser.add_argument("--tpchbin", default="tpchgen-cli",
                       help="Path to tpchgen-cli binary")
    parser.add_argument("--data-only", action="store_true",
                       help="Generate only data, skip queries")
    parser.add_argument("--queries-only", action="store_true", 
                       help="Generate only queries, skip data")
    parser.add_argument("--seed", type=int, help="Random seed for query parameters")
    
    args = parser.parse_args()
    
    if args.data_only and args.queries_only:
        print("Error: Cannot specify both --data-only and --queries-only")
        sys.exit(1)
    
    # Create output directories
    data_dir = os.path.join(args.output_dir, "data")
    queries_dir = os.path.join(args.output_dir, "queries")
    
    os.makedirs(args.output_dir, exist_ok=True)
    
    success = True
    
    # Generate data
    if not args.queries_only:
        print("Starting TPC-H benchmark generation")
        print(f"Scale factor: {args.scale}")
        print(f"Output directory: {args.output_dir}")
        
        data_cmd = [
            sys.executable, "scripts/generate_tpch.py", data_dir,
            "--scale", str(args.scale),
            "--compression", args.compression,
            "--tpchbin", args.tpchbin
        ]
        
        if args.tables:
            data_cmd.extend(["--tables", args.tables])
        
        if args.no_dict:
            data_cmd.append("--no-dict")
        
        success &= run_command(data_cmd, f"TPC-H data generation (scale {args.scale})")
    
    # Generate queries
    if not args.data_only:
        query_cmd = [
            sys.executable, "scripts/generate_tpch_queries.py", queries_dir,
            "--streams", str(args.streams)
        ]
        
        if args.seed:
            query_cmd.extend(["--seed", str(args.seed)])
        
        success &= run_command(query_cmd, f"TPC-H query generation (SQL format)")
    
    # Summary
    if success:
        print("TPC-H benchmark generation completed successfully")
        print("Generated files:")
        
        if not args.queries_only:
            print(f"Data: {data_dir}")
            if os.path.exists(data_dir):
                tables = [d for d in os.listdir(data_dir) if os.path.isdir(os.path.join(data_dir, d))]
                print(f"Tables: {', '.join(sorted(tables))}")
        
        if not args.data_only:
            print(f"Queries: {queries_dir}")
            if os.path.exists(queries_dir):
                streams = [d for d in os.listdir(queries_dir) if d.startswith("stream_")]
                print(f"Streams: {len(streams)}")
                print("Format: SQL")
        
        print("Ready for benchmarking")
        
        if not args.data_only and not args.queries_only:
            print("Next steps:")
            print("1. Run queries against the generated data")
            print("2. Measure execution times and resource usage")
            print("3. Compare results across different systems")
    else:
        print("TPC-H benchmark generation failed")
        sys.exit(1)

if __name__ == "__main__":
    main()

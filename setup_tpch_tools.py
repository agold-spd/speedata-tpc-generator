#!/usr/bin/env python3
"""
Setup script to compile TPC-H tools for query generation.
This script downloads and compiles the official TPC-H tools for the current platform.
"""
import os
import subprocess
import sys
import tempfile
import shutil
import platform
from pathlib import Path

def detect_platform():
    """Detect the current platform and return appropriate compile flags."""
    system = platform.system().lower()
    machine = platform.machine().lower()
    
    if system == "darwin":  # macOS
        return "MACOS", "gcc"
    elif system == "linux":
        return "LINUX", "gcc"
    elif system == "windows":
        return "WIN32", "gcc"  # Assuming MinGW/MSYS2
    else:
        return "LINUX", "gcc"  # Default fallback

def fix_malloc_includes(file_path):
    """Fix malloc.h includes for macOS/BSD systems."""
    with open(file_path, 'r') as f:
        content = f.read()
    
    # Replace malloc.h include with stdlib.h for macOS
    if '#include <malloc.h>' in content:
        content = content.replace(
            '#include <malloc.h>',
            '#ifdef __APPLE__\n#include <stdlib.h>\n#else\n#include <malloc.h>\n#endif'
        )
        
        with open(file_path, 'w') as f:
            f.write(content)

def compile_tpch_tools():
    """Download and compile TPC-H tools."""
    print("Setting up TPC-H tools for query generation...")
    
    # Detect platform
    machine_def, compiler = detect_platform()
    print(f"Detected platform: {machine_def}")
    
    # Create temporary directory
    with tempfile.TemporaryDirectory() as temp_dir:
        print("Downloading TPC-H tools...")
        
        # Clone the tpch-kit repository
        try:
            subprocess.run([
                "git", "clone", "--depth", "1", 
                "https://github.com/gregrahn/tpch-kit.git", 
                os.path.join(temp_dir, "tpch-kit")
            ], check=True, capture_output=True)
        except subprocess.CalledProcessError:
            print("Error: Failed to download TPC-H tools. Please check your internet connection.")
            sys.exit(1)
        
        dbgen_dir = os.path.join(temp_dir, "tpch-kit", "dbgen")
        
        # Fix malloc.h issues for macOS
        if machine_def == "MACOS":
            print("Applying macOS compatibility fixes...")
            fix_malloc_includes(os.path.join(dbgen_dir, "bm_utils.c"))
            fix_malloc_includes(os.path.join(dbgen_dir, "varsub.c"))
        
        # Compile qgen
        print("Compiling TPC-H tools...")
        
        env = os.environ.copy()
        env['CC'] = compiler
        env['MACHINE'] = machine_def
        env['DATABASE'] = 'POSTGRESQL'  # Generic SQL database
        env['WORKLOAD'] = 'TPCH'
        
        try:
            # Compile qgen only (we don't need dbgen since we use tpchgen-cli)
            result = subprocess.run([
                "make", "qgen"
            ], cwd=dbgen_dir, env=env, capture_output=True, text=True)
            
            if result.returncode != 0:
                print(f"Compilation failed:")
                print(result.stderr)
                sys.exit(1)
                
        except FileNotFoundError:
            print("Error: 'make' command not found. Please install build tools:")
            if machine_def == "MACOS":
                print("  Install Xcode Command Line Tools: xcode-select --install")
            elif machine_def == "LINUX":
                print("  Install build-essential: sudo apt-get install build-essential")
            elif machine_def == "WIN32":
                print("  Install MinGW-w64 or use WSL with build-essential")
            sys.exit(1)
        
        # Copy compiled tools to bin directory
        project_root = Path(__file__).parent
        bin_dir = project_root / "bin"
        bin_dir.mkdir(exist_ok=True)
        
        # Copy qgen
        qgen_src = os.path.join(dbgen_dir, "qgen")
        if machine_def == "WIN32":
            qgen_src += ".exe"
        
        if os.path.exists(qgen_src):
            qgen_dst = bin_dir / ("qgen.exe" if machine_def == "WIN32" else "qgen")
            shutil.copy2(qgen_src, qgen_dst)
            # Make executable on Unix systems
            if machine_def != "WIN32":
                os.chmod(qgen_dst, 0o755)
            print(f"✓ Compiled and installed qgen to {qgen_dst}")
        else:
            print("Error: qgen compilation failed - executable not found")
            sys.exit(1)
        
        # Copy required data files
        dists_src = os.path.join(dbgen_dir, "dists.dss")
        if os.path.exists(dists_src):
            shutil.copy2(dists_src, bin_dir / "dists.dss")
            print(f"✓ Copied dists.dss to {bin_dir}")
        
        print(f"\n✓ TPC-H tools successfully compiled for {machine_def}")
        print(f"✓ Tools installed in: {bin_dir}")
        print("\nYou can now use the query generation script:")
        print("  python3 scripts/generate_tpch_queries.py queries/")

def main():
    if len(sys.argv) > 1 and sys.argv[1] in ["--help", "-h"]:
        print(__doc__)
        return
    
    compile_tpch_tools()

if __name__ == "__main__":
    main()

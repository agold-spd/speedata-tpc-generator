#!/usr/bin/env python3
"""Generate TPC-H benchmark queries with parameter substitution.

This script implements the TPC-H parameter substitution logic using the official
query templates, providing cross-platform compatibility without external tools.
"""
import argparse
import os
import random
import re
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Any

# TPC-H Parameter definitions based on the official specification
TPCH_PARAMETERS = {
    1: {
        "name": "Pricing Summary Report",
        "params": {
            1: {"type": "days", "min": 60, "max": 120}  # DELTA days back from 1998-12-01
        }
    },
    2: {
        "name": "Minimum Cost Supplier", 
        "params": {
            1: {"type": "size", "values": [1, 2, 3, 4, 5, 10, 14, 15, 16, 17, 18, 19, 20, 25, 30, 35, 40, 45, 49, 50]},
            2: {"type": "type", "values": ["BRASS", "COPPER", "NICKEL", "STEEL", "TIN"]},
            3: {"type": "region", "values": ["AFRICA", "AMERICA", "ASIA", "EUROPE", "MIDDLE EAST"]}
        }
    },
    3: {
        "name": "Shipping Priority",
        "params": {
            1: {"type": "segment", "values": ["AUTOMOBILE", "BUILDING", "FURNITURE", "HOUSEHOLD", "MACHINERY"]},
            2: {"type": "date", "min": 1, "max": 31}  # Days in March 1995
        }
    },
    4: {
        "name": "Order Priority Checking",
        "params": {
            1: {"type": "date_quarter", "quarters": [
                "1993-01-01", "1993-04-01", "1993-07-01", "1993-10-01",
                "1994-01-01", "1994-04-01", "1994-07-01", "1994-10-01",
                "1995-01-01", "1995-04-01", "1995-07-01", "1995-10-01",
                "1996-01-01", "1996-04-01", "1996-07-01", "1996-10-01",
                "1997-01-01", "1997-04-01", "1997-07-01", "1997-10-01"
            ]}
        }
    },
    5: {
        "name": "Local Supplier Volume",
        "params": {
            1: {"type": "region", "values": ["AFRICA", "AMERICA", "ASIA", "EUROPE", "MIDDLE EAST"]},
            2: {"type": "year", "values": ["1993", "1994", "1995", "1996", "1997"]}
        }
    },
    6: {
        "name": "Forecasting Revenue Change",
        "params": {
            1: {"type": "year", "values": ["1993", "1994", "1995", "1996", "1997"]},
            2: {"type": "discount", "min": 0.02, "max": 0.09, "precision": 2},
            3: {"type": "quantity", "min": 24, "max": 25}
        }
    },
    7: {
        "name": "Volume Shipping",
        "params": {
            1: {"type": "nation", "values": ["ALGERIA", "ARGENTINA", "BRAZIL", "CANADA", "EGYPT", "ETHIOPIA", "FRANCE", "GERMANY", "INDIA", "INDONESIA", "IRAN", "IRAQ", "JAPAN", "JORDAN", "KENYA", "MOROCCO", "MOZAMBIQUE", "PERU", "CHINA", "ROMANIA", "SAUDI ARABIA", "VIETNAM", "RUSSIA", "UNITED KINGDOM", "UNITED STATES"]},
            2: {"type": "nation", "values": ["ALGERIA", "ARGENTINA", "BRAZIL", "CANADA", "EGYPT", "ETHIOPIA", "FRANCE", "GERMANY", "INDIA", "INDONESIA", "IRAN", "IRAQ", "JAPAN", "JORDAN", "KENYA", "MOROCCO", "MOZAMBIQUE", "PERU", "CHINA", "ROMANIA", "SAUDI ARABIA", "VIETNAM", "RUSSIA", "UNITED KINGDOM", "UNITED STATES"]}
        }
    },
    8: {
        "name": "National Market Share",
        "params": {
            1: {"type": "nation", "values": ["ALGERIA", "ARGENTINA", "BRAZIL", "CANADA", "EGYPT", "ETHIOPIA", "FRANCE", "GERMANY", "INDIA", "INDONESIA", "IRAN", "IRAQ", "JAPAN", "JORDAN", "KENYA", "MOROCCO", "MOZAMBIQUE", "PERU", "CHINA", "ROMANIA", "SAUDI ARABIA", "VIETNAM", "RUSSIA", "UNITED KINGDOM", "UNITED STATES"]},
            2: {"type": "region", "values": ["AFRICA", "AMERICA", "ASIA", "EUROPE", "MIDDLE EAST"]},
            3: {"type": "type", "values": ["BRASS", "COPPER", "NICKEL", "STEEL", "TIN"]}
        }
    },
    9: {
        "name": "Product Type Profit Measure",
        "params": {
            1: {"type": "color", "values": ["almond", "antique", "aquamarine", "azure", "beige", "bisque", "black", "blanched", "blue", "blush", "brown", "burlywood", "burnished", "chartreuse", "chiffon", "chocolate", "coral", "cornflower", "cornsilk", "cream", "cyan", "dark", "deep", "dim", "dodger", "drab", "firebrick", "floral", "forest", "frosted", "gainsboro", "ghost", "goldenrod", "green", "grey", "honeydew", "hot", "indian", "ivory", "khaki", "lace", "lavender", "lawn", "lemon", "light", "lime", "linen", "magenta", "maroon", "medium", "metallic", "midnight", "mint", "misty", "moccasin", "navajo", "navy", "olive", "orange", "orchid", "pale", "papaya", "peach", "peru", "pink", "plum", "powder", "puff", "purple", "red", "rose", "rosy", "royal", "saddle", "salmon", "sandy", "seashell", "sienna", "sky", "slate", "smoke", "snow", "spring", "steel", "tan", "thistle", "tomato", "turquoise", "violet", "wheat", "white", "yellow"]}
        }
    },
    10: {
        "name": "Returned Item Reporting",
        "params": {
            1: {"type": "date_month", "months": [
                "1993-01-01", "1993-02-01", "1993-03-01", "1993-04-01", "1993-05-01", "1993-06-01",
                "1993-07-01", "1993-08-01", "1993-09-01", "1993-10-01", "1993-11-01", "1993-12-01",
                "1994-01-01", "1994-02-01", "1994-03-01", "1994-04-01", "1994-05-01", "1994-06-01",
                "1994-07-01", "1994-08-01", "1994-09-01", "1994-10-01", "1994-11-01", "1994-12-01",
                "1995-01-01", "1995-02-01", "1995-03-01", "1995-04-01", "1995-05-01", "1995-06-01",
                "1995-07-01", "1995-08-01", "1995-09-01", "1995-10-01", "1995-11-01", "1995-12-01"
            ]}
        }
    },
    11: {
        "name": "Important Stock Identification",
        "params": {
            1: {"type": "nation", "values": ["ALGERIA", "ARGENTINA", "BRAZIL", "CANADA", "EGYPT", "ETHIOPIA", "FRANCE", "GERMANY", "INDIA", "INDONESIA", "IRAN", "IRAQ", "JAPAN", "JORDAN", "KENYA", "MOROCCO", "MOZAMBIQUE", "PERU", "CHINA", "ROMANIA", "SAUDI ARABIA", "VIETNAM", "RUSSIA", "UNITED KINGDOM", "UNITED STATES"]},
            2: {"type": "fraction", "value": 0.0001}
        }
    },
    12: {
        "name": "Shipping Modes and Order Priority",
        "params": {
            1: {"type": "shipmode", "values": ["MAIL", "SHIP"]},
            2: {"type": "shipmode", "values": ["AIR", "AIR REG", "FOB", "RAIL", "REG AIR", "TRUCK"]},
            3: {"type": "year", "values": ["1993", "1994", "1995", "1996", "1997"]}
        }
    },
    13: {
        "name": "Customer Distribution",
        "params": {
            1: {"type": "word1", "values": ["special", "pending", "unusual", "express"]},
            2: {"type": "word2", "values": ["packages", "requests", "accounts", "deposits"]}
        }
    },
    14: {
        "name": "Promotion Effect",
        "params": {
            1: {"type": "date_month", "months": [
                "1993-01-01", "1994-01-01", "1995-01-01", "1996-01-01", "1997-01-01"
            ]}
        }
    },
    15: {
        "name": "Top Supplier",
        "params": {
            1: {"type": "date_quarter", "quarters": [
                "1996-01-01", "1996-04-01", "1996-07-01", "1996-10-01"
            ]}
        }
    },
    16: {
        "name": "Parts/Supplier Relationship",
        "params": {
            1: {"type": "brand", "min": 1, "max": 5},
            2: {"type": "type", "values": ["BRASS", "COPPER", "NICKEL", "STEEL", "TIN"]},
            3: {"type": "size_list", "sizes": [3, 9, 14, 19, 23, 36, 45, 49]}
        }
    },
    17: {
        "name": "Small-Quantity-Order Revenue",
        "params": {
            1: {"type": "brand", "min": 1, "max": 5},
            2: {"type": "container", "values": ["SM CASE", "SM BOX", "SM PACK", "SM PKG"]}
        }
    },
    18: {
        "name": "Large Volume Customer",
        "params": {
            1: {"type": "quantity", "min": 312, "max": 315}
        }
    },
    19: {
        "name": "Discounted Revenue",
        "params": {
            1: {"type": "quantity1", "min": 1, "max": 10},
            2: {"type": "quantity2", "min": 10, "max": 20},
            3: {"type": "quantity3", "min": 20, "max": 30},
            4: {"type": "brand1", "min": 1, "max": 5},
            5: {"type": "brand2", "min": 1, "max": 5},
            6: {"type": "brand3", "min": 1, "max": 5}
        }
    },
    20: {
        "name": "Potential Part Promotion",
        "params": {
            1: {"type": "color", "values": ["almond", "antique", "aquamarine", "azure", "beige", "bisque", "black", "blanched", "blue", "blush", "brown", "burlywood", "burnished", "chartreuse", "chiffon", "chocolate", "coral", "cornflower", "cornsilk", "cream", "cyan", "dark", "deep", "dim", "dodger", "drab", "firebrick", "floral", "forest", "frosted", "gainsboro", "ghost", "goldenrod", "green", "grey", "honeydew", "hot", "indian", "ivory", "khaki", "lace", "lavender", "lawn", "lemon", "light", "lime", "linen", "magenta", "maroon", "medium", "metallic", "midnight", "mint", "misty", "moccasin", "navajo", "navy", "olive", "orange", "orchid", "pale", "papaya", "peach", "peru", "pink", "plum", "powder", "puff", "purple", "red", "rose", "rosy", "royal", "saddle", "salmon", "sandy", "seashell", "sienna", "sky", "slate", "smoke", "snow", "spring", "steel", "tan", "thistle", "tomato", "turquoise", "violet", "wheat", "white", "yellow"]},
            2: {"type": "date_year", "values": ["1993-01-01", "1994-01-01", "1995-01-01", "1996-01-01", "1997-01-01"]},
            3: {"type": "nation", "values": ["ALGERIA", "ARGENTINA", "BRAZIL", "CANADA", "EGYPT", "ETHIOPIA", "FRANCE", "GERMANY", "INDIA", "INDONESIA", "IRAN", "IRAQ", "JAPAN", "JORDAN", "KENYA", "MOROCCO", "MOZAMBIQUE", "PERU", "CHINA", "ROMANIA", "SAUDI ARABIA", "VIETNAM", "RUSSIA", "UNITED KINGDOM", "UNITED STATES"]}
        }
    },
    21: {
        "name": "Suppliers Who Kept Orders Waiting",
        "params": {
            1: {"type": "nation", "values": ["ALGERIA", "ARGENTINA", "BRAZIL", "CANADA", "EGYPT", "ETHIOPIA", "FRANCE", "GERMANY", "INDIA", "INDONESIA", "IRAN", "IRAQ", "JAPAN", "JORDAN", "KENYA", "MOROCCO", "MOZAMBIQUE", "PERU", "CHINA", "ROMANIA", "SAUDI ARABIA", "VIETNAM", "RUSSIA", "UNITED KINGDOM", "UNITED STATES"]}
        }
    },
    22: {
        "name": "Global Sales Opportunity",
        "params": {
            1: {"type": "country_code", "values": ["13", "31", "23", "29", "30", "18", "17"]},
            2: {"type": "country_code", "values": ["13", "31", "23", "29", "30", "18", "17"]},
            3: {"type": "country_code", "values": ["13", "31", "23", "29", "30", "18", "17"]},
            4: {"type": "country_code", "values": ["13", "31", "23", "29", "30", "18", "17"]},
            5: {"type": "country_code", "values": ["13", "31", "23", "29", "30", "18", "17"]},
            6: {"type": "country_code", "values": ["13", "31", "23", "29", "30", "18", "17"]},
            7: {"type": "country_code", "values": ["13", "31", "23", "29", "30", "18", "17"]}
        }
    }
}

def generate_parameter_value(param_def: Dict[str, Any], random_gen: random.Random) -> str:
    """Generate a parameter value based on its definition."""
    param_type = param_def["type"]
    
    if "values" in param_def:
        return random_gen.choice(param_def["values"])
    elif "min" in param_def and "max" in param_def:
        if param_type == "discount":
            value = random_gen.uniform(param_def["min"], param_def["max"])
            return f"{value:.{param_def.get('precision', 2)}f}"
        elif param_type in ["quantity", "size", "brand", "quantity1", "quantity2", "quantity3", "brand1", "brand2", "brand3"]:
            return str(random_gen.randint(param_def["min"], param_def["max"]))
        elif param_type == "days":
            # For Q1: days back from 1998-12-01
            days_back = random_gen.randint(param_def["min"], param_def["max"])
            return str(days_back)
        elif param_type == "date":
            # For Q3: days in March 1995
            day = random_gen.randint(param_def["min"], param_def["max"])
            return f"1995-03-{day:02d}"
    elif param_type == "fraction":
        return str(param_def["value"])
    elif param_type == "date_quarter":
        return random_gen.choice(param_def["quarters"])
    elif param_type == "date_month":
        return random_gen.choice(param_def["months"])
    elif param_type == "date_year":
        return random_gen.choice(param_def["values"])
    elif param_type == "year":
        return random_gen.choice(param_def["values"])
    elif param_type == "size_list":
        # Select random subset of sizes
        subset_size = random_gen.randint(3, min(8, len(param_def["sizes"])))
        selected = random_gen.sample(param_def["sizes"], subset_size)
        return ", ".join(map(str, sorted(selected)))
    
    return ""

def substitute_template(template_content: str, query_num: int, seed: int) -> str:
    """Substitute parameters in a TPC-H query template."""
    if query_num not in TPCH_PARAMETERS:
        raise ValueError(f"Query {query_num} not supported")
    
    # Create seeded random generator for reproducible results
    random_gen = random.Random(seed)
    
    # Generate parameter values
    param_values = {}
    query_def = TPCH_PARAMETERS[query_num]
    
    for param_num, param_def in query_def["params"].items():
        param_values[param_num] = generate_parameter_value(param_def, random_gen)
    
    # Remove qgen directives and substitute parameters
    lines = template_content.split('\n')
    result_lines = []
    
    for line in lines:
        # Skip qgen directive lines that are standalone
        if re.match(r'^\s*:[xobn]\s*$', line):
            continue
        if re.match(r'^\s*:n\s+-?\d+\s*$', line):
            continue
        
        # Process the line for parameter substitution
        modified_line = line
        
        # Substitute numbered parameters (:1, :2, etc.)
        for param_num, value in param_values.items():
            pattern = f':{param_num}'
            if pattern in modified_line:
                modified_line = modified_line.replace(pattern, str(value))
        
        # Handle special directives that appear inline
        # :s (stream number) - replace with 0 for simplicity
        modified_line = re.sub(r':s\b', '0', modified_line)
        
        # :q (query number) - replace with actual query number
        modified_line = re.sub(r':q\b', str(query_num), modified_line)
        
        result_lines.append(modified_line)
    
    # Join lines and clean up
    result = '\n'.join(result_lines)
    
    return result.strip()

def get_template_dir():
    """Get the directory containing query templates."""
    script_dir = Path(__file__).parent.absolute()
    project_root = script_dir.parent
    
    template_dir = project_root / "templates"
    if template_dir.exists() and (template_dir / "1.sql").exists():
        return str(template_dir)
    
    return None

def generate_queries(output_dir: str, seed: int = 1):
    """Generate all 22 TPC-H queries."""
    template_dir = get_template_dir()
    if not template_dir:
        print("Error: Query templates not found in templates/ directory")
        return False
    
    os.makedirs(output_dir, exist_ok=True)
    
    # Generate all 22 queries
    for query_num in range(1, 23):
        template_file = os.path.join(template_dir, f"{query_num}.sql")
        output_file = os.path.join(output_dir, f"q{query_num:02d}.sql")
        
        if not os.path.exists(template_file):
            print(f"Warning: Template {template_file} not found, skipping query {query_num}")
            continue
        
        try:
            # Read template
            with open(template_file, 'r') as f:
                template_content = f.read()
            
            # Generate query with different seed for each query for variety
            query_seed = seed + query_num - 1
            substituted_query = substitute_template(template_content, query_num, query_seed)
            
            # Write output
            with open(output_file, 'w') as f:
                f.write(f"-- TPC-H Query {query_num}: {TPCH_PARAMETERS[query_num]['name']}\n")
                f.write(f"-- Generated with seed: {query_seed}\n\n")
                f.write(substituted_query)
                f.write('\n')
            
            print(f"Generated q{query_num:02d}.sql")
            
        except Exception as e:
            print(f"Error generating query {query_num}: {e}")
            return False
    
    print(f"Generated 22 TPC-H queries in: {output_dir}")
    return True

def generate_single_query(query_num: int, seed: int = 1):
    """Generate a single query and print to stdout."""
    template_dir = get_template_dir()
    if not template_dir:
        print("Error: Query templates not found in templates/ directory")
        return False
    
    template_file = os.path.join(template_dir, f"{query_num}.sql")
    
    if not os.path.exists(template_file):
        print(f"Error: Template {template_file} not found")
        return False
    
    try:
        # Read template
        with open(template_file, 'r') as f:
            template_content = f.read()
        
        # Generate query
        substituted_query = substitute_template(template_content, query_num, seed)
        
        # Print to stdout
        print(substituted_query)
        return True
        
    except Exception as e:
        print(f"Error generating query {query_num}: {e}")
        return False

def main():
    parser = argparse.ArgumentParser(description="Generate TPC-H benchmark queries with parameter substitution")
    parser.add_argument("output_dir", help="Output directory for generated queries")
    parser.add_argument("--seed", type=int, default=1, help="Random seed for parameter generation")
    parser.add_argument("--query", type=int, choices=range(1, 23), metavar="1-22",
                       help="Generate single query instead of full suite")
    
    args = parser.parse_args()
    
    if args.query:
        # Generate single query
        success = generate_single_query(args.query, args.seed)
    else:
        # Generate all queries
        success = generate_queries(args.output_dir, args.seed)
    
    exit(0 if success else 1)

if __name__ == "__main__":
    main()

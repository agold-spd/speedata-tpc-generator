# TPC-H Query Templates

This directory contains templates for all 22 standard TPC-H benchmark queries with parameter substitution support.

## Query List

1. **Pricing Summary Report** - Summary of line items by return flag and line status
2. **Minimum Cost Supplier** - Find suppliers with minimum cost for each part/region combination
3. **Shipping Priority** - Get the 10 unshipped orders with the highest value
4. **Order Priority Checking** - Count orders by priority in a given time period
5. **Local Supplier Volume** - List revenue volume done through local suppliers
6. **Forecasting Revenue Change** - Quantify revenue increase from eliminating discounts
7. **Volume Shipping** - Determine revenue from orders shipped between certain dates
8. **National Market Share** - Determine market share for a given nation within a region
9. **Product Type Profit Measure** - Determine profit for parts ordered in a year by nation
10. **Returned Item Reporting** - Identify customers who might be having problems with parts
11. **Important Stock Identification** - Find most important subset of suppliers' stock
12. **Shipping Modes and Order Priority** - Determine whether selecting less expensive shipping modes
13. **Customer Distribution** - Count customers by country code and phone number patterns
14. **Promotion Effect** - Monitor market response to a promotion
15. **Top Supplier** - Determine top supplier so it can be rewarded or recognized
16. **Parts/Supplier Relationship** - Find parts and suppliers relationships
17. **Small-Quantity-Order Revenue** - Determine how much average yearly revenue
18. **Large Volume Customer** - Rank customers based on total revenue
19. **Discounted Revenue** - Calculate potential revenue increase from discount elimination
20. **Suppliers Who Kept Orders Waiting** - Identify suppliers causing delays
21. **Suppliers Who Satisfied Orders** - Suppliers who satisfied orders for a multipart order
22. **Global Sales Opportunity** - Identify global sales opportunities

## Parameter Types

- **delta**: Date offset from base date (1992-01-01)
- **size**: Part size (1-50)
- **type**: Part type suffix (BRASS, COPPER, NICKEL, STEEL, TIN)
- **region**: Region name (AFRICA, AMERICA, ASIA, EUROPE, MIDDLE EAST)
- **segment**: Market segment (AUTOMOBILE, BUILDING, FURNITURE, HOUSEHOLD, MACHINERY)
- **date**: Random date within TPC-H date range
- **discount**: Discount percentage (0.02-0.09)
- **quantity**: Quantity threshold (typically 24-25)
- **nation1/nation2**: Nation names for comparison queries

## Usage

The query generator supports multiple SQL engines and can adapt table references for different data storage formats (database tables vs. Parquet files).

See `generate_tpch_queries.py --help` for detailed usage instructions.

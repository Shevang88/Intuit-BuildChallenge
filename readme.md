# Intuit Build Challenge (Python)

This repo has two small apps:
- A producer-consumer example that shows thread coordination with a bounded buffer.
- A sales CSV analyzer that prints common rollups like totals, monthly numbers, and top subcategories.

## Quick start
- Run everything: `python3 -m unittest -v`
- Producer-consumer demo: `python3 producer_consumer.py` (prints the final destination list)
- Sales report: `python3 sales_analysis.py` (reads the CSV and prints a console report)

## Producer-consumer (Assignment 1)
- Uses a `BoundedBuffer` with a `Condition` so producers wait when the buffer is full and consumers wait when it is empty.
- `Producer` threads read from a source list and push items; `Consumer` threads pull items, can transform them, and store them.
- Clean shutdown: producers can close the buffer; consumers exit when it closes or when told to stop.

## Sales analysis (Assignment 2)
- Data file: `data/superstore_sales.csv` (500 synthetic 2023 orders: region, category, subcategory, sales, quantity, profit).
- Main helpers in `sales_analysis.py`: `load_sales`, `total_sales`, `total_profit`, `monthly_sales`, `sales_by_category`, `profit_by_region`, `category_profit_margin`, `region_category_sales`, `top_subcategories_by_sales`, `top_orders_by_profit`.
- Console report: `python3 sales_analysis.py` prints all the above in one go.

### Sample output (shortened)
```
Loaded 500 records from data/superstore_sales.csv

Totals
  Total sales : $557,998.71
  Total profit: $79,657.96

Top subcategories by sales
  Copiers: $195,249.47
  Machines: $106,707.60
  Phones: $67,794.34
  Tables: $49,885.40
  Bookcases: $31,136.95
```


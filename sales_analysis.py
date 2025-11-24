"""Analytics on Superstore-style sales CSV using functional-style operations."""
from __future__ import annotations

import csv
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Dict, Iterable, List, Sequence, Tuple


@dataclass(frozen=True)
class SaleRecord:
    order_id: str
    order_date: date
    region: str
    category: str
    subcategory: str
    sales: float
    quantity: int
    profit: float


def load_sales(path: str | Path) -> List[SaleRecord]:
    """Read CSV rows into SaleRecord objects."""
    records: List[SaleRecord] = []
    with Path(path).open(newline="") as f:
        reader = csv.DictReader(f)
        for idx, row in enumerate(reader, start=1):
            try:
                records.append(
                    SaleRecord(
                        order_id=row["order_id"],
                        order_date=datetime.fromisoformat(row["order_date"]).date(),
                        region=row["region"],
                        category=row["category"],
                        subcategory=row["subcategory"],
                        sales=float(row["sales"]),
                        quantity=int(row["quantity"]),
                        profit=float(row["profit"]),
                    )
                )
            except Exception as exc:  # pragma: no cover - defensive path
                raise ValueError(f"bad row {idx}: {row}") from exc
    return records


def total_sales(records: Iterable[SaleRecord]) -> float:
    return sum(r.sales for r in records)


def total_profit(records: Iterable[SaleRecord]) -> float:
    return sum(r.profit for r in records)


def sales_by_category(records: Iterable[SaleRecord]) -> Dict[str, float]:
    return _sum_by(records, key_fn=lambda r: r.category, value_fn=lambda r: r.sales)


def profit_by_region(records: Iterable[SaleRecord]) -> Dict[str, float]:
    return _sum_by(records, key_fn=lambda r: r.region, value_fn=lambda r: r.profit)


def monthly_sales(records: Iterable[SaleRecord]) -> Dict[str, float]:
    return _sum_by(
        records,
        key_fn=lambda r: r.order_date.strftime("%Y-%m"),
        value_fn=lambda r: r.sales,
    )


def top_subcategories_by_sales(
    records: Iterable[SaleRecord], *, top_n: int = 5
) -> List[Tuple[str, float]]:
    totals = _sum_by(records, key_fn=lambda r: r.subcategory, value_fn=lambda r: r.sales)
    return sorted(totals.items(), key=lambda item: (-item[1], item[0]))[:top_n]


def category_profit_margin(records: Iterable[SaleRecord]) -> Dict[str, float]:
    totals = _sum_counts(records, key_fn=lambda r: r.category, value_fn=lambda r: (r.profit, r.sales))
    margins: Dict[str, float] = {}
    for cat, (profit_sum, sales_sum) in totals.items():
        margins[cat] = 0.0 if sales_sum == 0 else profit_sum / sales_sum
    return margins


def region_category_sales(records: Iterable[SaleRecord]) -> Dict[Tuple[str, str], float]:
    return _sum_by(
        records,
        key_fn=lambda r: (r.region, r.category),
        value_fn=lambda r: r.sales,
    )


def top_orders_by_profit(
    records: Sequence[SaleRecord], *, top_n: int = 3
) -> List[SaleRecord]:
    return sorted(records, key=lambda r: r.profit, reverse=True)[:top_n]


def _sum_by(
    records: Iterable[SaleRecord], key_fn, value_fn
) -> Dict:
    totals: Dict = {}
    for r in records:
        key = key_fn(r)
        totals[key] = totals.get(key, 0.0) + value_fn(r)
    return totals


def _sum_counts(records: Iterable[SaleRecord], key_fn, value_fn) -> Dict:
    totals: Dict = {}
    for r in records:
        key = key_fn(r)
        profit, sales = value_fn(r)
        if key not in totals:
            totals[key] = (0.0, 0.0)
        p_sum, s_sum = totals[key]
        totals[key] = (p_sum + profit, s_sum + sales)
    return totals


def run_console_report(path: str | Path = "data/superstore_sales.csv") -> None:
    """Load CSV and print a suite of analyses to stdout."""
    records = load_sales(path)

    print(f"Loaded {len(records)} records from {path}\n")

    print("Totals")
    print(f"  Total sales : ${total_sales(records):,.2f}")
    print(f"  Total profit: ${total_profit(records):,.2f}\n")

    print("Monthly sales (YYYY-MM)")
    for month, value in sorted(monthly_sales(records).items()):
        print(f"  {month}: ${value:,.2f}")
    print()

    print("Top subcategories by sales")
    for name, value in top_subcategories_by_sales(records, top_n=5):
        print(f"  {name}: ${value:,.2f}")
    print()

    print("Sales by category")
    for cat, value in sorted(sales_by_category(records).items()):
        print(f"  {cat}: ${value:,.2f}")
    print()

    print("Profit by region")
    for region, value in sorted(profit_by_region(records).items()):
        print(f"  {region}: ${value:,.2f}")
    print()

    print("Category profit margin (profit / sales)")
    for cat, margin in sorted(category_profit_margin(records).items()):
        print(f"  {cat}: {margin:.2%}")
    print()

    print("Region + category sales")
    for (region, cat), value in sorted(region_category_sales(records).items()):
        print(f"  {region} / {cat}: ${value:,.2f}")
    print()

    print("Top orders by profit")
    for rec in top_orders_by_profit(records, top_n=5):
        print(
            f"  {rec.order_id} ({rec.order_date.isoformat()} - {rec.region} - {rec.category}/{rec.subcategory}): "
            f"sales=${rec.sales:,.2f}, profit=${rec.profit:,.2f}"
        )


if __name__ == "__main__":
    run_console_report()

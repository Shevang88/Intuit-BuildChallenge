import unittest
from datetime import date

from sales_analysis import (
    SaleRecord,
    category_profit_margin,
    load_sales,
    monthly_sales,
    profit_by_region,
    region_category_sales,
    sales_by_category,
    top_orders_by_profit,
    top_subcategories_by_sales,
    total_profit,
    total_sales,
)


def sample_records() -> list[SaleRecord]:
    return [
        SaleRecord("O1", date(2023, 1, 10), "East", "Technology", "Phones", 1000.0, 3, 200.0),
        SaleRecord("O2", date(2023, 1, 12), "East", "Furniture", "Chairs", 300.0, 4, 60.0),
        SaleRecord("O3", date(2023, 2, 1), "West", "Technology", "Accessories", 150.0, 5, 70.0),
        SaleRecord("O4", date(2023, 2, 15), "Central", "Office Supplies", "Paper", 80.0, 8, 30.0),
        SaleRecord("O5", date(2023, 3, 5), "West", "Furniture", "Tables", 700.0, 2, 90.0),
        SaleRecord("O6", date(2023, 3, 6), "Central", "Technology", "Phones", 400.0, 1, -50.0),
    ]


class LoaderTests(unittest.TestCase):
    def test_loads_full_dataset(self) -> None:
        records = load_sales("data/superstore_sales.csv")
        self.assertEqual(len(records), 500)
        self.assertTrue(all(isinstance(r, SaleRecord) for r in records))


class AggregationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.records = sample_records()

    def test_totals(self) -> None:
        self.assertAlmostEqual(total_sales(self.records), 2630.0)
        self.assertAlmostEqual(total_profit(self.records), 400.0)

    def test_sales_by_category(self) -> None:
        result = sales_by_category(self.records)
        self.assertEqual(result["Technology"], 1550.0)
        self.assertEqual(result["Furniture"], 1000.0)
        self.assertEqual(result["Office Supplies"], 80.0)

    def test_profit_by_region(self) -> None:
        result = profit_by_region(self.records)
        self.assertAlmostEqual(result["East"], 260.0)
        self.assertAlmostEqual(result["West"], 160.0)
        self.assertAlmostEqual(result["Central"], -20.0)

    def test_monthly_sales(self) -> None:
        result = monthly_sales(self.records)
        self.assertEqual(result["2023-01"], 1300.0)
        self.assertEqual(result["2023-02"], 230.0)
        self.assertEqual(result["2023-03"], 1100.0)

    def test_top_subcategories(self) -> None:
        result = top_subcategories_by_sales(self.records, top_n=3)
        self.assertEqual(result[0], ("Phones", 1400.0))
        self.assertEqual(result[1], ("Tables", 700.0))
        self.assertEqual(result[2], ("Chairs", 300.0))

    def test_category_profit_margin(self) -> None:
        margins = category_profit_margin(self.records)
        self.assertAlmostEqual(margins["Technology"], 220.0 / 1550.0)
        self.assertAlmostEqual(margins["Furniture"], 150.0 / 1000.0)
        self.assertAlmostEqual(margins["Office Supplies"], 30.0 / 80.0)

    def test_region_category_sales(self) -> None:
        result = region_category_sales(self.records)
        self.assertEqual(result[("East", "Technology")], 1000.0)
        self.assertEqual(result[("East", "Furniture")], 300.0)
        self.assertEqual(result[("West", "Furniture")], 700.0)
        self.assertEqual(result[("West", "Technology")], 150.0)
        self.assertEqual(result[("Central", "Technology")], 400.0)
        self.assertEqual(result[("Central", "Office Supplies")], 80.0)

    def test_top_orders_by_profit(self) -> None:
        result = top_orders_by_profit(self.records, top_n=2)
        self.assertEqual([r.order_id for r in result], ["O1", "O5"])


class EdgeCaseTests(unittest.TestCase):
    def test_empty_inputs(self) -> None:
        empty: list[SaleRecord] = []
        self.assertEqual(total_sales(empty), 0)
        self.assertEqual(total_profit(empty), 0)
        self.assertEqual(sales_by_category(empty), {})
        self.assertEqual(monthly_sales(empty), {})
        self.assertEqual(top_subcategories_by_sales(empty), [])
        self.assertEqual(region_category_sales(empty), {})
        self.assertEqual(category_profit_margin(empty), {})

    def test_ties_in_top_subcategories_ordered_alphabetically(self) -> None:
        records = [
            SaleRecord("A", date(2023, 1, 1), "East", "Tech", "Alpha", 100.0, 1, 10.0),
            SaleRecord("B", date(2023, 1, 2), "East", "Tech", "Beta", 100.0, 1, 5.0),
            SaleRecord("C", date(2023, 1, 3), "East", "Tech", "Gamma", 50.0, 1, 2.0),
        ]
        result = top_subcategories_by_sales(records, top_n=2)
        self.assertEqual(result, [("Alpha", 100.0), ("Beta", 100.0)])

    def test_category_profit_margin_handles_zero_sales(self) -> None:
        records = [
            SaleRecord("A", date(2023, 1, 1), "East", "Tech", "Alpha", 0.0, 1, 10.0),
            SaleRecord("B", date(2023, 1, 2), "East", "Tech", "Alpha", 200.0, 1, 50.0),
        ]
        margins = category_profit_margin(records)
        self.assertAlmostEqual(margins["Tech"], 60.0 / 200.0)

    def test_region_category_sales_accumulates_duplicates(self) -> None:
        records = [
            SaleRecord("A", date(2023, 1, 1), "East", "Furniture", "Chairs", 100.0, 1, 10.0),
            SaleRecord("B", date(2023, 1, 2), "East", "Furniture", "Chairs", 150.0, 2, 15.0),
            SaleRecord("C", date(2023, 1, 3), "West", "Furniture", "Chairs", 75.0, 1, 7.0),
        ]
        totals = region_category_sales(records)
        self.assertEqual(totals[("East", "Furniture")], 250.0)
        self.assertEqual(totals[("West", "Furniture")], 75.0)

    def test_top_orders_by_profit_with_losses(self) -> None:
        records = [
            SaleRecord("A", date(2023, 1, 1), "East", "Tech", "Alpha", 100.0, 1, 300.0),
            SaleRecord("B", date(2023, 1, 1), "East", "Tech", "Alpha", 100.0, 1, -50.0),
            SaleRecord("C", date(2023, 1, 1), "East", "Tech", "Alpha", 100.0, 1, 25.0),
        ]
        top = top_orders_by_profit(records, top_n=2)
        self.assertEqual([r.order_id for r in top], ["A", "C"])


if __name__ == "__main__":
    unittest.main()

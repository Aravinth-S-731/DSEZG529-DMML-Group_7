import csv
import gzip
import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from gold.common import sha256_file
from gold.features import (
    CUSTOMER_COLUMNS,
    INTERACTION_COLUMNS,
    PRODUCT_FEATURE_COLUMNS,
)
from gold.load_sqlite import load_gold_to_sqlite


class GoldSQLiteTests(unittest.TestCase):
    def setUp(self):
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary_directory.name)
        self.gold_root = self.root / "gold"
        self.gold_root.mkdir()
        self.database_path = self.root / "warehouse" / "gold.sqlite3"
        self._write_gold_files()

    def tearDown(self):
        self.temporary_directory.cleanup()

    def _write_gold_files(self):
        datasets = {
            "customer_features.csv.gz": (
                CUSTOMER_COLUMNS,
                [
                    {
                        "customer_id": "123",
                        "unique_products": "1",
                        "purchase_line_count": "1",
                        "total_units_purchased": "2",
                        "total_spend": "12.50",
                        "first_purchase_at": "2011-01-01T10:00:00",
                        "last_purchase_at": "2011-01-01T10:00:00",
                    }
                ],
            ),
            "product_features.csv.gz": (
                PRODUCT_FEATURE_COLUMNS,
                [
                    {
                        "stock_code": "A1",
                        "description": "Sold item",
                        "catalog_record_count": "2",
                        "is_duplicate_catalog_code": "true",
                        "purchase_line_count": "1",
                        "unique_customers": "1",
                        "units_sold": "2",
                        "gross_sales": "12.50",
                        "first_sold_at": "2011-01-01T10:00:00",
                        "last_sold_at": "2011-01-01T10:00:00",
                        "popularity_rank": "1",
                    },
                    {
                        "stock_code": "B2",
                        "description": "Unsold item",
                        "catalog_record_count": "1",
                        "is_duplicate_catalog_code": "false",
                        "purchase_line_count": "0",
                        "unique_customers": "0",
                        "units_sold": "0",
                        "gross_sales": "0",
                        "first_sold_at": "",
                        "last_sold_at": "",
                        "popularity_rank": "",
                    },
                ],
            ),
            "customer_product_interactions.csv.gz": (
                INTERACTION_COLUMNS,
                [
                    {
                        "customer_id": "123",
                        "stock_code": "A1",
                        "purchase_line_count": "1",
                        "quantity_purchased": "2",
                        "spend": "12.50",
                        "first_purchase_at": "2011-01-01T10:00:00",
                        "last_purchase_at": "2011-01-01T10:00:00",
                    }
                ],
            ),
        }

        entries = []
        for filename, (columns, rows) in datasets.items():
            path = self.gold_root / filename
            with gzip.open(path, "wt", encoding="utf-8", newline="") as output:
                writer = csv.DictWriter(output, fieldnames=columns, lineterminator="\n")
                writer.writeheader()
                writer.writerows(rows)
            entries.append(
                {
                    "file": filename,
                    "sha256": sha256_file(path),
                    "size_bytes": path.stat().st_size,
                    "row_count": len(rows),
                }
            )
        (self.gold_root / "gold_manifest.json").write_text(
            json.dumps({"schema_version": "1", "files": entries}),
            encoding="utf-8",
        )

    def test_loads_verified_tables_and_maps_empty_nullable_values_to_null(self):
        counts = load_gold_to_sqlite(self.gold_root, self.database_path)

        self.assertEqual(
            {
                "gold_customer_features": 1,
                "gold_product_features": 2,
                "gold_customer_product_interactions": 1,
            },
            counts,
        )
        connection = sqlite3.connect(self.database_path)
        try:
            unsold_product = connection.execute(
                "SELECT first_sold_at, last_sold_at, popularity_rank "
                "FROM gold_product_features WHERE stock_code = 'B2'"
            ).fetchone()
            self.assertEqual((None, None, None), unsold_product)
            self.assertEqual(
                [],
                connection.execute("PRAGMA foreign_key_check").fetchall(),
            )
        finally:
            connection.close()

    def test_bad_checksum_does_not_replace_existing_database(self):
        database_path = self.root / "existing.sqlite3"
        connection = sqlite3.connect(database_path)
        try:
            connection.execute("CREATE TABLE marker (value TEXT)")
            connection.execute("INSERT INTO marker VALUES ('keep')")
            connection.commit()
        finally:
            connection.close()

        manifest_path = self.gold_root / "gold_manifest.json"
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        manifest["files"][0]["sha256"] = "0" * 64
        manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

        with self.assertRaisesRegex(ValueError, "SHA-256 mismatch"):
            load_gold_to_sqlite(self.gold_root, database_path)

        connection = sqlite3.connect(database_path)
        try:
            self.assertEqual(
                ("keep",),
                connection.execute("SELECT value FROM marker").fetchone(),
            )
        finally:
            connection.close()


if __name__ == "__main__":
    unittest.main()

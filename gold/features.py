import logging
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path

from gold.common import (
    PRODUCT_COLUMNS,
    TRANSACTION_COLUMNS,
    iter_silver_rows,
    read_silver_manifest,
    write_gzip_csv_atomic,
    write_json_atomic,
)


INTERACTION_COLUMNS = (
    "customer_id",
    "stock_code",
    "purchase_line_count",
    "quantity_purchased",
    "spend",
    "first_purchase_at",
    "last_purchase_at",
)
CUSTOMER_COLUMNS = (
    "customer_id",
    "unique_products",
    "purchase_line_count",
    "total_units_purchased",
    "total_spend",
    "first_purchase_at",
    "last_purchase_at",
)
PRODUCT_FEATURE_COLUMNS = (
    "stock_code",
    "description",
    "catalog_record_count",
    "is_duplicate_catalog_code",
    "purchase_line_count",
    "unique_customers",
    "units_sold",
    "gross_sales",
    "first_sold_at",
    "last_sold_at",
    "popularity_rank",
)


@dataclass
class Interaction:
    purchase_line_count: int = 0
    quantity_purchased: int = 0
    spend: Decimal = Decimal("0")
    first_purchase_at: str = ""
    last_purchase_at: str = ""

    def add(self, quantity: int, spend: Decimal, purchased_at: str) -> None:
        self.purchase_line_count += 1
        self.quantity_purchased += quantity
        self.spend += spend
        if not self.first_purchase_at or purchased_at < self.first_purchase_at:
            self.first_purchase_at = purchased_at
        if not self.last_purchase_at or purchased_at > self.last_purchase_at:
            self.last_purchase_at = purchased_at


def _decimal_text(value: Decimal) -> str:
    if value == 0:
        return "0"
    return format(value.normalize(), "f")


def _read_catalog(
    silver_root: Path,
    manifest: dict,
) -> tuple[dict[str, dict], dict[str, int]]:
    descriptions: dict[str, Counter] = defaultdict(Counter)
    catalog_record_counts: Counter = Counter()
    input_rows = 0

    for row in iter_silver_rows(
        silver_root,
        "product_catalog",
        manifest,
        PRODUCT_COLUMNS,
    ):
        stock_code = (row.get("stock_code") or "").strip().upper()
        if not stock_code:
            raise ValueError("Silver catalog contains an empty stock code")
        description = " ".join((row.get("description") or "").split())
        catalog_record_counts[stock_code] += 1
        if description:
            descriptions[stock_code][description] += 1
        input_rows += 1

    catalog = {}
    for stock_code, record_count in catalog_record_counts.items():
        variants = descriptions.get(stock_code, Counter())
        description = (
            sorted(variants, key=lambda value: (-variants[value], value.casefold(), value))[0]
            if variants
            else ""
        )
        catalog[stock_code] = {
            "description": description,
            "catalog_record_count": record_count,
        }

    expected_rows = manifest.get("silver_rows")
    if isinstance(expected_rows, int) and input_rows != expected_rows:
        raise ValueError(
            f"Product catalog row-count mismatch: manifest states {expected_rows}, "
            f"files contain {input_rows}"
        )
    return catalog, {
        "catalog_rows": input_rows,
        "catalog_products": len(catalog),
        "duplicate_catalog_rows": sum(
            count - 1 for count in catalog_record_counts.values() if count > 1
        ),
    }


def _build_aggregates(
    silver_root: Path,
    manifest: dict,
) -> tuple[dict, dict, dict, dict[str, int]]:
    interactions: dict[tuple[str, str], Interaction] = {}
    metrics: Counter = Counter()

    for row in iter_silver_rows(
        silver_root,
        "online_retail",
        manifest,
        TRANSACTION_COLUMNS,
    ):
        metrics["input_rows"] += 1
        if (row.get("is_recommendation_eligible") or "").strip().lower() != "true":
            metrics["ineligible_rows"] += 1
            continue
        metrics["eligible_rows"] += 1
        if (row.get("is_duplicate_content") or "").strip().lower() == "true":
            metrics["duplicate_rows_excluded"] += 1
            continue

        customer_id = (row.get("customer_id") or "").strip()
        stock_code = (row.get("stock_code") or "").strip().upper()
        purchased_at = (row.get("invoice_datetime") or "").strip()
        if not customer_id or not stock_code or not purchased_at:
            raise ValueError(
                "Recommendation-eligible Silver row is missing a customer, "
                "product, or invoice timestamp"
            )
        try:
            quantity = int(row["quantity"])
            spend = Decimal(row["line_total"])
            datetime.fromisoformat(purchased_at)
        except (KeyError, ValueError, InvalidOperation) as error:
            raise ValueError(
                f"Invalid recommendation-eligible Silver row: {error}"
            ) from error
        if quantity <= 0 or not spend.is_finite() or spend < 0:
            raise ValueError(
                "Recommendation-eligible Silver rows must have positive "
                "quantity and nonnegative line total"
            )

        key = (customer_id, stock_code)
        interaction = interactions.setdefault(key, Interaction())
        interaction.add(quantity, spend, purchased_at)
        metrics["training_rows"] += 1

    expected_rows = manifest.get("silver_rows")
    if isinstance(expected_rows, int) and metrics["input_rows"] != expected_rows:
        raise ValueError(
            f"Transaction row-count mismatch: manifest states {expected_rows}, "
            f"files contain {metrics['input_rows']}"
        )

    customers: dict[str, dict] = {}
    products: dict[str, dict] = {}
    for (customer_id, stock_code), interaction in interactions.items():
        customer = customers.setdefault(
            customer_id,
            {
                "unique_products": 0,
                "purchase_line_count": 0,
                "total_units_purchased": 0,
                "total_spend": Decimal("0"),
                "first_purchase_at": "",
                "last_purchase_at": "",
            },
        )
        customer["unique_products"] += 1
        customer["purchase_line_count"] += interaction.purchase_line_count
        customer["total_units_purchased"] += interaction.quantity_purchased
        customer["total_spend"] += interaction.spend
        if (
            not customer["first_purchase_at"]
            or interaction.first_purchase_at < customer["first_purchase_at"]
        ):
            customer["first_purchase_at"] = interaction.first_purchase_at
        if (
            not customer["last_purchase_at"]
            or interaction.last_purchase_at > customer["last_purchase_at"]
        ):
            customer["last_purchase_at"] = interaction.last_purchase_at

        product = products.setdefault(
            stock_code,
            {
                "unique_customers": 0,
                "purchase_line_count": 0,
                "units_sold": 0,
                "gross_sales": Decimal("0"),
                "first_sold_at": "",
                "last_sold_at": "",
            },
        )
        product["unique_customers"] += 1
        product["purchase_line_count"] += interaction.purchase_line_count
        product["units_sold"] += interaction.quantity_purchased
        product["gross_sales"] += interaction.spend
        if not product["first_sold_at"] or interaction.first_purchase_at < product["first_sold_at"]:
            product["first_sold_at"] = interaction.first_purchase_at
        if not product["last_sold_at"] or interaction.last_purchase_at > product["last_sold_at"]:
            product["last_sold_at"] = interaction.last_purchase_at

    return interactions, customers, products, dict(metrics)


def _interaction_rows(interactions: dict):
    for (customer_id, stock_code), interaction in sorted(interactions.items()):
        yield {
            "customer_id": customer_id,
            "stock_code": stock_code,
            "purchase_line_count": str(interaction.purchase_line_count),
            "quantity_purchased": str(interaction.quantity_purchased),
            "spend": _decimal_text(interaction.spend),
            "first_purchase_at": interaction.first_purchase_at,
            "last_purchase_at": interaction.last_purchase_at,
        }


def _customer_rows(customers: dict):
    for customer_id in sorted(customers):
        customer = customers[customer_id]
        yield {
            "customer_id": customer_id,
            "unique_products": str(customer["unique_products"]),
            "purchase_line_count": str(customer["purchase_line_count"]),
            "total_units_purchased": str(customer["total_units_purchased"]),
            "total_spend": _decimal_text(customer["total_spend"]),
            "first_purchase_at": customer["first_purchase_at"],
            "last_purchase_at": customer["last_purchase_at"],
        }


def _product_rows(products: dict, catalog: dict):
    stock_codes = set(products) | set(catalog)
    ranked_codes = sorted(
        products,
        key=lambda code: (
            -products[code]["unique_customers"],
            -products[code]["purchase_line_count"],
            code,
        ),
    )
    popularity_ranks = {
        stock_code: str(rank)
        for rank, stock_code in enumerate(ranked_codes, start=1)
    }

    for stock_code in sorted(stock_codes):
        sales = products.get(stock_code, {})
        catalog_entry = catalog.get(
            stock_code,
            {"description": "", "catalog_record_count": 0},
        )
        yield {
            "stock_code": stock_code,
            "description": catalog_entry["description"],
            "catalog_record_count": str(catalog_entry["catalog_record_count"]),
            "is_duplicate_catalog_code": str(
                catalog_entry["catalog_record_count"] > 1
            ).lower(),
            "purchase_line_count": str(sales.get("purchase_line_count", 0)),
            "unique_customers": str(sales.get("unique_customers", 0)),
            "units_sold": str(sales.get("units_sold", 0)),
            "gross_sales": _decimal_text(sales.get("gross_sales", Decimal("0"))),
            "first_sold_at": sales.get("first_sold_at", ""),
            "last_sold_at": sales.get("last_sold_at", ""),
            "popularity_rank": popularity_ranks.get(stock_code, ""),
        }


def build_gold(
    silver_root: Path,
    gold_root: Path,
) -> Path:
    transaction_manifest, transaction_manifest_sha256 = read_silver_manifest(
        silver_root,
        "online_retail",
    )
    catalog_manifest, catalog_manifest_sha256 = read_silver_manifest(
        silver_root,
        "product_catalog",
    )
    catalog, catalog_metrics = _read_catalog(silver_root, catalog_manifest)
    interactions, customers, products, transaction_metrics = _build_aggregates(
        silver_root,
        transaction_manifest,
    )

    gold_root.mkdir(parents=True, exist_ok=True)
    output_files = [
        write_gzip_csv_atomic(
            gold_root / "customer_product_interactions.csv.gz",
            INTERACTION_COLUMNS,
            _interaction_rows(interactions),
        ),
        write_gzip_csv_atomic(
            gold_root / "customer_features.csv.gz",
            CUSTOMER_COLUMNS,
            _customer_rows(customers),
        ),
        write_gzip_csv_atomic(
            gold_root / "product_features.csv.gz",
            PRODUCT_FEATURE_COLUMNS,
            _product_rows(products, catalog),
        ),
    ]

    generated_at = datetime.now(timezone.utc).isoformat()
    quality_report = {
        "generated_at_utc": generated_at,
        "transaction_silver_manifest_sha256": transaction_manifest_sha256,
        "catalog_silver_manifest_sha256": catalog_manifest_sha256,
        "transactions": transaction_metrics,
        "catalog": catalog_metrics,
        "customer_product_interactions": len(interactions),
        "customers": len(customers),
        "products_with_sales": len(products),
        "products_without_sales": sum(
            1 for stock_code in catalog if stock_code not in products
        ),
        "sold_products_missing_catalog_entry": sum(
            1 for stock_code in products if stock_code not in catalog
        ),
    }
    write_json_atomic(gold_root / "quality_report.json", quality_report)
    gold_manifest = {
        "schema_version": "1",
        "created_at_utc": generated_at,
        "inputs": {
            "online_retail": {
                "silver_manifest_sha256": transaction_manifest_sha256,
                "input_rows": transaction_metrics["input_rows"],
            },
            "product_catalog": {
                "silver_manifest_sha256": catalog_manifest_sha256,
                "input_rows": catalog_metrics["catalog_rows"],
            },
        },
        "files": output_files,
    }
    write_json_atomic(gold_root / "gold_manifest.json", gold_manifest)
    logging.info(
        "Gold complete: input=%d, training=%d, interactions=%d, customers=%d, products=%d",
        transaction_metrics["input_rows"],
        transaction_metrics["training_rows"],
        len(interactions),
        len(customers),
        len(set(products) | set(catalog)),
    )
    return gold_root

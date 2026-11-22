import argparse
import calendar
import csv
import gzip
import html
import json
import logging
import math
import os
import tempfile
from collections import Counter
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Iterator

import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt

from gold.common import TRANSACTION_COLUMNS, iter_silver_rows, read_silver_manifest, sha256_file
from gold.features import (
    CUSTOMER_COLUMNS,
    INTERACTION_COLUMNS,
    PRODUCT_FEATURE_COLUMNS,
)


_PROJECT_ROOT = Path(__file__).resolve().parents[1]
_DEFAULT_SILVER_ROOT = _PROJECT_ROOT / "data" / "silver"
_DEFAULT_GOLD_ROOT = _PROJECT_ROOT / "data" / "gold"
_DEFAULT_OUTPUT_DIR = _PROJECT_ROOT / "data" / "eda"
_TOP_ITEMS = 20
_TOP_CUSTOMERS = 20
_CUSTOMER_HISTOGRAM_BINS = 32


@dataclass
class SilverActivity:
    silver_rows: int = 0
    eligible_rows: int = 0
    duplicate_rows_excluded: int = 0
    purchase_lines: int = 0
    first_purchase_date: str = ""
    last_purchase_date: str = ""
    monthly_lines: Counter[str] = field(default_factory=Counter)


@dataclass
class GoldActivity:
    customers: dict[str, int] = field(default_factory=dict)
    products: dict[str, dict[str, str | int]] = field(default_factory=dict)
    interaction_pairs: int = 0
    purchase_lines: int = 0
    top_customer_counts: list[dict[str, str | int]] = field(default_factory=list)
    top_products: list[dict[str, str | int]] = field(default_factory=list)
    heatmap_presence: set[tuple[str, str]] = field(default_factory=set)
    gold_manifest_sha256: str = ""
    silver_manifest_sha256: str = ""
    silver_product_catalog_manifest_sha256: str = ""


def analyze_silver(silver_root: Path) -> tuple[SilverActivity, str]:
    manifest, manifest_sha256 = read_silver_manifest(silver_root, "online_retail")
    activity = SilverActivity()

    for row in iter_silver_rows(
        silver_root,
        "online_retail",
        manifest,
        TRANSACTION_COLUMNS,
    ):
        activity.silver_rows += 1
        if (row.get("is_recommendation_eligible") or "").strip().lower() != "true":
            continue
        activity.eligible_rows += 1
        if (row.get("is_duplicate_content") or "").strip().lower() == "true":
            activity.duplicate_rows_excluded += 1
            continue

        try:
            purchased_at = datetime.fromisoformat(
                (row.get("invoice_datetime") or "").strip()
            )
            quantity = int(row["quantity"])
            line_total = Decimal(row["line_total"])
        except (KeyError, ValueError, InvalidOperation) as error:
            raise ValueError(
                f"Invalid recommendation-eligible Silver row: {error}"
            ) from error
        if not (row.get("customer_id") or "").strip():
            raise ValueError(
                "Recommendation-eligible Silver rows require a customer ID"
            )
        if not (row.get("stock_code") or "").strip():
            raise ValueError(
                "Recommendation-eligible Silver rows require a stock code"
            )
        if quantity <= 0 or not line_total.is_finite() or line_total < 0:
            raise ValueError(
                "Recommendation-eligible Silver rows must have positive quantity "
                "and nonnegative line total"
            )

        activity.purchase_lines += 1
        activity.monthly_lines[purchased_at.strftime("%Y-%m")] += 1
        purchase_date = purchased_at.date().isoformat()
        if (
            not activity.first_purchase_date
            or purchase_date < activity.first_purchase_date
        ):
            activity.first_purchase_date = purchase_date
        if purchase_date > activity.last_purchase_date:
            activity.last_purchase_date = purchase_date

    if not activity.purchase_lines:
        raise ValueError("No recommendation-eligible interactions found in Silver data")
    return activity, manifest_sha256


def _read_gold_manifest(gold_root: Path) -> tuple[dict, dict[str, dict], str]:
    manifest_path = gold_root / "gold_manifest.json"
    if not manifest_path.is_file():
        raise FileNotFoundError(f"Gold manifest not found: {manifest_path}")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if not isinstance(manifest, dict) or manifest.get("schema_version") != "1":
        raise ValueError(f"Invalid or unsupported Gold manifest: {manifest_path}")
    inputs = manifest.get("inputs")
    files = manifest.get("files")
    if not isinstance(inputs, dict) or not isinstance(files, list) or not files:
        raise ValueError(f"Incomplete Gold manifest: {manifest_path}")

    entries = {}
    for entry in files:
        if not isinstance(entry, dict):
            raise ValueError(f"Invalid Gold file entry in {manifest_path}")
        filename = entry.get("file")
        digest = entry.get("sha256")
        row_count = entry.get("row_count")
        if (
            not isinstance(filename, str)
            or Path(filename).name != filename
            or not filename.endswith(".csv.gz")
            or not isinstance(digest, str)
            or len(digest) != 64
            or isinstance(row_count, bool)
            or not isinstance(row_count, int)
            or row_count < 0
            or filename in entries
        ):
            raise ValueError(f"Invalid or duplicate Gold file metadata: {entry!r}")
        entries[filename] = entry
    return manifest, entries, sha256_file(manifest_path)


def _iter_verified_gold_rows(
    gold_root: Path,
    file_entries: dict[str, dict],
    filename: str,
    expected_columns: tuple[str, ...],
) -> Iterator[dict[str, str]]:
    entry = file_entries.get(filename)
    if not isinstance(entry, dict):
        raise ValueError(f"Gold manifest does not list {filename}")
    source_path = gold_root / filename
    if not source_path.is_file():
        raise FileNotFoundError(f"Gold feature file not found: {source_path}")
    actual_sha256 = sha256_file(source_path)
    if actual_sha256 != entry["sha256"]:
        raise ValueError(
            f"Gold checksum mismatch for {source_path}: "
            f"expected {entry['sha256']}, got {actual_sha256}"
        )

    row_count = 0
    with gzip.open(source_path, "rt", encoding="utf-8-sig", newline="") as source:
        reader = csv.DictReader(source)
        if reader.fieldnames != list(expected_columns):
            raise ValueError(
                f"Gold schema mismatch in {filename}: expected "
                f"{list(expected_columns)}, received {reader.fieldnames}"
            )
        for row in reader:
            if None in row or any(value is None for value in row.values()):
                raise ValueError(f"Malformed Gold CSV row in {filename}")
            row_count += 1
            yield row
    if row_count != entry["row_count"]:
        raise ValueError(
            f"Gold row-count mismatch for {filename}: manifest states "
            f"{entry['row_count']}, file contains {row_count}"
        )


def analyze_gold(gold_root: Path) -> GoldActivity:
    manifest, file_entries, manifest_sha256 = _read_gold_manifest(gold_root)
    retail_input = manifest["inputs"].get("online_retail")
    catalog_input = manifest["inputs"].get("product_catalog")
    if not isinstance(retail_input, dict) or not isinstance(
        retail_input.get("silver_manifest_sha256"), str
    ):
        raise ValueError("Gold manifest is missing Online Retail Silver lineage")
    if not isinstance(catalog_input, dict) or not isinstance(
        catalog_input.get("silver_manifest_sha256"), str
    ):
        raise ValueError("Gold manifest is missing product-catalog Silver lineage")
    activity = GoldActivity(
        gold_manifest_sha256=manifest_sha256,
        silver_manifest_sha256=retail_input["silver_manifest_sha256"],
        silver_product_catalog_manifest_sha256=catalog_input[
            "silver_manifest_sha256"
        ],
    )
    customer_purchase_lines = Counter()
    product_purchase_lines = Counter()
    for row in _iter_verified_gold_rows(
        gold_root,
        file_entries,
        "customer_features.csv.gz",
        CUSTOMER_COLUMNS,
    ):
        customer_id = row["customer_id"].strip()
        if not customer_id or customer_id in activity.customers:
            raise ValueError("Gold customer feature table contains an invalid key")
        try:
            unique_products = int(row["unique_products"])
            purchase_lines = int(row["purchase_line_count"])
        except ValueError as error:
            raise ValueError("Gold customer features contain invalid counts") from error
        if unique_products <= 0 or purchase_lines <= 0:
            raise ValueError("Gold customer features must describe active customers")
        activity.customers[customer_id] = unique_products
        customer_purchase_lines[customer_id] = purchase_lines

    for row in _iter_verified_gold_rows(
        gold_root,
        file_entries,
        "product_features.csv.gz",
        PRODUCT_FEATURE_COLUMNS,
    ):
        stock_code = row["stock_code"].strip().upper()
        if not stock_code or stock_code in activity.products:
            raise ValueError("Gold product feature table contains an invalid key")
        try:
            unique_customers = int(row["unique_customers"])
            purchase_lines = int(row["purchase_line_count"])
        except ValueError as error:
            raise ValueError("Gold product features contain invalid counts") from error
        if unique_customers < 0 or purchase_lines < 0:
            raise ValueError("Gold product features cannot contain negative counts")
        activity.products[stock_code] = {
            "description": " ".join(row["description"].split()),
            "unique_customers": unique_customers,
            "purchase_lines": purchase_lines,
        }
        product_purchase_lines[stock_code] = purchase_lines

    if not activity.customers:
        raise ValueError("Gold customer feature table is empty")
    active_products = {
        stock_code: metrics
        for stock_code, metrics in activity.products.items()
        if int(metrics["unique_customers"]) > 0
    }
    if not active_products:
        raise ValueError("Gold has no products with sales")

    activity.top_customer_counts = [
        {"customer_id": customer_id, "unique_products": unique_products}
        for customer_id, unique_products in sorted(
            activity.customers.items(),
            key=lambda item: (-item[1], item[0]),
        )[:_TOP_CUSTOMERS]
    ]
    activity.top_products = [
        {
            "stock_code": stock_code,
            "description": str(metrics["description"]),
            "unique_customers": int(metrics["unique_customers"]),
            "purchase_lines": int(metrics["purchase_lines"]),
        }
        for stock_code, metrics in sorted(
            active_products.items(),
            key=lambda item: (
                -int(item[1]["unique_customers"]),
                -int(item[1]["purchase_lines"]),
                item[0],
            ),
        )[:_TOP_ITEMS]
    ]
    top_customer_ids = {str(item["customer_id"]) for item in activity.top_customer_counts}
    top_product_codes = {str(item["stock_code"]) for item in activity.top_products}
    customer_pair_counts = Counter()
    activity_pairs_by_product = Counter()

    for row in _iter_verified_gold_rows(
        gold_root,
        file_entries,
        "customer_product_interactions.csv.gz",
        INTERACTION_COLUMNS,
    ):
        customer_id = row["customer_id"].strip()
        stock_code = row["stock_code"].strip().upper()
        if customer_id not in activity.customers or stock_code not in activity.products:
            raise ValueError("Gold interaction references an unknown customer or product")
        try:
            purchase_lines = int(row["purchase_line_count"])
            quantity = int(row["quantity_purchased"])
            Decimal(row["spend"])
        except (ValueError, InvalidOperation) as error:
            raise ValueError("Gold interactions contain invalid feature values") from error
        if purchase_lines <= 0 or quantity <= 0:
            raise ValueError("Gold interactions must represent positive purchases")
        activity.interaction_pairs += 1
        activity.purchase_lines += purchase_lines
        customer_pair_counts[customer_id] += 1
        activity_pairs_by_product[stock_code] += 1
        if customer_id in top_customer_ids and stock_code in top_product_codes:
            activity.heatmap_presence.add((customer_id, stock_code))

    if activity.interaction_pairs != sum(activity.customers.values()):
        raise ValueError("Gold customer features do not reconcile with interaction pairs")
    if activity.interaction_pairs != sum(
        int(metrics["unique_customers"])
        for metrics in activity.products.values()
    ):
        raise ValueError("Gold product features do not reconcile with interaction pairs")
    if activity.purchase_lines != sum(customer_purchase_lines.values()):
        raise ValueError("Gold customer purchase-line totals do not reconcile")
    if activity.purchase_lines != sum(product_purchase_lines.values()):
        raise ValueError("Gold product purchase-line totals do not reconcile")
    if any(
        customer_pair_counts[customer_id] != unique_products
        for customer_id, unique_products in activity.customers.items()
    ):
        raise ValueError("Gold customer unique-product counts do not reconcile")
    if any(
        activity_pairs_by_product[stock_code] != int(metrics["unique_customers"])
        for stock_code, metrics in activity.products.items()
    ):
        raise ValueError("Gold product unique-customer counts do not reconcile")
    return activity


def _customer_diversity_histogram(values: list[int]) -> list[dict[str, int | float]]:
    if not values:
        raise ValueError("Gold customer feature table contains no diversity values")
    minimum = min(values)
    maximum = max(values)
    if minimum == maximum:
        return [{"lower": minimum, "upper": maximum, "count": len(values)}]

    lower_log = math.log10(minimum)
    upper_log = math.log10(maximum)
    edges = [
        10 ** (lower_log + (upper_log - lower_log) * index / _CUSTOMER_HISTOGRAM_BINS)
        for index in range(_CUSTOMER_HISTOGRAM_BINS + 1)
    ]
    counts = [0] * _CUSTOMER_HISTOGRAM_BINS
    for value in values:
        index = min(
            _CUSTOMER_HISTOGRAM_BINS - 1,
            int(
                (math.log10(value) - lower_log)
                / (upper_log - lower_log)
                * _CUSTOMER_HISTOGRAM_BINS
            ),
        )
        counts[index] += 1
    return [
        {
            "lower": round(edges[index], 3),
            "upper": round(edges[index + 1], 3),
            "count": counts[index],
        }
        for index in range(_CUSTOMER_HISTOGRAM_BINS)
    ]


def _interaction_matrix_summary(activity: GoldActivity) -> tuple[dict, dict]:
    customer_count = len(activity.customers)
    active_products = sum(
        int(metrics["unique_customers"]) > 0
        for metrics in activity.products.values()
    )
    possible_pairs = customer_count * active_products
    if possible_pairs == 0:
        raise ValueError("Cannot calculate interaction density for an empty matrix")
    density_percent = activity.interaction_pairs / possible_pairs * 100
    sparsity_percent = 100 - density_percent

    top_customers = activity.top_customer_counts
    top_products = activity.top_products
    purchased = [
        [
            int(
                (
                    str(customer["customer_id"]),
                    str(product["stock_code"]),
                )
                in activity.heatmap_presence
            )
            for product in top_products
        ]
        for customer in top_customers
    ]
    subset_pairs = sum(sum(row) for row in purchased)
    subset_possible_pairs = len(top_customers) * len(top_products)
    matrix = {
        "active_customers": customer_count,
        "active_products": active_products,
        "observed_customer_product_pairs": activity.interaction_pairs,
        "possible_active_customer_product_pairs": possible_pairs,
        "density_percent": round(density_percent, 6),
        "sparsity_percent": round(sparsity_percent, 6),
        "top_customer_product_subset_density_percent": round(
            subset_pairs / subset_possible_pairs * 100,
            6,
        ),
    }
    heatmap = {
        "customers": top_customers,
        "products": top_products,
        "purchased": purchased,
    }
    return matrix, heatmap


def _last_month_is_partial(last_purchase_date: str) -> bool:
    last_date = date.fromisoformat(last_purchase_date)
    return last_date.day < calendar.monthrange(last_date.year, last_date.month)[1]


def _write_interaction_plot(
    monthly_lines: list[dict],
    diversity_histogram: list[dict],
    output_path: Path,
) -> None:
    months = [item["month"] + ("*" if item["partial"] else "") for item in monthly_lines]
    month_counts = [item["count"] for item in monthly_lines]
    positions = list(range(len(months)))
    bin_counts = [int(item["count"]) for item in diversity_histogram]
    bin_labels = [
        f"{item['lower']:g}–{item['upper']:g}" for item in diversity_histogram
    ]

    figure, axes = plt.subplots(1, 2, figsize=(14, 5))
    axes[0].bar(positions, month_counts, color="#4472C4")
    axes[0].set_title("Eligible purchase lines by month (Silver)")
    axes[0].set_xlabel("Invoice month")
    axes[0].set_ylabel("Purchase lines")
    axes[0].set_xticks(positions, months, rotation=45, ha="right")

    bin_positions = list(range(len(bin_counts)))
    axes[1].bar(bin_positions, bin_counts, color="#70AD47", edgecolor="white")
    tick_step = max(1, len(bin_labels) // 6)
    tick_positions = bin_positions[::tick_step]
    axes[1].set_xticks(
        tick_positions,
        [bin_labels[index] for index in tick_positions],
        rotation=35,
        ha="right",
    )
    axes[1].set_title("Distinct products per customer (Gold, log-width bins)")
    axes[1].set_xlabel("Distinct products purchased")
    axes[1].set_ylabel("Customers")

    figure.tight_layout()
    figure.savefig(output_path, dpi=160, bbox_inches="tight")
    plt.close(figure)


def _write_item_popularity_plot(
    top_products: list[dict],
    output_path: Path,
) -> None:
    labels = [
        f"{item['stock_code']} - {item['description'] or 'Description unavailable'}"
        for item in reversed(top_products)
    ]
    values = [int(item["unique_customers"]) for item in reversed(top_products)]

    figure, axis = plt.subplots(figsize=(13, 8))
    bars = axis.barh(labels, values, color="#ED7D31")
    axis.bar_label(bars, padding=3)
    axis.set_title(f"Top {_TOP_ITEMS} Gold products by unique customer count")
    axis.set_xlabel("Unique customers")
    axis.set_ylabel("Stock code and catalog description")
    axis.margins(x=0.12)
    figure.tight_layout()
    figure.savefig(output_path, dpi=160, bbox_inches="tight")
    plt.close(figure)


def _write_sparsity_plot(
    matrix: dict,
    heatmap: dict,
    output_path: Path,
) -> None:
    density_percent = matrix["density_percent"]
    sparsity_percent = matrix["sparsity_percent"]
    purchased = heatmap["purchased"]
    figure, axes = plt.subplots(
        1,
        2,
        figsize=(14, 6),
        gridspec_kw={"width_ratios": [1, 1.5]},
    )
    axes[0].barh([0], [density_percent], color="#4472C4")
    axes[0].barh(
        [0],
        [sparsity_percent],
        left=[density_percent],
        color="#D9E2F3",
    )
    axes[0].set_xlim(0, 100)
    axes[0].set_yticks([])
    axes[0].set_xlabel("Share of possible user-item pairs (%)")
    axes[0].set_title(
        f"Gold active matrix: {sparsity_percent:.2f}% sparse\n"
        f"{matrix['observed_customer_product_pairs']:,} of "
        f"{matrix['possible_active_customer_product_pairs']:,} pairs observed"
    )
    axes[0].annotate(
        f"Observed {density_percent:.2f}%",
        xy=(density_percent / 2, 0),
        xytext=(14, 0.35),
        arrowprops={"arrowstyle": "->", "color": "#404040"},
        ha="left",
        va="center",
    )

    image = axes[1].imshow(
        purchased,
        aspect="auto",
        interpolation="nearest",
        cmap="Blues",
    )
    axes[1].set_title(
        f"Gold: top {len(heatmap['customers'])} customers × "
        f"top {len(heatmap['products'])} products\n"
        f"Subset density: {matrix['top_customer_product_subset_density_percent']:.1f}%"
    )
    axes[1].set_xlabel("Products, ranked by unique customers")
    axes[1].set_ylabel("Customers, ranked by distinct products")
    axes[1].set_xticks(
        range(len(heatmap["products"])),
        range(1, len(heatmap["products"]) + 1),
    )
    axes[1].set_yticks(
        range(len(heatmap["customers"])),
        range(1, len(heatmap["customers"]) + 1),
    )
    figure.colorbar(image, ax=axes[1], ticks=[0, 1], label="Purchased (0/1)")
    figure.tight_layout()
    figure.savefig(output_path, dpi=160, bbox_inches="tight")
    plt.close(figure)


def _write_text_atomic(output_path: Path, text: str) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            dir=output_path.parent,
            prefix=f".{output_path.stem}-",
            suffix=".tmp",
            delete=False,
            encoding="utf-8",
            newline="\n",
        ) as output:
            temporary_path = Path(output.name)
            output.write(text)
            output.flush()
            os.fsync(output.fileno())
        os.replace(temporary_path, output_path)
        temporary_path = None
    finally:
        if temporary_path is not None:
            temporary_path.unlink(missing_ok=True)


def _write_summary(output_path: Path, summary: dict) -> None:
    _write_text_atomic(output_path, json.dumps(summary, indent=2) + "\n")


def _write_dashboard(output_dir: Path, summary: dict) -> None:
    counts = summary["counts"]
    matrix = summary["interaction_matrix"]
    months = summary["monthly_purchase_lines"]
    top_products = summary["top_products_by_unique_customers"]
    busiest_month = max(months, key=lambda item: item["count"])
    top_product = top_products[0]
    generated_at = summary["generated_at_utc"]
    cache_version = (
        generated_at.replace("-", "")
        .replace(":", "")
        .replace(".", "")
        .replace("+", "")
    )
    top_item_rows = "\n".join(
        "<tr>"
        f"<td>{html.escape(str(item['stock_code']))}</td>"
        f"<td>{html.escape(str(item['description']) or 'Description unavailable')}</td>"
        f"<td>{int(item['unique_customers']):,}</td>"
        f"<td>{int(item['purchase_lines']):,}</td>"
        "</tr>"
        for item in top_products[:10]
    )
    chart_data = {
        "monthlyPurchaseLines": months,
        "customerDiversityHistogram": summary["customer_product_diversity_histogram"],
        "topProducts": top_products,
        "interactionMatrix": matrix,
        "heatmap": summary["top_customer_product_heatmap"],
    }
    chart_data_json = (
        json.dumps(chart_data, ensure_ascii=True, separators=(",", ":"))
        .replace("<", "\\u003c")
        .replace(">", "\\u003e")
        .replace("&", "\\u0026")
    )
    period = summary["period"]
    period_description = (
        f"Eligible invoice dates: {period['first_eligible_invoice_date']} to "
        f"{period['last_eligible_invoice_date']}."
    )
    if period["final_month_is_partial"]:
        period_description += " The final month is partial and marked * in the chart."
    top_product_name = html.escape(
        f"{top_product['stock_code']} - "
        f"{top_product['description'] or 'Description unavailable'}"
    )
    monthly_caption = (
        f"* marks the partial final month, through "
        f"{period['last_eligible_invoice_date']}."
        if period["final_month_is_partial"]
        else f"Last eligible invoice date: {period['last_eligible_invoice_date']}."
    )
    date_range = (
        f"{period['first_eligible_invoice_date']} to "
        f"{period['last_eligible_invoice_date']}"
    )
    page = """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Online Retail Recommendation EDA</title>
  <style>
    :root { color-scheme: light; font-family: "Segoe UI", Arial, sans-serif; color: #172033; background: #f3f6fb; }
    body { margin: 0; }
    main { max-width: 1220px; margin: 0 auto; padding: 28px 20px 48px; }
    header { display: flex; justify-content: space-between; align-items: flex-start; gap: 20px; margin-bottom: 22px; }
    h1 { margin: 0 0 8px; font-size: clamp(1.7rem, 3vw, 2.4rem); }
    h2 { margin: 0 0 14px; font-size: 1.2rem; }
    p { line-height: 1.55; }
    .muted { color: #607087; }
    .refresh { border: 0; border-radius: 8px; background: #2456a6; color: white; padding: 11px 15px; font-weight: 600; cursor: pointer; }
    .cards { display: grid; grid-template-columns: repeat(auto-fit, minmax(190px, 1fr)); gap: 14px; margin: 22px 0; }
    .card, .panel { background: white; border: 1px solid #dce4ef; border-radius: 12px; box-shadow: 0 2px 8px #1e35500a; }
    .card { padding: 16px; }
    .card-label { color: #607087; font-size: .9rem; }
    .card-value { display: block; margin-top: 6px; font-size: 1.65rem; font-weight: 700; }
    .panel { padding: 20px; margin: 16px 0; }
    .panel h3 { margin: 20px 0 6px; font-size: 1.05rem; }
    .panel h3:first-of-type { margin-top: 8px; }
    .guide-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(230px, 1fr)); gap: 12px; }
    .guide-card { padding: 14px; background: #f5f8fd; border: 1px solid #e1e9f4; border-radius: 8px; }
    .guide-card p { margin: 6px 0 0; }
    .callout { margin-top: 14px; padding: 14px; background: #fff8e8; border-left: 4px solid #e79b37; border-radius: 5px; }
    .metric-note { margin: 8px 0 0; color: #607087; font-size: .88rem; line-height: 1.45; }
    .insights { display: grid; grid-template-columns: repeat(auto-fit, minmax(250px, 1fr)); gap: 12px; }
    .insight { padding: 14px; border-left: 4px solid #4472c4; background: #f5f8fd; border-radius: 5px; }
    .chart-container { width: 100%; overflow-x: auto; }
    .interactive-chart { display: block; width: 100%; height: auto; min-width: 760px; }
    .interactive-chart text { fill: #34445a; font-family: "Segoe UI", Arial, sans-serif; }
    .chart-grid { stroke: #e2e8f0; stroke-width: 1; }
    .chart-bar { transition: opacity .12s ease; }
    .chart-bar:hover, .heat-cell:hover { opacity: .68; stroke: #152b4d; stroke-width: 2; }
    figure { margin: 0; }
    figure img { display: block; width: 100%; height: auto; }
    figcaption { margin-top: 10px; color: #607087; line-height: 1.45; }
    .table-wrap { overflow-x: auto; }
    table { border-collapse: collapse; width: 100%; }
    th, td { text-align: left; padding: 10px 12px; border-bottom: 1px solid #e4eaf2; }
    th { background: #f5f8fd; }
    details { margin-top: 18px; }
    summary { cursor: pointer; color: #2456a6; font-weight: 600; }
    .png-export { margin-top: 18px; }
    footer { margin-top: 20px; overflow-wrap: anywhere; font-size: .85rem; }
    @media (max-width: 640px) { header { flex-direction: column; } .refresh { width: 100%; } .panel { padding: 14px; } }
  </style>
</head>
<body>
<main>
  <header>
    <div>
      <h1>Online Retail Recommendation EDA</h1>
      <div class="muted">A beginner-friendly guide to the shopping patterns behind the recommender.</div>
      <div class="muted">Generated (UTC): __GENERATED_AT__</div>
      <div class="muted">__PERIOD_DESCRIPTION__</div>
      <p class="muted">EDA means Exploratory Data Analysis: looking at data to understand it before relying on a model. Refresh after a Prefect run to see its latest results.</p>
    </div>
    <button class="refresh" type="button" onclick="window.location.reload()">Refresh dashboard</button>
  </header>

  <section class="panel">
    <h2>Start here: what are we looking at?</h2>
    <p>This project studies past purchases from the UCI Online Retail dataset to understand what customers buy and prepare data for product recommendations. The source is historical purchase data, not live website activity, clicks, or star ratings.</p>
    <div class="guide-grid">
      <div class="guide-card"><strong>One row is one product line</strong><p>An invoice can contain several products, so one order may create several rows. A purchase line is not the same as a complete order or a single customer.</p></div>
      <div class="guide-card"><strong>A purchase is our positive signal</strong><p>Because there are no ratings, a recorded customer-product purchase is treated as evidence of interest. No recorded purchase does not mean the customer disliked the product.</p></div>
      <div class="guide-card"><strong>Silver then Gold</strong><p>Silver is the checked and prepared transaction data. Gold combines it into easier-to-analyze customer, product, and customer-product summaries.</p></div>
    </div>
    <div class="callout"><strong>What is included?</strong> The charts count customer-linked, positive purchase lines and exclude exact duplicate records. Cancellations, returns, and rows without a customer ID are not counted as positive recommendations. The analyzed period is __DATE_RANGE__.</div>
  </section>

  <section class="cards" aria-label="Key metrics">
    <div class="card"><span class="card-label">Usable product purchase lines</span><strong class="card-value">__PURCHASE_LINES__</strong><p class="metric-note">Individual product rows included in this analysis.</p></div>
    <div class="card"><span class="card-label">Customers with purchases</span><strong class="card-value">__CUSTOMERS__</strong><p class="metric-note">Customers with at least one included purchase.</p></div>
    <div class="card"><span class="card-label">Products with purchases</span><strong class="card-value">__PRODUCTS__</strong><p class="metric-note">Different products bought by at least one included customer.</p></div>
    <div class="card"><span class="card-label">Possible customer-product pairs with no purchase recorded</span><strong class="card-value">__SPARSITY__%</strong><p class="metric-note">This is missing history, not evidence of dislike.</p></div>
  </section>

  <section class="panel">
    <h2>Quick takeaways</h2>
    <div class="insights">
      <div class="insight"><strong>Most purchase lines:</strong> __BUSIEST_MONTH__ had __BUSIEST_COUNT__ included product rows. This counts lines, not customers or money spent.</div>
      <div class="insight"><strong>Most widely purchased product:</strong> __TOP_PRODUCT__ was bought by __TOP_PRODUCT_CUSTOMERS__ different customers.</div>
      <div class="insight"><strong>Why recommendations are useful:</strong> purchase histories cover only __DENSITY__% of all possible active customer-product pairs, so a recommender can help surface products a customer has not yet bought.</div>
    </div>
  </section>

  <section class="panel">
    <h2>1. When do purchases happen, and how varied are customers?</h2>
    <h3>Monthly purchase activity</h3>
    <p>Each bar counts included product lines by invoice month. A higher bar means more product rows were bought that month; it does not necessarily mean more unique customers or higher revenue. Hover over a bar for its exact count.</p>
    <figure>
      <div class="chart-container"><div id="monthly-chart"></div></div>
      <figcaption>__MONTHLY_CAPTION__ An orange bar and * mark the final, incomplete month, so compare it cautiously with full months.</figcaption>
    </figure>
    <h3>How many different products customers purchased</h3>
    <p>This chart groups customers by the number of distinct products they bought at least once. The vertical axis is the number of customers in each group. The horizontal groups are logarithmically spaced, which means ranges become wider at larger values; hover over a bar to see its exact range and customer count.</p>
    <figure><div class="chart-container"><div id="diversity-chart"></div></div></figure>
  </section>

  <section class="panel">
    <h2>2. Which products reach the most customers?</h2>
    <p>Each bar shows how many different customers bought that product at least once. Longer bars mean wider customer reach. This is not a ranking by units sold, revenue, or number of orders. Hover over a bar or label for the full product details.</p>
    <figure>
      <div class="chart-container"><div id="popularity-chart"></div></div>
      <figcaption>The table below gives the same top products in a readable format. “Purchase lines” counts product rows across invoices; “unique customers” counts each customer once per product.</figcaption>
    </figure>
  </section>

  <section class="panel">
    <h2>3. How much purchase history is available?</h2>
    <p>A customer-product pair means one particular customer and one particular product. The full matrix considers every active customer with every active product, then checks whether a purchase was recorded for each pair.</p>
    <div class="guide-grid">
      <div class="guide-card"><strong>Blue bar: recorded purchases</strong><p>__OBSERVED_PAIRS__ customer-product pairs have at least one purchase in the analyzed history.</p></div>
      <div class="guide-card"><strong>Pale bar: no purchase recorded</strong><p>__UNOBSERVED_PAIRS__ of the __POSSIBLE_PAIRS__ possible pairs have no recorded purchase. This is unknown preference, not negative feedback.</p></div>
    </div>
    <figure>
      <div class="chart-container"><div id="sparsity-chart"></div></div>
      <figcaption><strong>How to read the heatmap:</strong> each row is one of the 20 customers with the most distinct products purchased; each column is one of the 20 products bought by the most customers. A blue square means that customer bought that product; a pale square means no purchase was observed. This deliberately selected top-20 view is not representative of the full dataset, and its percentage is not the overall sparsity.</figcaption>
    </figure>
  </section>

  <section class="panel">
    <h2>Top 10 products, in numbers</h2>
    <div class="table-wrap">
      <table>
        <thead><tr><th>Stock code</th><th>Description</th><th>Unique customers</th><th>Purchase lines</th></tr></thead>
        <tbody>__TOP_ITEM_ROWS__</tbody>
      </table>
    </div>
    <details>
      <summary>Show static chart images (PNG downloads)</summary>
      <figure class="png-export"><img src="plots/interaction_patterns.png?v=__CACHE_VERSION__" alt="PNG export of monthly Silver purchases and Gold customer diversity"><figcaption>Interaction patterns. <a href="plots/interaction_patterns.png" download>Download PNG</a></figcaption></figure>
      <figure class="png-export"><img src="plots/item_popularity.png?v=__CACHE_VERSION__" alt="PNG export of Gold product popularity"><figcaption>Item popularity. <a href="plots/item_popularity.png" download>Download PNG</a></figcaption></figure>
      <figure class="png-export"><img src="plots/interaction_sparsity.png?v=__CACHE_VERSION__" alt="PNG export of Gold interaction sparsity"><figcaption>Interaction sparsity. <a href="plots/interaction_sparsity.png" download>Download PNG</a></figcaption></figure>
    </details>
  </section>

  <details class="panel">
    <summary>Technical details: source validation and lineage</summary>
    <p>Before these charts are generated, the pipeline checks the Silver and Gold manifests, file checksums, schemas, row counts, and that Gold was built from the same Silver data.</p>
    <p class="muted">
    Silver Online Retail manifest SHA-256: __SILVER_HASH__<br>
    Silver product catalog manifest SHA-256: __CATALOG_HASH__<br>
    Gold manifest SHA-256: __GOLD_HASH__
    </p>
  </details>
</main>
<script id="eda-data" type="application/json">__CHART_DATA__</script>
<script>
(() => {
  "use strict";
  const data = JSON.parse(document.getElementById("eda-data").textContent);
  const namespace = "http://www.w3.org/2000/svg";
  const numberFormat = new Intl.NumberFormat("en-US");

  function node(tag, attributes = {}, text = null) {
    const element = document.createElementNS(namespace, tag);
    for (const [key, value] of Object.entries(attributes)) {
      element.setAttribute(key, String(value));
    }
    if (text !== null) element.textContent = String(text);
    return element;
  }

  function addText(svg, x, y, text, attributes = {}) {
    svg.appendChild(node("text", { x, y, ...attributes }, text));
  }

  function addTitle(element, text) {
    element.appendChild(node("title", {}, text));
  }

  function newChart(containerId, width, height, label) {
    const svg = node("svg", {
      viewBox: `0 0 ${width} ${height}`,
      role: "img",
      "aria-label": label,
      class: "interactive-chart",
    });
    document.getElementById(containerId).replaceChildren(svg);
    return svg;
  }

  function drawMonthly() {
    const rows = data.monthlyPurchaseLines;
    const width = 1000, height = 350;
    const left = 72, right = 28, top = 26, bottom = 68;
    const plotWidth = width - left - right;
    const plotHeight = height - top - bottom;
    const maximum = Math.max(...rows.map(row => row.count), 1);
    const svg = newChart("monthly-chart", width, height, "Eligible Silver purchase lines by month");

    for (let tick = 0; tick <= 4; tick++) {
      const value = maximum * tick / 4;
      const y = top + plotHeight - plotHeight * tick / 4;
      svg.appendChild(node("line", { x1: left, y1: y, x2: width - right, y2: y, class: "chart-grid" }));
      addText(svg, left - 10, y + 4, numberFormat.format(Math.round(value)), { "text-anchor": "end", "font-size": 12 });
    }
    const slot = plotWidth / rows.length;
    const barWidth = slot * 0.66;
    rows.forEach((row, index) => {
      const barHeight = plotHeight * row.count / maximum;
      const x = left + index * slot + (slot - barWidth) / 2;
      const y = top + plotHeight - barHeight;
      const bar = node("rect", {
        x, y, width: barWidth, height: barHeight,
        rx: 3, fill: row.partial ? "#e79b37" : "#4472c4", class: "chart-bar",
      });
      addTitle(bar, `${row.month}${row.partial ? "*" : ""}: ${numberFormat.format(row.count)} purchase lines`);
      svg.appendChild(bar);
      const label = node("text", {
        x: x + barWidth / 2,
        y: height - bottom + 16,
        "text-anchor": "end",
        "font-size": 11,
        transform: `rotate(-42 ${x + barWidth / 2} ${height - bottom + 16})`,
      }, `${row.month}${row.partial ? "*" : ""}`);
      svg.appendChild(label);
    });
    addText(svg, width / 2, height - 7, "Invoice month", { "text-anchor": "middle", "font-size": 13 });
  }

  function drawDiversity() {
    const bins = data.customerDiversityHistogram;
    const width = 1000, height = 330;
    const left = 72, right = 26, top = 24, bottom = 56;
    const plotWidth = width - left - right;
    const plotHeight = height - top - bottom;
    const maximum = Math.max(...bins.map(bin => bin.count), 1);
    const slot = plotWidth / bins.length;
    const svg = newChart("diversity-chart", width, height, "Gold customers by number of distinct purchased products");

    for (let tick = 0; tick <= 4; tick++) {
      const y = top + plotHeight - plotHeight * tick / 4;
      svg.appendChild(node("line", { x1: left, y1: y, x2: width - right, y2: y, class: "chart-grid" }));
      addText(svg, left - 10, y + 4, numberFormat.format(Math.round(maximum * tick / 4)), { "text-anchor": "end", "font-size": 12 });
    }
    bins.forEach((bin, index) => {
      const barHeight = plotHeight * bin.count / maximum;
      const x = left + index * slot + 1;
      const bar = node("rect", {
        x, y: top + plotHeight - barHeight, width: Math.max(1, slot - 2), height: barHeight,
        fill: "#70ad47", class: "chart-bar",
      });
      addTitle(bar, `${bin.lower} to ${bin.upper} distinct products: ${numberFormat.format(bin.count)} customers`);
      svg.appendChild(bar);
    });
    const step = Math.max(1, Math.ceil(bins.length / 7));
    for (let index = 0; index < bins.length; index += step) {
      const bin = bins[index];
      addText(svg, left + index * slot, height - bottom + 17, `${bin.lower}`, { "font-size": 11 });
    }
    addText(svg, width / 2, height - 7, "Distinct products per customer (bins are logarithmically spaced)", { "text-anchor": "middle", "font-size": 13 });
  }

  function drawPopularity() {
    const products = data.topProducts;
    const width = 1160, rowHeight = 32, top = 28, bottom = 42;
    const height = top + products.length * rowHeight + bottom;
    const left = 390, right = 74;
    const plotWidth = width - left - right;
    const maximum = Math.max(...products.map(product => product.unique_customers), 1);
    const svg = newChart("popularity-chart", width, height, "Gold products ranked by unique purchasing customers");

    products.forEach((product, index) => {
      const y = top + index * rowHeight;
      const barWidth = plotWidth * product.unique_customers / maximum;
      const label = `${product.stock_code} - ${product.description || "Description unavailable"}`;
      const visibleLabel = label.length > 48 ? `${label.slice(0, 45)}...` : label;
      const text = node("text", { x: left - 12, y: y + 19, "text-anchor": "end", "font-size": 12 }, visibleLabel);
      addTitle(text, label);
      svg.appendChild(text);
      const bar = node("rect", {
        x: left, y: y + 3, width: barWidth, height: 22, rx: 3,
        fill: "#ed7d31", class: "chart-bar",
      });
      addTitle(bar, `${label}; ${numberFormat.format(product.unique_customers)} unique customers; ${numberFormat.format(product.purchase_lines)} purchase lines`);
      svg.appendChild(bar);
      addText(svg, left + barWidth + 8, y + 19, numberFormat.format(product.unique_customers), { "font-size": 12 });
    });
    addText(svg, left + plotWidth / 2, height - 8, "Unique customers", { "text-anchor": "middle", "font-size": 13 });
  }

  function drawSparsity() {
    const matrix = data.interactionMatrix;
    const heatmap = data.heatmap;
    const width = 1180, height = 590;
    const svg = newChart("sparsity-chart", width, height, "Gold customer-product matrix sparsity and ranked interaction heatmap");
    const barX = 46, barY = 100, barWidth = 390, barHeight = 30;
    const observedWidth = barWidth * matrix.density_percent / 100;
    addText(svg, barX, 36, `${matrix.sparsity_percent.toFixed(2)}% of possible active pairs are unobserved`, { "font-size": 16, "font-weight": "600" });
    addText(svg, barX, 66, `${numberFormat.format(matrix.observed_customer_product_pairs)} observed / ${numberFormat.format(matrix.possible_active_customer_product_pairs)} possible`, { "font-size": 13 });
    svg.appendChild(node("rect", { x: barX, y: barY, width: barWidth, height: barHeight, rx: 4, fill: "#d9e2f3" }));
    svg.appendChild(node("rect", { x: barX, y: barY, width: Math.max(observedWidth, 2), height: barHeight, rx: 3, fill: "#4472c4" }));
    addText(svg, barX, barY + 58, `Observed density: ${matrix.density_percent.toFixed(2)}%`, { "font-size": 13, fill: "#2456a6" });
    addText(svg, barX, barY + 86, "Missing interactions are unknown, not negative feedback.", { "font-size": 12 });

    const cell = 20, heatX = 560, heatY = 102;
    addText(svg, heatX, 35, `Top ${heatmap.customers.length} customers × top ${heatmap.products.length} products`, { "font-size": 16, "font-weight": "600" });
    addText(svg, heatX, 62, `Subset density: ${matrix.top_customer_product_subset_density_percent.toFixed(1)}%`, { "font-size": 13 });
    heatmap.purchased.forEach((row, rowIndex) => {
      row.forEach((purchased, columnIndex) => {
        const product = heatmap.products[columnIndex];
        const customer = heatmap.customers[rowIndex];
        const cellNode = node("rect", {
          x: heatX + columnIndex * cell,
          y: heatY + rowIndex * cell,
          width: cell - 1,
          height: cell - 1,
          fill: purchased ? "#244e9b" : "#edf2f8",
          class: "heat-cell",
        });
        addTitle(cellNode, `Customer ${customer.customer_id} × ${product.stock_code} ${product.description}: ${purchased ? "purchased" : "not observed"}`);
        svg.appendChild(cellNode);
      });
    });
    [0, 4, 9, 14, 19].forEach(index => {
      if (index < heatmap.customers.length) {
        addText(svg, heatX - 10, heatY + index * cell + 14, String(index + 1), { "text-anchor": "end", "font-size": 11 });
      }
      if (index < heatmap.products.length) {
        addText(svg, heatX + index * cell + 8, heatY + heatmap.customers.length * cell + 15, String(index + 1), { "text-anchor": "middle", "font-size": 11 });
      }
    });
  }

  drawMonthly();
  drawDiversity();
  drawPopularity();
  drawSparsity();
})();
</script>
</body>
</html>
"""
    replacements = {
        "__GENERATED_AT__": html.escape(summary["generated_at_utc"]),
        "__PERIOD_DESCRIPTION__": html.escape(period_description),
        "__DATE_RANGE__": html.escape(date_range),
        "__PURCHASE_LINES__": f"{counts['eligible_purchase_lines_analyzed']:,}",
        "__CUSTOMERS__": f"{matrix['active_customers']:,}",
        "__PRODUCTS__": f"{matrix['active_products']:,}",
        "__SPARSITY__": f"{matrix['sparsity_percent']:.2f}",
        "__OBSERVED_PAIRS__": f"{matrix['observed_customer_product_pairs']:,}",
        "__POSSIBLE_PAIRS__": f"{matrix['possible_active_customer_product_pairs']:,}",
        "__UNOBSERVED_PAIRS__": (
            f"{matrix['possible_active_customer_product_pairs'] - matrix['observed_customer_product_pairs']:,}"
        ),
        "__BUSIEST_MONTH__": html.escape(busiest_month["month"]),
        "__BUSIEST_COUNT__": f"{busiest_month['count']:,}",
        "__TOP_PRODUCT__": top_product_name,
        "__TOP_PRODUCT_CUSTOMERS__": f"{int(top_product['unique_customers']):,}",
        "__DENSITY__": f"{matrix['density_percent']:.2f}",
        "__MONTHLY_CAPTION__": html.escape(monthly_caption),
        "__TOP_ITEM_ROWS__": top_item_rows,
        "__CACHE_VERSION__": cache_version,
        "__SILVER_HASH__": html.escape(summary["source"]["silver_manifest_sha256"]),
        "__CATALOG_HASH__": html.escape(
            summary["source"]["silver_product_catalog_manifest_sha256"]
        ),
        "__GOLD_HASH__": html.escape(summary["source"]["gold_manifest_sha256"]),
        "__CHART_DATA__": chart_data_json,
    }
    for placeholder, value in replacements.items():
        page = page.replace(placeholder, value)
    _write_text_atomic(output_dir / "index.html", page)


def generate_eda(
    silver_root: Path,
    gold_root: Path,
    output_dir: Path,
) -> dict:
    silver, silver_manifest_sha256 = analyze_silver(silver_root)
    gold = analyze_gold(gold_root)
    if gold.silver_manifest_sha256 != silver_manifest_sha256:
        raise ValueError(
            "Gold was built from a different Silver Online Retail manifest; "
            "rerun Gold before generating EDA."
        )
    _, silver_product_catalog_manifest_sha256 = read_silver_manifest(
        silver_root,
        "product_catalog",
    )
    if (
        gold.silver_product_catalog_manifest_sha256
        != silver_product_catalog_manifest_sha256
    ):
        raise ValueError(
            "Gold was built from a different Silver product-catalog manifest; "
            "rerun Gold before generating EDA."
        )
    if gold.purchase_lines != silver.purchase_lines:
        raise ValueError(
            f"Gold interactions contain {gold.purchase_lines} purchase lines, "
            f"but Silver analysis found {silver.purchase_lines}"
        )

    monthly_lines = [
        {
            "month": month,
            "count": silver.monthly_lines[month],
            "partial": (
                _last_month_is_partial(silver.last_purchase_date)
                and month == silver.last_purchase_date[:7]
            ),
        }
        for month in sorted(silver.monthly_lines)
    ]
    diversity_histogram = _customer_diversity_histogram(
        list(gold.customers.values())
    )
    matrix, heatmap = _interaction_matrix_summary(gold)
    output_dir.mkdir(parents=True, exist_ok=True)
    plots_dir = output_dir / "plots"
    plots_dir.mkdir(parents=True, exist_ok=True)

    _write_interaction_plot(
        monthly_lines,
        diversity_histogram,
        plots_dir / "interaction_patterns.png",
    )
    _write_item_popularity_plot(
        gold.top_products,
        plots_dir / "item_popularity.png",
    )
    _write_sparsity_plot(
        matrix,
        heatmap,
        plots_dir / "interaction_sparsity.png",
    )

    summary = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "source": {
            "silver_dataset": "online_retail",
            "silver_manifest_sha256": silver_manifest_sha256,
            "silver_product_catalog_manifest_sha256": (
                silver_product_catalog_manifest_sha256
            ),
            "gold_manifest_sha256": gold.gold_manifest_sha256,
        },
        "recommendation_policy": {
            "include_only_recommendation_eligible_rows": True,
            "exclude_duplicate_content": True,
        },
        "counts": {
            "silver_rows": silver.silver_rows,
            "eligible_rows_including_duplicates": silver.eligible_rows,
            "duplicate_eligible_rows_excluded": silver.duplicate_rows_excluded,
            "eligible_purchase_lines_analyzed": silver.purchase_lines,
            "gold_customer_product_interactions": gold.interaction_pairs,
        },
        "period": {
            "first_eligible_invoice_date": silver.first_purchase_date,
            "last_eligible_invoice_date": silver.last_purchase_date,
            "final_month_is_partial": _last_month_is_partial(
                silver.last_purchase_date
            ),
        },
        "interaction_matrix": matrix,
        "monthly_purchase_lines": monthly_lines,
        "customer_product_diversity_histogram": diversity_histogram,
        "top_products_by_unique_customers": gold.top_products,
        "top_customer_product_heatmap": heatmap,
    }
    _write_summary(output_dir / "summary.json", summary)
    _write_dashboard(output_dir, summary)
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate EDA from Silver transaction dates and Gold features."
    )
    parser.add_argument(
        "--silver-root",
        type=Path,
        default=_DEFAULT_SILVER_ROOT,
        help="Directory containing the Silver dataset folders.",
    )
    parser.add_argument(
        "--gold-root",
        type=Path,
        default=_DEFAULT_GOLD_ROOT,
        help="Directory containing the Gold feature tables and manifest.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=_DEFAULT_OUTPUT_DIR,
        help="Directory for the EDA summary, dashboard, and chart exports.",
    )
    arguments = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    summary = generate_eda(
        arguments.silver_root,
        arguments.gold_root,
        arguments.output_dir,
    )
    logging.info(
        "EDA complete: %d Silver purchase lines, %d Gold customer-product pairs, "
        "%.2f%% Gold matrix sparsity",
        summary["counts"]["eligible_purchase_lines_analyzed"],
        summary["interaction_matrix"]["observed_customer_product_pairs"],
        summary["interaction_matrix"]["sparsity_percent"],
    )
    logging.info("EDA dashboard: %s", (arguments.output_dir / "index.html").resolve())


if __name__ == "__main__":
    main()

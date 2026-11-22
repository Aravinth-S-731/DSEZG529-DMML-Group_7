import csv
import gzip
import os
import tempfile
from collections import Counter
from contextlib import ExitStack
from datetime import datetime
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from pathlib import Path

from silver.common import (
    BRONZE_METADATA_COLUMNS,
    canonical_decimal,
    open_bronze_csv,
    sha256_file,
)


TRANSACTION_COLUMNS = (
    "InvoiceNo",
    "StockCode",
    "Description",
    "Quantity",
    "InvoiceDate",
    "UnitPrice",
    "CustomerID",
    "Country",
)
BRONZE_COLUMNS = (*TRANSACTION_COLUMNS, *BRONZE_METADATA_COLUMNS)
SILVER_COLUMNS = (
    "invoice_no",
    "stock_code",
    "description",
    "quantity",
    "invoice_datetime",
    "unit_price",
    "customer_id",
    "country",
    "line_total",
    "is_cancelled",
    "is_return",
    "is_adjustment",
    "is_recommendation_eligible",
    "is_duplicate_content",
    "quality_flags",
    *BRONZE_METADATA_COLUMNS,
)


def _normalize_transaction(
    row: dict,
    seen_record_hashes: set[str],
) -> tuple[dict[str, str], list[str]]:
    invoice_no = (row.get("InvoiceNo") or "").strip()
    stock_code = (row.get("StockCode") or "").strip().upper()
    if not invoice_no:
        raise ValueError("missing_invoice_number")
    if not stock_code:
        raise ValueError("missing_stock_code")

    date_value = (row.get("InvoiceDate") or "").strip()
    try:
        invoice_datetime = datetime.fromisoformat(date_value)
    except ValueError as error:
        raise ValueError("invalid_invoice_datetime") from error

    try:
        quantity_decimal = Decimal((row.get("Quantity") or "").strip())
        if not quantity_decimal.is_finite() or quantity_decimal != quantity_decimal.to_integral_value():
            raise ValueError("invalid_quantity")
        quantity = int(quantity_decimal)
        price = Decimal((row.get("UnitPrice") or "").strip())
        if not price.is_finite():
            raise ValueError("invalid_unit_price")
        price = price.quantize(Decimal("0.001"), rounding=ROUND_HALF_UP)
    except (InvalidOperation, OverflowError) as error:
        raise ValueError("invalid_numeric_value") from error

    customer_id_value = (row.get("CustomerID") or "").strip()
    flags = []
    if customer_id_value:
        try:
            customer_decimal = Decimal(customer_id_value)
            if not customer_decimal.is_finite() or customer_decimal != customer_decimal.to_integral_value():
                raise InvalidOperation
            customer_id = str(int(customer_decimal))
        except (InvalidOperation, ValueError, OverflowError):
            customer_id = ""
            flags.append("invalid_customer_id")
    else:
        customer_id = ""
        flags.append("missing_customer_id")

    description = " ".join((row.get("Description") or "").split())
    country = " ".join((row.get("Country") or "").split())
    if not description:
        flags.append("missing_description")
    if not country:
        flags.append("missing_country")

    is_cancelled = invoice_no.upper().startswith("C")
    is_return = quantity < 0
    is_adjustment = quantity == 0 or price < 0
    if is_cancelled:
        flags.append("cancelled_invoice")
    if is_return:
        flags.append("negative_quantity_return")
    elif quantity == 0:
        flags.append("zero_quantity")
    if price < 0:
        flags.append("negative_unit_price")

    record_hash = (row.get("_record_hash") or "").strip()
    if not record_hash:
        raise ValueError("missing_bronze_record_hash")
    is_duplicate = record_hash in seen_record_hashes
    seen_record_hashes.add(record_hash)
    if is_duplicate:
        flags.append("duplicate_record_content")

    recommendation_eligible = (
        bool(customer_id)
        and not is_cancelled
        and quantity > 0
        and price >= 0
    )
    line_total = (Decimal(quantity) * price).quantize(
        Decimal("0.001"),
        rounding=ROUND_HALF_UP,
    )
    silver_row = {
        "invoice_no": invoice_no,
        "stock_code": stock_code,
        "description": description,
        "quantity": str(quantity),
        "invoice_datetime": invoice_datetime.isoformat(timespec="seconds"),
        "unit_price": canonical_decimal(price),
        "customer_id": customer_id,
        "country": country,
        "line_total": canonical_decimal(line_total),
        "is_cancelled": str(is_cancelled).lower(),
        "is_return": str(is_return).lower(),
        "is_adjustment": str(is_adjustment).lower(),
        "is_recommendation_eligible": str(recommendation_eligible).lower(),
        "is_duplicate_content": str(is_duplicate).lower(),
        "quality_flags": "|".join(flags),
    }
    for column in BRONZE_METADATA_COLUMNS:
        silver_row[column] = (row.get(column) or "").strip()
    return silver_row, flags


def process_transaction_batch(
    bronze_path: Path,
    output_dir: Path,
    expected_sha256: str,
    expected_row_count: int,
) -> dict:
    output_dir.mkdir(parents=True, exist_ok=True)
    silver_path = output_dir / f"{bronze_path.name}.gz"
    quarantine_path = (
        output_dir / "quarantine" / f"invalid_{bronze_path.name}.gz"
    )
    temp_silver: Path | None = None
    temp_quarantine: Path | None = None
    metrics = Counter()
    seen_record_hashes: set[str] = set()
    input_rows = 0
    silver_rows = 0

    try:
        descriptor, temp_name = tempfile.mkstemp(
            dir=output_dir,
            prefix=".silver-",
            suffix=".csv.gz.tmp",
        )
        os.close(descriptor)
        temp_silver = Path(temp_name)
        descriptor, temp_name = tempfile.mkstemp(
            dir=output_dir,
            prefix=".quarantine-",
            suffix=".csv.gz.tmp",
        )
        os.close(descriptor)
        temp_quarantine = Path(temp_name)

        with ExitStack() as stack:
            reader = stack.enter_context(
                open_bronze_csv(bronze_path, expected_sha256, BRONZE_COLUMNS)
            )
            silver_file = stack.enter_context(
                gzip.open(
                    temp_silver,
                    mode="wt",
                    encoding="utf-8",
                    newline="",
                )
            )
            quarantine_file = stack.enter_context(
                gzip.open(
                    temp_quarantine,
                    mode="wt",
                    encoding="utf-8",
                    newline="",
                )
            )
            silver_writer = csv.DictWriter(
                silver_file,
                fieldnames=SILVER_COLUMNS,
                lineterminator="\n",
            )
            quarantine_columns = (*BRONZE_COLUMNS, "_silver_error")
            quarantine_writer = csv.DictWriter(
                quarantine_file,
                fieldnames=quarantine_columns,
                lineterminator="\n",
            )
            silver_writer.writeheader()
            quarantine_writer.writeheader()

            for input_rows, row in enumerate(reader, start=1):
                if None in row:
                    raw_row = {
                        key: value or ""
                        for key, value in row.items()
                        if isinstance(key, str)
                    }
                    quarantine_writer.writerow(
                        {
                            **raw_row,
                            "_silver_error": "unexpected_extra_columns",
                        }
                    )
                    metrics["quarantined_rows"] += 1
                    metrics["unexpected_extra_columns"] += 1
                    continue
                try:
                    silver_row, flags = _normalize_transaction(row, seen_record_hashes)
                except ValueError as error:
                    raw_row = {column: row.get(column) or "" for column in BRONZE_COLUMNS}
                    quarantine_writer.writerow(
                        {**raw_row, "_silver_error": str(error)}
                    )
                    metrics["quarantined_rows"] += 1
                    metrics[str(error)] += 1
                    continue

                silver_writer.writerow(silver_row)
                silver_rows += 1
                metrics["silver_rows"] += 1
                for flag in flags:
                    metrics[flag] += 1
                if silver_row["is_recommendation_eligible"] == "true":
                    metrics["recommendation_eligible_rows"] += 1

            silver_file.flush()
            quarantine_file.flush()

        for temp_path in (temp_silver, temp_quarantine):
            with temp_path.open("rb+") as staged_file:
                os.fsync(staged_file.fileno())

        if input_rows != expected_row_count:
            raise ValueError(
                f"Row-count mismatch for {bronze_path.name}: Bronze manifest "
                f"states {expected_row_count}, file contains {input_rows}"
            )

        silver_sha256 = sha256_file(temp_silver)
        silver_size = temp_silver.stat().st_size
        os.replace(temp_silver, silver_path)
        temp_silver = None

        if metrics["quarantined_rows"]:
            quarantine_path.parent.mkdir(parents=True, exist_ok=True)
            os.replace(temp_quarantine, quarantine_path)
            temp_quarantine = None
        else:
            quarantine_path.unlink(missing_ok=True)
    finally:
        if temp_silver is not None:
            temp_silver.unlink(missing_ok=True)
        if temp_quarantine is not None:
            temp_quarantine.unlink(missing_ok=True)

    return {
        "bronze_file": bronze_path.name,
        "bronze_sha256": expected_sha256,
        "input_rows": input_rows,
        "silver_rows": silver_rows,
        "quarantined_rows": metrics["quarantined_rows"],
        "silver_file": silver_path.name,
        "silver_sha256": silver_sha256,
        "silver_size_bytes": silver_size,
        "quarantine_file": (
            str(quarantine_path.relative_to(output_dir))
            if metrics["quarantined_rows"]
            else None
        ),
        "quality_metrics": dict(sorted(metrics.items())),
    }

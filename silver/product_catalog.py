import csv
import gzip
import os
import tempfile
from collections import Counter
from contextlib import ExitStack
from pathlib import Path

from silver.common import BRONZE_METADATA_COLUMNS, open_bronze_csv, sha256_file


PRODUCT_COLUMNS = ("stock_code", "description")
BRONZE_COLUMNS = (*PRODUCT_COLUMNS, *BRONZE_METADATA_COLUMNS)
SILVER_COLUMNS = (
    "stock_code",
    "description",
    "is_duplicate_stock_code",
    "quality_flags",
    *BRONZE_METADATA_COLUMNS,
)


def process_product_catalog(
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
    seen_stock_codes: set[str] = set()
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
            quarantine_writer = csv.DictWriter(
                quarantine_file,
                fieldnames=(*BRONZE_COLUMNS, "_silver_error"),
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

                raw_stock_code = (row.get("stock_code") or "").strip()
                if not raw_stock_code:
                    quarantine_writer.writerow(
                        {
                            **{column: row.get(column) or "" for column in BRONZE_COLUMNS},
                            "_silver_error": "missing_stock_code",
                        }
                    )
                    metrics["quarantined_rows"] += 1
                    metrics["missing_stock_code"] += 1
                    continue
                if not (row.get("_record_hash") or "").strip():
                    quarantine_writer.writerow(
                        {
                            **{column: row.get(column) or "" for column in BRONZE_COLUMNS},
                            "_silver_error": "missing_bronze_record_hash",
                        }
                    )
                    metrics["quarantined_rows"] += 1
                    metrics["missing_bronze_record_hash"] += 1
                    continue

                stock_code = raw_stock_code.upper()
                is_duplicate = stock_code in seen_stock_codes
                seen_stock_codes.add(stock_code)
                description = " ".join((row.get("description") or "").split())
                flags = []
                if not description:
                    flags.append("missing_description")
                    metrics["missing_description"] += 1
                if is_duplicate:
                    flags.append("duplicate_stock_code")
                    metrics["duplicate_stock_code_rows"] += 1
                silver_row = {
                    "stock_code": stock_code,
                    "description": description,
                    "is_duplicate_stock_code": str(is_duplicate).lower(),
                    "quality_flags": "|".join(flags),
                }
                for column in BRONZE_METADATA_COLUMNS:
                    silver_row[column] = (row.get(column) or "").strip()
                silver_writer.writerow(silver_row)
                silver_rows += 1
                metrics["silver_rows"] += 1

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

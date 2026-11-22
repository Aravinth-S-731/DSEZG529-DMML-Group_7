from datetime import datetime
from pathlib import Path

from bronze.common import enrich_csv


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


def get_transaction_batches(landing_dir: Path, landing_manifest: dict) -> list[dict]:
    if landing_manifest.get("format") != "CSV, one file per InvoiceDate month":
        raise ValueError("Unexpected transaction landing format")
    if tuple(landing_manifest.get("columns", ())) != TRANSACTION_COLUMNS:
        raise ValueError("Transaction landing manifest has an unsupported schema")

    batches = landing_manifest.get("monthly_batches")
    if not isinstance(batches, list) or not batches:
        raise ValueError("Transaction landing manifest contains no monthly batches")

    results = []
    for batch in batches:
        if not isinstance(batch, dict):
            raise ValueError("Invalid transaction batch metadata")
        filename = batch.get("file")
        month = batch.get("month")
        source_sha256 = batch.get("sha256")
        row_count = batch.get("row_count")
        if (
            not isinstance(filename, str)
            or Path(filename).name != filename
            or not filename.startswith("transactions_")
            or not filename.endswith(".csv")
            or not isinstance(month, str)
            or not isinstance(source_sha256, str)
            or isinstance(row_count, bool)
            or not isinstance(row_count, int)
            or row_count < 0
        ):
            raise ValueError(f"Invalid transaction batch metadata: {batch!r}")
        source_path = landing_dir / filename
        if not source_path.is_file():
            raise FileNotFoundError(f"Landing batch not found: {source_path}")
        results.append(
            {
                "source_path": source_path,
                "source_sha256": source_sha256,
                "row_count": row_count,
                "batch_id": month,
            }
        )
    return results


def process_batch(
    batch: dict,
    destination_dir: Path,
    source_type: str,
    ingested_at: datetime,
) -> dict:
    source_path = batch["source_path"]
    return enrich_csv(
        source_path=source_path,
        destination_path=destination_dir / source_path.name,
        expected_columns=TRANSACTION_COLUMNS,
        dataset="online_retail",
        source_type=source_type,
        source_sha256=batch["source_sha256"],
        batch_id=batch["batch_id"],
        ingested_at=ingested_at,
        expected_row_count=batch["row_count"],
    )

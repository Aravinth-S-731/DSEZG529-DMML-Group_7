from datetime import datetime
from pathlib import Path

from bronze.common import enrich_csv


PRODUCT_COLUMNS = ("stock_code", "description")


def get_product_catalog_batch(landing_dir: Path, landing_manifest: dict) -> dict:
    if landing_manifest.get("format") != "CSV":
        raise ValueError("Unexpected product-catalog landing format")
    if tuple(landing_manifest.get("columns", ())) != PRODUCT_COLUMNS:
        raise ValueError("Product-catalog landing manifest has an unsupported schema")

    processed_file = landing_manifest.get("processed_file")
    if not isinstance(processed_file, dict):
        raise ValueError("Product-catalog landing manifest has no processed_file entry")
    filename = processed_file.get("file")
    source_sha256 = processed_file.get("sha256")
    row_count = landing_manifest.get("row_count")
    if (
        not isinstance(filename, str)
        or Path(filename).name != filename
        or filename != "products.csv"
        or not isinstance(source_sha256, str)
        or isinstance(row_count, bool)
        or not isinstance(row_count, int)
        or row_count < 0
    ):
        raise ValueError("Invalid product-catalog file metadata")

    batch_id = landing_manifest.get("created_at_utc")
    if not isinstance(batch_id, str) or not batch_id:
        raise ValueError("Product-catalog landing manifest has no creation timestamp")
    source_path = landing_dir / filename
    if not source_path.is_file():
        raise FileNotFoundError(f"Landing product catalog not found: {source_path}")
    return {
        "source_path": source_path,
        "source_sha256": source_sha256,
        "row_count": row_count,
        "batch_id": batch_id,
    }


def process_catalog(
    batch: dict,
    destination_dir: Path,
    source_type: str,
    ingested_at: datetime,
) -> dict:
    source_path = batch["source_path"]
    return enrich_csv(
        source_path=source_path,
        destination_path=destination_dir / source_path.name,
        expected_columns=PRODUCT_COLUMNS,
        dataset="product_catalog",
        source_type=source_type,
        source_sha256=batch["source_sha256"],
        batch_id=batch["batch_id"],
        ingested_at=ingested_at,
        expected_row_count=batch["row_count"],
    )

import argparse
import json
import logging
from datetime import datetime, timezone
from pathlib import Path

from silver.common import sha256_file, write_json_atomic
from silver.product_catalog import process_product_catalog
from silver.transactions import process_transaction_batch


_PROJECT_ROOT = Path(__file__).resolve().parents[1]
_DEFAULT_BRONZE_ROOT = _PROJECT_ROOT / "data" / "bronze"
_DEFAULT_SILVER_ROOT = _PROJECT_ROOT / "data" / "silver"
_SUPPORTED_DATASETS = {"online_retail", "product_catalog"}


def _read_bronze_manifest(bronze_dir: Path, dataset: str) -> tuple[dict, str]:
    if not bronze_dir.is_dir():
        raise NotADirectoryError(f"Bronze dataset directory not found: {bronze_dir}")
    manifest_path = bronze_dir / "bronze_manifest.json"
    if not manifest_path.is_file():
        raise FileNotFoundError(f"Bronze manifest not found: {manifest_path}")

    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if not isinstance(manifest, dict):
        raise ValueError(f"Bronze manifest must be a JSON object: {manifest_path}")
    if manifest.get("dataset") != dataset:
        raise ValueError(
            f"Bronze manifest dataset mismatch: expected {dataset}, "
            f"received {manifest.get('dataset')!r}"
        )
    if manifest.get("schema_version") != "1":
        raise ValueError(
            f"Unsupported Bronze schema version: {manifest.get('schema_version')!r}"
        )
    batches = manifest.get("batches")
    if not isinstance(batches, dict) or not batches:
        raise ValueError(f"Bronze manifest contains no batches: {manifest_path}")
    if not all(
        isinstance(filename, str) and isinstance(entry, dict)
        for filename, entry in batches.items()
    ):
        raise ValueError(f"Invalid batch entries in Bronze manifest: {manifest_path}")
    return manifest, sha256_file(manifest_path)


def _select_batches(dataset: str, batches: dict) -> list[tuple[str, dict]]:
    if dataset == "online_retail":
        selected = [
            (filename, entry)
            for filename, entry in batches.items()
            if filename.startswith("transactions_") and filename.endswith(".csv")
        ]
    else:
        selected = [(filename, batches[filename]) for filename in ("products.csv",) if filename in batches]
    if not selected:
        raise ValueError(f"No supported Bronze CSV batches found for {dataset}")
    return sorted(selected, key=lambda item: item[0])


def run_silver(
    dataset: str,
    bronze_root: Path = _DEFAULT_BRONZE_ROOT,
    silver_root: Path = _DEFAULT_SILVER_ROOT,
) -> Path:
    if dataset not in _SUPPORTED_DATASETS:
        raise ValueError(f"Unsupported Silver dataset: {dataset}")

    bronze_dir = bronze_root / dataset
    bronze_manifest, bronze_manifest_sha256 = _read_bronze_manifest(bronze_dir, dataset)
    selected_batches = _select_batches(dataset, bronze_manifest["batches"])
    silver_dir = silver_root / dataset
    silver_dir.mkdir(parents=True, exist_ok=True)

    output_files = []
    aggregate_metrics: dict[str, int] = {}
    total_input_rows = 0
    total_silver_rows = 0
    total_quarantined_rows = 0

    for filename, batch in selected_batches:
        if batch.get("bronze_file") != filename:
            raise ValueError(f"Bronze manifest filename mismatch for {filename}")
        source_path = bronze_dir / filename
        if not source_path.is_file():
            raise FileNotFoundError(f"Bronze file listed in manifest is missing: {source_path}")
        source_sha256 = batch.get("bronze_sha256")
        row_count = batch.get("row_count")
        if (
            not isinstance(source_sha256, str)
            or isinstance(row_count, bool)
            or not isinstance(row_count, int)
            or row_count < 0
        ):
            raise ValueError(f"Invalid Bronze file metadata for {filename}")

        if dataset == "online_retail":
            result = process_transaction_batch(
                source_path,
                silver_dir,
                source_sha256,
                row_count,
            )
        else:
            result = process_product_catalog(
                source_path,
                silver_dir,
                source_sha256,
                row_count,
            )
        output_files.append(result)
        total_input_rows += result["input_rows"]
        total_silver_rows += result["silver_rows"]
        total_quarantined_rows += result["quarantined_rows"]
        for metric, value in result["quality_metrics"].items():
            aggregate_metrics[metric] = aggregate_metrics.get(metric, 0) + value
        logging.info(
            "Silver processed %s: input=%d, silver=%d, quarantined=%d",
            filename,
            result["input_rows"],
            result["silver_rows"],
            result["quarantined_rows"],
        )

    generated_at = datetime.now(timezone.utc).isoformat()
    quality_report = {
        "dataset": dataset,
        "generated_at_utc": generated_at,
        "bronze_manifest_sha256": bronze_manifest_sha256,
        "input_rows": total_input_rows,
        "silver_rows": total_silver_rows,
        "quarantined_rows": total_quarantined_rows,
        "metrics": dict(sorted(aggregate_metrics.items())),
        "files": output_files,
    }
    write_json_atomic(silver_dir / "quality_report.json", quality_report)
    silver_manifest = {
        "dataset": dataset,
        "schema_version": "1",
        "created_at_utc": generated_at,
        "bronze_directory": str(bronze_dir.resolve()),
        "bronze_manifest_sha256": bronze_manifest_sha256,
        "input_batch_count": len(selected_batches),
        "input_rows": total_input_rows,
        "silver_rows": total_silver_rows,
        "quarantined_rows": total_quarantined_rows,
        "files": [
            {
                "bronze_file": item["bronze_file"],
                "bronze_sha256": item["bronze_sha256"],
                "silver_file": item["silver_file"],
                "silver_sha256": item["silver_sha256"],
                "row_count": item["silver_rows"],
                "quarantined_rows": item["quarantined_rows"],
                "quarantine_file": item["quarantine_file"],
            }
            for item in output_files
        ],
    }
    write_json_atomic(silver_dir / "silver_manifest.json", silver_manifest)
    logging.info(
        "Silver complete for %s: input=%d, silver=%d, quarantined=%d",
        dataset,
        total_input_rows,
        total_silver_rows,
        total_quarantined_rows,
    )
    return silver_dir


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Validate Bronze data, standardize records, and produce Silver CSVs."
    )
    parser.add_argument(
        "--dataset",
        choices=sorted(_SUPPORTED_DATASETS),
        required=True,
    )
    parser.add_argument("--bronze-root", type=Path, default=_DEFAULT_BRONZE_ROOT)
    parser.add_argument("--silver-root", type=Path, default=_DEFAULT_SILVER_ROOT)
    arguments = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    silver_dir = run_silver(
        arguments.dataset,
        bronze_root=arguments.bronze_root,
        silver_root=arguments.silver_root,
    )
    print(silver_dir)


if __name__ == "__main__":
    main()

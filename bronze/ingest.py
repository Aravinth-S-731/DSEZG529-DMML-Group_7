import argparse
import json
import logging
from datetime import datetime, timezone
from pathlib import Path

from bronze.common import METADATA_COLUMNS, SCHEMA_VERSION, sha256_file, write_json_atomic
from bronze.product_catalog import get_product_catalog_batch, process_catalog
from bronze.transactions import get_transaction_batches, process_batch


_DEFAULT_OUTPUT_ROOT = Path(__file__).resolve().parents[1] / "data" / "bronze"


def _read_landing_manifest(landing_dir: Path) -> tuple[dict, str]:
    if not landing_dir.is_dir():
        raise NotADirectoryError(f"Landing dataset directory not found: {landing_dir}")
    manifest_path = landing_dir / "manifest.json"
    if not manifest_path.is_file():
        raise FileNotFoundError(f"Landing manifest not found: {manifest_path}")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if not isinstance(manifest, dict):
        raise ValueError(f"Landing manifest must be a JSON object: {manifest_path}")
    return manifest, sha256_file(manifest_path)


def _load_existing_batches(bronze_manifest_path: Path, dataset: str) -> dict[str, dict]:
    if not bronze_manifest_path.is_file():
        return {}
    existing = json.loads(bronze_manifest_path.read_text(encoding="utf-8"))
    if not isinstance(existing, dict):
        raise ValueError(f"Bronze manifest must be a JSON object: {bronze_manifest_path}")
    if (
        existing.get("dataset") == dataset
        and existing.get("schema_version") == SCHEMA_VERSION
        and isinstance(existing.get("batches"), dict)
    ):
        batches = existing["batches"]
        if not all(isinstance(name, str) and isinstance(entry, dict) for name, entry in batches.items()):
            raise ValueError(f"Invalid batch entries in Bronze manifest: {bronze_manifest_path}")
        return batches

    logging.warning(
        "Existing Bronze manifest at %s uses an older layout; source batches will be rebuilt",
        bronze_manifest_path,
    )
    return {}


def ingest_landing_dataset(landing_dir: Path, output_root: Path = _DEFAULT_OUTPUT_ROOT) -> Path:
    landing_dir = landing_dir.resolve()
    landing_manifest, landing_manifest_sha256 = _read_landing_manifest(landing_dir)
    dataset = landing_manifest.get("dataset")
    source_type = landing_manifest.get("source_type")
    if (
        not isinstance(dataset, str)
        or dataset not in {"online_retail", "product_catalog"}
    ):
        raise ValueError(f"Unsupported landing dataset: {dataset!r}")
    if not isinstance(source_type, str) or not source_type:
        raise ValueError("Landing manifest must contain a source_type")

    if dataset == "online_retail":
        source_batches = get_transaction_batches(landing_dir, landing_manifest)
        process_source = process_batch
    else:
        source_batches = [get_product_catalog_batch(landing_dir, landing_manifest)]
        process_source = process_catalog

    bronze_dir = output_root / dataset
    bronze_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = bronze_dir / "bronze_manifest.json"
    batches = _load_existing_batches(manifest_path, dataset)
    ingested_at = datetime.now(timezone.utc)
    ingested_count = 0
    skipped_count = 0
    current_source_files = set()

    for source_batch in source_batches:
        source_path = source_batch["source_path"]
        source_file = source_path.name
        current_source_files.add(source_file)
        actual_source_sha256 = sha256_file(source_path)
        if actual_source_sha256 != source_batch["source_sha256"]:
            raise ValueError(
                f"Landing checksum mismatch for {source_path}: "
                f"expected {source_batch['source_sha256']}, got {actual_source_sha256}"
            )

        output_path = bronze_dir / source_file
        previous = batches.get(source_file, {})
        if (
            previous.get("source_sha256") == actual_source_sha256
            and previous.get("bronze_sha256")
            and output_path.is_file()
            and sha256_file(output_path) == previous["bronze_sha256"]
        ):
            previous["last_seen_at_utc"] = ingested_at.isoformat()
            previous["status"] = "skipped_unchanged"
            skipped_count += 1
            logging.info("Skipped unchanged batch %s", source_file)
            continue

        result = process_source(
            source_batch,
            bronze_dir,
            source_type,
            ingested_at,
        )
        result["status"] = (
            "reprocessed"
            if previous.get("source_sha256")
            else "ingested"
        )
        result["landing_manifest_sha256"] = landing_manifest_sha256
        result["last_seen_at_utc"] = ingested_at.isoformat()
        batches[source_file] = result
        ingested_count += 1
        logging.info(
            "%s %s: %d rows, record hashes and lineage added",
            result["status"].capitalize(),
            source_file,
            result["row_count"],
        )

    run_summary = {
        "input_batch_count": len(source_batches),
        "ingested_or_reprocessed_count": ingested_count,
        "skipped_unchanged_count": skipped_count,
        "current_input_rows": sum(batch["row_count"] for batch in source_batches),
        "current_input_files": sorted(current_source_files),
    }
    bronze_manifest = {
        "dataset": dataset,
        "schema_version": SCHEMA_VERSION,
        "last_updated_at_utc": ingested_at.isoformat(),
        "source_landing_dir": str(landing_dir),
        "landing_manifest_sha256": landing_manifest_sha256,
        "record_metadata_columns": list(METADATA_COLUMNS),
        "batches": dict(sorted(batches.items())),
        "last_run": run_summary,
    }
    write_json_atomic(manifest_path, bronze_manifest)
    logging.info(
        "Bronze run complete for %s: ingested/reprocessed=%d, skipped=%d",
        dataset,
        ingested_count,
        skipped_count,
    )
    return bronze_dir


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Incrementally enrich processed landing batches for the Silver layer."
    )
    parser.add_argument(
        "--landing-dir",
        type=Path,
        required=True,
        help="Member 1 dataset folder containing processed files and manifest.json.",
    )
    parser.add_argument("--output-root", type=Path, default=_DEFAULT_OUTPUT_ROOT)
    arguments = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    bronze_dir = ingest_landing_dataset(arguments.landing_dir, arguments.output_root)
    print(bronze_dir)


if __name__ == "__main__":
    main()

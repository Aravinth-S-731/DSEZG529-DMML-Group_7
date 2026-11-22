import csv
import gzip
import hashlib
import json
import os
import tempfile
from contextlib import contextmanager
from pathlib import Path
from typing import Generator, Iterable


TRANSACTION_COLUMNS = (
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
    "_record_id",
    "_record_hash",
    "_ingested_at_utc",
    "_source_file",
    "_source_type",
    "_source_sha256",
    "_source_row_number",
    "_batch_id",
    "_schema_version",
)
PRODUCT_COLUMNS = (
    "stock_code",
    "description",
    "is_duplicate_stock_code",
    "quality_flags",
    "_record_id",
    "_record_hash",
    "_ingested_at_utc",
    "_source_file",
    "_source_type",
    "_source_sha256",
    "_source_row_number",
    "_batch_id",
    "_schema_version",
)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        while chunk := source.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def read_silver_manifest(silver_root: Path, dataset: str) -> tuple[dict, str]:
    dataset_dir = silver_root / dataset
    if not dataset_dir.is_dir():
        raise NotADirectoryError(f"Silver dataset directory not found: {dataset_dir}")

    manifest_path = dataset_dir / "silver_manifest.json"
    if not manifest_path.is_file():
        raise FileNotFoundError(f"Silver manifest not found: {manifest_path}")

    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if not isinstance(manifest, dict):
        raise ValueError(f"Silver manifest must be a JSON object: {manifest_path}")
    if manifest.get("dataset") != dataset:
        raise ValueError(
            f"Silver manifest dataset mismatch: expected {dataset}, "
            f"received {manifest.get('dataset')!r}"
        )
    if manifest.get("schema_version") != "1":
        raise ValueError(
            f"Unsupported Silver schema version: {manifest.get('schema_version')!r}"
        )

    files = manifest.get("files")
    if not isinstance(files, list) or not files:
        raise ValueError(f"Silver manifest contains no files: {manifest_path}")
    for entry in files:
        if not isinstance(entry, dict):
            raise ValueError(f"Invalid file entry in Silver manifest: {manifest_path}")
        filename = entry.get("silver_file")
        row_count = entry.get("row_count")
        if (
            not isinstance(filename, str)
            or Path(filename).name != filename
            or not filename.endswith(".csv.gz")
            or not isinstance(entry.get("silver_sha256"), str)
            or isinstance(row_count, bool)
            or not isinstance(row_count, int)
            or row_count < 0
        ):
            raise ValueError(f"Invalid file metadata in Silver manifest: {entry!r}")

    return manifest, sha256_file(manifest_path)


@contextmanager
def open_verified_silver_csv(
    source_path: Path,
    expected_sha256: str,
    expected_columns: tuple[str, ...],
) -> Generator[csv.DictReader, None, None]:
    actual_sha256 = sha256_file(source_path)
    if actual_sha256 != expected_sha256:
        raise ValueError(
            f"Silver checksum mismatch for {source_path}: "
            f"expected {expected_sha256}, got {actual_sha256}"
        )

    with gzip.open(source_path, "rt", encoding="utf-8-sig", newline="") as source:
        reader = csv.DictReader(source)
        if reader.fieldnames != list(expected_columns):
            raise ValueError(
                f"Schema mismatch in {source_path.name}: expected "
                f"{list(expected_columns)}, received {reader.fieldnames}"
            )
        yield reader


def iter_silver_rows(
    silver_root: Path,
    dataset: str,
    manifest: dict,
    expected_columns: tuple[str, ...],
) -> Generator[dict[str, str], None, None]:
    dataset_dir = silver_root / dataset
    for entry in sorted(manifest["files"], key=lambda item: item["silver_file"]):
        source_path = dataset_dir / entry["silver_file"]
        if not source_path.is_file():
            raise FileNotFoundError(
                f"Silver file listed in manifest is missing: {source_path}"
            )

        row_count = 0
        with open_verified_silver_csv(
            source_path,
            entry["silver_sha256"],
            expected_columns,
        ) as reader:
            for row in reader:
                if None in row:
                    raise ValueError(
                        f"Unexpected extra columns in Silver file: {source_path}"
                    )
                row_count += 1
                yield row

        if row_count != entry["row_count"]:
            raise ValueError(
                f"Row-count mismatch for {source_path.name}: manifest states "
                f"{entry['row_count']}, file contains {row_count}"
            )


def write_gzip_csv_atomic(
    path: Path,
    fieldnames: tuple[str, ...],
    rows: Iterable[dict[str, str]],
) -> dict:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temp_name = tempfile.mkstemp(
        dir=path.parent,
        prefix=f".{path.stem}-",
        suffix=".csv.gz.tmp",
    )
    os.close(descriptor)
    temp_path = Path(temp_name)
    row_count = 0

    try:
        with gzip.open(temp_path, "wt", encoding="utf-8", newline="") as output:
            writer = csv.DictWriter(
                output,
                fieldnames=fieldnames,
                lineterminator="\n",
            )
            writer.writeheader()
            for row in rows:
                writer.writerow(row)
                row_count += 1

        with temp_path.open("rb+") as output:
            os.fsync(output.fileno())
        digest = sha256_file(temp_path)
        size = temp_path.stat().st_size
        os.replace(temp_path, path)
    finally:
        temp_path.unlink(missing_ok=True)

    return {
        "file": path.name,
        "sha256": digest,
        "size_bytes": size,
        "row_count": row_count,
    }


def write_json_atomic(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            dir=path.parent,
            prefix=".gold-manifest-",
            suffix=".tmp",
            delete=False,
            encoding="utf-8",
            newline="\n",
        ) as output:
            temp_path = Path(output.name)
            json.dump(payload, output, indent=2, ensure_ascii=False)
            output.write("\n")
            output.flush()
            os.fsync(output.fileno())
        os.replace(temp_path, path)
        temp_path = None
    finally:
        if temp_path is not None:
            temp_path.unlink(missing_ok=True)

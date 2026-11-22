import csv
import hashlib
import json
import os
import tempfile
from contextlib import contextmanager
from decimal import Decimal
from pathlib import Path
from typing import Generator


BRONZE_METADATA_COLUMNS = (
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


@contextmanager
def open_bronze_csv(
    source_path: Path,
    expected_sha256: str,
    expected_columns: tuple[str, ...],
) -> Generator[csv.DictReader, None, None]:
    actual_sha256 = sha256_file(source_path)
    if actual_sha256 != expected_sha256:
        raise ValueError(
            f"Bronze checksum mismatch for {source_path}: "
            f"expected {expected_sha256}, got {actual_sha256}"
        )

    with source_path.open("r", encoding="utf-8-sig", newline="") as source:
        reader = csv.DictReader(source)
        if reader.fieldnames != list(expected_columns):
            raise ValueError(
                f"Schema mismatch in {source_path.name}: expected "
                f"{list(expected_columns)}, received {reader.fieldnames}"
            )
        yield reader


def write_json_atomic(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            dir=path.parent,
            prefix=".silver-manifest-",
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
def canonical_decimal(value: Decimal) -> str:
    if value == 0:
        return "0"
    return format(value.normalize(), "f")

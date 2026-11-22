import csv
import hashlib
import json
import os
import tempfile
from contextlib import ExitStack
from datetime import datetime
from pathlib import Path
from typing import Sequence


SCHEMA_VERSION = "1"
METADATA_COLUMNS = (
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


def write_json_atomic(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            dir=path.parent,
            prefix=".manifest-",
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


def enrich_csv(
    source_path: Path,
    destination_path: Path,
    expected_columns: Sequence[str],
    dataset: str,
    source_type: str,
    source_sha256: str,
    batch_id: str,
    ingested_at: datetime,
    expected_row_count: int,
) -> dict:
    destination_path.parent.mkdir(parents=True, exist_ok=True)
    output_columns = [*expected_columns, *METADATA_COLUMNS]
    collisions = set(expected_columns) & set(METADATA_COLUMNS)
    if collisions:
        raise ValueError(f"Input schema uses reserved Bronze columns: {sorted(collisions)}")

    temp_path: Path | None = None
    row_count = 0
    empty_value_counts = {column: 0 for column in expected_columns}
    try:
        with ExitStack() as stack:
            source = stack.enter_context(
                source_path.open("r", encoding="utf-8-sig", newline="")
            )
            output = stack.enter_context(
                tempfile.NamedTemporaryFile(
                    mode="w",
                    dir=destination_path.parent,
                    prefix=".bronze-",
                    suffix=".tmp",
                    delete=False,
                    encoding="utf-8",
                    newline="",
                )
            )
            temp_path = Path(output.name)
            reader = csv.DictReader(source)
            if reader.fieldnames != list(expected_columns):
                raise ValueError(
                    f"Schema mismatch in {source_path.name}: expected "
                    f"{list(expected_columns)}, received {reader.fieldnames}"
                )
            writer = csv.DictWriter(
                output,
                fieldnames=output_columns,
                extrasaction="raise",
                lineterminator="\n",
            )
            writer.writeheader()
            for row_count, row in enumerate(reader, start=1):
                if None in row:
                    raise ValueError(
                        f"Unexpected extra values in {source_path.name} row {row_count}"
                    )
                business_values = {}
                for column in expected_columns:
                    value = row.get(column)
                    if value is None:
                        raise ValueError(
                            f"Missing column value in {source_path.name}, "
                            f"row {row_count}, column {column}"
                        )
                    business_values[column] = value
                    if value == "":
                        empty_value_counts[column] += 1

                canonical_record = json.dumps(
                    [business_values[column] for column in expected_columns],
                    ensure_ascii=False,
                    separators=(",", ":"),
                )
                record_hash = hashlib.sha256(canonical_record.encode("utf-8")).hexdigest()
                record_id = hashlib.sha256(
                    f"{dataset}\0{source_path.name}\0{source_sha256}\0{row_count}".encode(
                        "utf-8"
                    )
                ).hexdigest()
                writer.writerow(
                    {
                        **business_values,
                        "_record_id": record_id,
                        "_record_hash": record_hash,
                        "_ingested_at_utc": ingested_at.isoformat(),
                        "_source_file": source_path.name,
                        "_source_type": source_type,
                        "_source_sha256": source_sha256,
                        "_source_row_number": row_count,
                        "_batch_id": batch_id,
                        "_schema_version": SCHEMA_VERSION,
                    }
                )
            output.flush()
            os.fsync(output.fileno())

        if row_count != expected_row_count:
            raise ValueError(
                f"Row-count mismatch for {source_path.name}: landing manifest "
                f"states {expected_row_count}, file contains {row_count}"
            )

        output_sha256 = sha256_file(temp_path)
        output_size = temp_path.stat().st_size
        os.replace(temp_path, destination_path)
        temp_path = None
    finally:
        if temp_path is not None:
            temp_path.unlink(missing_ok=True)

    return {
        "source_file": source_path.name,
        "source_sha256": source_sha256,
        "source_type": source_type,
        "batch_id": batch_id,
        "row_count": row_count,
        "bronze_file": destination_path.name,
        "bronze_sha256": output_sha256,
        "bronze_size_bytes": output_size,
        "empty_value_counts": empty_value_counts,
        "schema_version": SCHEMA_VERSION,
        "ingested_at_utc": ingested_at.isoformat(),
    }

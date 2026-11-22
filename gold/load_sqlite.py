import argparse
import csv
import gzip
import json
import logging
import os
import sqlite3
import tempfile
from dataclasses import dataclass
from pathlib import Path

from gold.common import sha256_file
from gold.features import (
    CUSTOMER_COLUMNS,
    INTERACTION_COLUMNS,
    PRODUCT_FEATURE_COLUMNS,
)


_PROJECT_ROOT = Path(__file__).resolve().parents[1]
_DEFAULT_GOLD_ROOT = _PROJECT_ROOT / "data" / "gold"
_DEFAULT_DATABASE_PATH = (
    _PROJECT_ROOT / "data" / "warehouse" / "online_retail.sqlite3"
)
_SCHEMA_PATH = _PROJECT_ROOT / "sql" / "gold_schema.sql"
_BATCH_SIZE = 2000


@dataclass(frozen=True)
class _TableSpec:
    name: str
    filename: str
    columns: tuple[str, ...]
    nullable_columns: frozenset[str] = frozenset()


_TABLES = (
    _TableSpec(
        "gold_customer_features",
        "customer_features.csv.gz",
        CUSTOMER_COLUMNS,
    ),
    _TableSpec(
        "gold_product_features",
        "product_features.csv.gz",
        PRODUCT_FEATURE_COLUMNS,
        frozenset({"first_sold_at", "last_sold_at", "popularity_rank"}),
    ),
    _TableSpec(
        "gold_customer_product_interactions",
        "customer_product_interactions.csv.gz",
        INTERACTION_COLUMNS,
    ),
)


def _read_gold_manifest(gold_root: Path) -> dict[str, dict]:
    manifest_path = gold_root / "gold_manifest.json"
    if not manifest_path.is_file():
        raise FileNotFoundError(f"Gold manifest not found: {manifest_path}")

    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if not isinstance(manifest, dict) or manifest.get("schema_version") != "1":
        raise ValueError(f"Unsupported Gold manifest: {manifest_path}")

    entries = manifest.get("files")
    if not isinstance(entries, list):
        raise ValueError(f"Gold manifest has no file list: {manifest_path}")

    expected_names = {table.filename for table in _TABLES}
    by_name: dict[str, dict] = {}
    for entry in entries:
        if not isinstance(entry, dict):
            raise ValueError(f"Invalid file entry in Gold manifest: {entry!r}")
        filename = entry.get("file")
        row_count = entry.get("row_count")
        size_bytes = entry.get("size_bytes")
        checksum = entry.get("sha256")
        if (
            not isinstance(filename, str)
            or Path(filename).name != filename
            or filename not in expected_names
            or filename in by_name
            or isinstance(row_count, bool)
            or not isinstance(row_count, int)
            or row_count < 0
            or isinstance(size_bytes, bool)
            or not isinstance(size_bytes, int)
            or size_bytes < 0
            or not isinstance(checksum, str)
            or len(checksum) != 64
        ):
            raise ValueError(f"Invalid file metadata in Gold manifest: {entry!r}")
        by_name[filename] = entry

    if set(by_name) != expected_names:
        raise ValueError(
            "Gold manifest file set mismatch: expected "
            f"{sorted(expected_names)}, received {sorted(by_name)}"
        )
    return by_name


def _row_values(
    table: _TableSpec,
    row: dict[str, str | None],
    path: Path,
    row_number: int,
) -> tuple:
    if None in row or any(value is None for value in row.values()):
        raise ValueError(
            f"Column count mismatch in {path} at data row {row_number}"
        )

    values = []
    for column in table.columns:
        value = row[column]
        if value is None:
            raise ValueError(
                f"Missing value for {column} in {path} at data row {row_number}"
            )
        if column in table.nullable_columns:
            value = value.strip() or None
        elif column == "is_duplicate_catalog_code":
            normalized = value.strip().lower()
            if normalized not in {"true", "false"}:
                raise ValueError(
                    f"Invalid boolean in {path} at row {row_number}: {value!r}"
                )
            value = normalized == "true"
        elif value == "" and not (
            table.name == "gold_product_features" and column == "description"
        ):
            raise ValueError(
                f"Unexpected empty value for {column} in {path} "
                f"at data row {row_number}"
            )
        values.append(value)
    return tuple(values)


def _load_table(
    connection: sqlite3.Connection,
    gold_root: Path,
    table: _TableSpec,
    manifest_entry: dict,
) -> int:
    source_path = gold_root / table.filename
    if not source_path.is_file():
        raise FileNotFoundError(f"Gold output not found: {source_path}")
    if source_path.stat().st_size != manifest_entry["size_bytes"]:
        raise ValueError(f"Size mismatch for Gold output: {source_path}")
    actual_checksum = sha256_file(source_path)
    if actual_checksum != manifest_entry["sha256"]:
        raise ValueError(
            f"SHA-256 mismatch for {source_path}: "
            f"expected {manifest_entry['sha256']}, got {actual_checksum}"
        )

    placeholders = ", ".join("?" for _ in table.columns)
    columns = ", ".join(f'"{column}"' for column in table.columns)
    statement = (
        f'INSERT INTO "{table.name}" ({columns}) VALUES ({placeholders})'
    )
    inserted = 0
    batch = []

    with gzip.open(source_path, "rt", encoding="utf-8-sig", newline="") as source:
        reader = csv.DictReader(source)
        if reader.fieldnames != list(table.columns):
            raise ValueError(
                f"Schema mismatch in {source_path.name}: expected "
                f"{list(table.columns)}, received {reader.fieldnames}"
            )
        for row_number, row in enumerate(reader, start=1):
            if row_number > manifest_entry["row_count"]:
                raise ValueError(f"Row-count mismatch for {source_path}")
            batch.append(_row_values(table, row, source_path, row_number))
            if len(batch) == _BATCH_SIZE:
                connection.executemany(statement, batch)
                inserted += len(batch)
                batch.clear()

    if batch:
        connection.executemany(statement, batch)
        inserted += len(batch)

    if inserted != manifest_entry["row_count"]:
        raise ValueError(
            f"Row-count mismatch for {source_path}: manifest states "
            f"{manifest_entry['row_count']}, file contains {inserted}"
        )
    return inserted


def load_gold_to_sqlite(gold_root: Path, database_path: Path) -> dict[str, int]:
    manifest_entries = _read_gold_manifest(gold_root)
    schema = _SCHEMA_PATH.read_text(encoding="utf-8")
    database_path.parent.mkdir(parents=True, exist_ok=True)

    descriptor, temporary_name = tempfile.mkstemp(
        dir=database_path.parent,
        prefix=f".{database_path.stem}-",
        suffix=".sqlite3.tmp",
    )
    os.close(descriptor)
    temporary_path = Path(temporary_name)
    connection: sqlite3.Connection | None = None

    try:
        connection = sqlite3.connect(temporary_path)
        connection.execute("PRAGMA foreign_keys = ON")
        connection.executescript(schema)

        schema_columns = {
            table.name: tuple(
                row[1]
                for row in connection.execute(
                    f'PRAGMA table_info("{table.name}")'
                )
            )
            for table in _TABLES
        }
        for table in _TABLES:
            if schema_columns[table.name] != table.columns:
                raise ValueError(
                    f"SQL schema mismatch for {table.name}: "
                    f"{schema_columns[table.name]} != {table.columns}"
                )

        connection.execute("BEGIN")
        row_counts = {}
        for table in _TABLES:
            row_counts[table.name] = _load_table(
                connection,
                gold_root,
                table,
                manifest_entries[table.filename],
            )

        foreign_key_errors = connection.execute(
            "PRAGMA foreign_key_check"
        ).fetchall()
        if foreign_key_errors:
            raise ValueError(f"SQLite foreign-key check failed: {foreign_key_errors}")

        connection.commit()
        integrity = connection.execute("PRAGMA integrity_check").fetchone()
        if integrity != ("ok",):
            raise ValueError(f"SQLite integrity check failed: {integrity}")
        connection.close()
        connection = None
        os.replace(temporary_path, database_path)
        temporary_path = None
        return row_counts
    finally:
        if connection is not None:
            connection.close()
        if temporary_path is not None:
            temporary_path.unlink(missing_ok=True)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Load verified Gold feature CSVs into a local SQLite warehouse."
    )
    parser.add_argument("--gold-root", type=Path, default=_DEFAULT_GOLD_ROOT)
    parser.add_argument("--database", type=Path, default=_DEFAULT_DATABASE_PATH)
    arguments = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    row_counts = load_gold_to_sqlite(arguments.gold_root, arguments.database)
    for table_name, row_count in row_counts.items():
        logging.info("Loaded %s: %d rows", table_name, row_count)
    logging.info("SQLite warehouse ready: %s", arguments.database)


if __name__ == "__main__":
    main()

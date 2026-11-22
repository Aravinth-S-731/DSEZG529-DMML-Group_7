import csv
import gzip
import hashlib
import json
import os
import re
import shutil
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator

from gold.common import sha256_file


_PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_GOLD_ROOT = _PROJECT_ROOT / "data" / "gold"
DEFAULT_STORE_ROOT = _PROJECT_ROOT / "data" / "feature_store"
DEFAULT_METADATA_PATH = Path(__file__).resolve().parent / "metadata.json"
_VERSION_PATTERN = re.compile(r"v1-[0-9a-f]{20}")


def _read_json(path: Path) -> dict:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"Expected a JSON object in {path}")
    return payload


def _validate_metadata(metadata: dict) -> dict:
    if not isinstance(metadata, dict):
        raise ValueError("Feature metadata must be a JSON object")
    if metadata.get("schema_version") != "1":
        raise ValueError(
            f"Unsupported feature metadata schema: {metadata.get('schema_version')!r}"
        )
    views = metadata.get("feature_views")
    if not isinstance(views, dict) or not views:
        raise ValueError("Feature metadata must define at least one feature view")

    for view_name, view in views.items():
        if not isinstance(view_name, str) or not isinstance(view, dict):
            raise ValueError("Invalid feature view definition")
        entity_keys = view.get("entity_keys")
        features = view.get("features")
        source_file = view.get("source_file")
        if (
            not isinstance(entity_keys, list)
            or not entity_keys
            or any(not isinstance(key, str) or not key for key in entity_keys)
            or len(entity_keys) != len(set(entity_keys))
        ):
            raise ValueError(f"Invalid entity keys for feature view {view_name!r}")
        if (
            not isinstance(source_file, str)
            or Path(source_file).name != source_file
            or not source_file.endswith(".csv.gz")
        ):
            raise ValueError(f"Invalid source file for feature view {view_name!r}")
        if not isinstance(features, list) or not features:
            raise ValueError(f"Feature view {view_name!r} defines no features")
        feature_names = []
        for feature in features:
            if not isinstance(feature, dict):
                raise ValueError(f"Invalid feature definition in {view_name!r}")
            name, feature_type = feature.get("name"), feature.get("type")
            if not isinstance(name, str) or not name or not isinstance(feature_type, str):
                raise ValueError(f"Invalid feature name or type in {view_name!r}")
            feature_names.append(name)
        if len(feature_names) != len(set(feature_names)):
            raise ValueError(f"Duplicate feature names in {view_name!r}")
        if set(entity_keys) & set(feature_names):
            raise ValueError(f"Entity key duplicated as a feature in {view_name!r}")
    return metadata


def _read_metadata(metadata_path: Path) -> tuple[dict, bytes, str]:
    metadata_bytes = metadata_path.read_bytes()
    metadata = _validate_metadata(json.loads(metadata_bytes))
    return metadata, metadata_bytes, hashlib.sha256(metadata_bytes).hexdigest()


def _read_gold_manifest(gold_root: Path) -> tuple[dict, dict[str, dict], str]:
    manifest_path = gold_root / "gold_manifest.json"
    if not manifest_path.is_file():
        raise FileNotFoundError(f"Gold manifest not found: {manifest_path}")
    manifest = _read_json(manifest_path)
    if manifest.get("schema_version") != "1":
        raise ValueError(
            f"Unsupported Gold manifest schema: {manifest.get('schema_version')!r}"
        )
    entries = manifest.get("files")
    if not isinstance(entries, list) or not entries:
        raise ValueError(f"Gold manifest contains no feature files: {manifest_path}")

    files = {}
    for entry in entries:
        if not isinstance(entry, dict):
            raise ValueError(f"Invalid Gold file entry in {manifest_path}")
        filename = entry.get("file")
        digest = entry.get("sha256")
        row_count = entry.get("row_count")
        if (
            not isinstance(filename, str)
            or Path(filename).name != filename
            or not filename.endswith(".csv.gz")
            or not isinstance(digest, str)
            or len(digest) != 64
            or isinstance(row_count, bool)
            or not isinstance(row_count, int)
            or row_count < 0
        ):
            raise ValueError(f"Invalid Gold file metadata: {entry!r}")
        if filename in files:
            raise ValueError(f"Duplicate file in Gold manifest: {filename}")
        files[filename] = entry

    if not isinstance(manifest.get("inputs"), dict):
        raise ValueError(f"Gold manifest has no input lineage: {manifest_path}")
    return manifest, files, sha256_file(manifest_path)


def _verify_feature_file(
    path: Path,
    expected_sha256: str,
    expected_row_count: int,
    expected_columns: list[str],
) -> dict:
    if not path.is_file():
        raise FileNotFoundError(f"Feature file not found: {path}")
    if (
        not isinstance(expected_sha256, str)
        or len(expected_sha256) != 64
        or any(character not in "0123456789abcdef" for character in expected_sha256)
        or isinstance(expected_row_count, bool)
        or not isinstance(expected_row_count, int)
        or expected_row_count < 0
    ):
        raise ValueError(f"Invalid feature file metadata for {path.name}")
    actual_sha256 = sha256_file(path)
    if actual_sha256 != expected_sha256:
        raise ValueError(
            f"Feature checksum mismatch for {path}: "
            f"expected {expected_sha256}, got {actual_sha256}"
        )

    row_count = 0
    with gzip.open(path, "rt", encoding="utf-8-sig", newline="") as source:
        reader = csv.DictReader(source)
        if reader.fieldnames != expected_columns:
            raise ValueError(
                f"Feature schema mismatch in {path.name}: expected "
                f"{expected_columns}, received {reader.fieldnames}"
            )
        for row in reader:
            if None in row or any(value is None for value in row.values()):
                raise ValueError(f"Malformed CSV row in {path}")
            row_count += 1
    if row_count != expected_row_count:
        raise ValueError(
            f"Feature row-count mismatch for {path.name}: expected "
            f"{expected_row_count}, found {row_count}"
        )
    return {
        "sha256": actual_sha256,
        "size_bytes": path.stat().st_size,
        "row_count": row_count,
    }


def _atomic_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            dir=path.parent,
            prefix=f".{path.stem}-",
            suffix=".tmp",
            delete=False,
            encoding="utf-8",
            newline="\n",
        ) as output:
            temporary_path = Path(output.name)
            json.dump(payload, output, indent=2)
            output.write("\n")
            output.flush()
            os.fsync(output.fileno())
        os.replace(temporary_path, path)
        temporary_path = None
    finally:
        if temporary_path is not None:
            temporary_path.unlink(missing_ok=True)


def _validate_snapshot(
    version_dir: Path,
    version_id: str,
    expected_gold_sha256: str,
    expected_metadata_sha256: str,
) -> tuple[dict, dict]:
    manifest_path = version_dir / "version_manifest.json"
    metadata_path = version_dir / "metadata.json"
    if not manifest_path.is_file() or not metadata_path.is_file():
        raise FileNotFoundError(f"Incomplete feature snapshot: {version_dir}")
    version_manifest = _read_json(manifest_path)
    metadata, _, metadata_sha256 = _read_metadata(metadata_path)
    if (
        version_manifest.get("schema_version") != "1"
        or version_manifest.get("version_id") != version_id
        or version_manifest.get("gold_manifest_sha256") != expected_gold_sha256
        or metadata_sha256 != expected_metadata_sha256
        or version_manifest.get("feature_metadata_sha256") != metadata_sha256
    ):
        raise ValueError(f"Feature snapshot identity mismatch: {version_dir}")

    files = version_manifest.get("files")
    if not isinstance(files, dict):
        raise ValueError(f"Invalid feature file manifest: {manifest_path}")
    for view_name, view in metadata["feature_views"].items():
        entry = files.get(view_name)
        if not isinstance(entry, dict) or entry.get("file") != view["source_file"]:
            raise ValueError(f"Missing file entry for feature view {view_name!r}")
        _verify_feature_file(
            version_dir / view["source_file"],
            entry.get("sha256", ""),
            entry.get("row_count", -1),
            view["entity_keys"] + [feature["name"] for feature in view["features"]],
        )
    return version_manifest, metadata


def _read_registry(store_root: Path) -> dict:
    registry_path = store_root / "registry.json"
    if not registry_path.is_file():
        return {
            "schema_version": "1",
            "active_version": None,
            "feature_views": {},
            "versions": [],
        }
    registry = _read_json(registry_path)
    if (
        registry.get("schema_version") != "1"
        or not isinstance(registry.get("versions"), list)
        or not isinstance(registry.get("feature_views"), dict)
    ):
        raise ValueError(f"Invalid feature store registry: {registry_path}")
    return registry


def register_gold_features(
    gold_root: Path = DEFAULT_GOLD_ROOT,
    store_root: Path = DEFAULT_STORE_ROOT,
    metadata_path: Path = DEFAULT_METADATA_PATH,
) -> dict:
    metadata, metadata_bytes, metadata_sha256 = _read_metadata(metadata_path)
    gold_manifest, gold_files, gold_sha256 = _read_gold_manifest(gold_root)
    views = metadata["feature_views"]
    expected_files = {view["source_file"] for view in views.values()}
    if not expected_files.issubset(gold_files):
        raise ValueError(
            "Gold manifest is missing feature views: "
            f"{sorted(expected_files - set(gold_files))}"
        )

    version_digest = hashlib.sha256(
        f"{gold_sha256}:{metadata_sha256}".encode("ascii")
    ).hexdigest()[:20]
    version_id = f"v1-{version_digest}"
    versions_root = store_root / "versions"
    versions_root.mkdir(parents=True, exist_ok=True)
    version_dir = versions_root / version_id

    if not version_dir.exists():
        staging_dir = Path(
            tempfile.mkdtemp(prefix=f".{version_id}-", dir=versions_root)
        )
        try:
            (staging_dir / "metadata.json").write_bytes(metadata_bytes)
            view_files = {}
            for view_name, view in views.items():
                filename = view["source_file"]
                gold_entry = gold_files[filename]
                expected_columns = view["entity_keys"] + [
                    feature["name"] for feature in view["features"]
                ]
                source_path = gold_root / filename
                _verify_feature_file(
                    source_path,
                    gold_entry["sha256"],
                    gold_entry["row_count"],
                    expected_columns,
                )
                snapshot_path = staging_dir / filename
                shutil.copyfile(source_path, snapshot_path)
                copied_file = _verify_feature_file(
                    snapshot_path,
                    gold_entry["sha256"],
                    gold_entry["row_count"],
                    expected_columns,
                )
                view_files[view_name] = {
                    "file": filename,
                    **copied_file,
                }

            version_manifest = {
                "schema_version": "1",
                "version_id": version_id,
                "created_at_utc": datetime.now(timezone.utc).isoformat(),
                "gold_manifest_sha256": gold_sha256,
                "silver_manifest_sha256s": {
                    name: input_manifest.get("silver_manifest_sha256")
                    for name, input_manifest in gold_manifest["inputs"].items()
                    if isinstance(input_manifest, dict)
                },
                "feature_metadata_sha256": metadata_sha256,
                "files": view_files,
            }
            _atomic_json(staging_dir / "version_manifest.json", version_manifest)
            os.replace(staging_dir, version_dir)
            staging_dir = None
        finally:
            if staging_dir is not None:
                shutil.rmtree(staging_dir)
    else:
        _validate_snapshot(version_dir, version_id, gold_sha256, metadata_sha256)

    registry = _read_registry(store_root)
    versions = registry["versions"]
    existing = next(
        (
            item
            for item in versions
            if isinstance(item, dict) and item.get("version_id") == version_id
        ),
        None,
    )
    if existing is None:
        version_manifest = _read_json(version_dir / "version_manifest.json")
        versions.append(
            {
                "version_id": version_id,
                "created_at_utc": version_manifest["created_at_utc"],
                "gold_manifest_sha256": gold_sha256,
                "feature_metadata_sha256": metadata_sha256,
                "version_manifest_sha256": sha256_file(
                    version_dir / "version_manifest.json"
                ),
                "path": f"versions/{version_id}",
                "row_counts": {
                    view_name: file_entry["row_count"]
                    for view_name, file_entry in version_manifest["files"].items()
                },
            }
        )
    elif (
        existing.get("gold_manifest_sha256") != gold_sha256
        or existing.get("feature_metadata_sha256") != metadata_sha256
    ):
        raise ValueError(f"Registry version metadata mismatch: {version_id}")
    else:
        version_manifest = _read_json(version_dir / "version_manifest.json")
        existing["version_manifest_sha256"] = sha256_file(
            version_dir / "version_manifest.json"
        )
        existing["path"] = f"versions/{version_id}"
        existing["row_counts"] = {
            view_name: file_entry["row_count"]
            for view_name, file_entry in version_manifest["files"].items()
        }

    registry["updated_at_utc"] = datetime.now(timezone.utc).isoformat()
    registry["active_version"] = version_id
    registry["feature_views"] = views
    _atomic_json(store_root / "registry.json", registry)
    return {
        "version_id": version_id,
        "registry_path": str(store_root / "registry.json"),
        "version_path": str(version_dir),
        "row_counts": {
            view_name: gold_files[view["source_file"]]["row_count"]
            for view_name, view in views.items()
        },
    }


def resolve_version_id(store_root: Path, version_id: str | None = None) -> str:
    registry = _read_registry(store_root)
    resolved_version = version_id or registry.get("active_version")
    if not isinstance(resolved_version, str) or not _VERSION_PATTERN.fullmatch(
        resolved_version
    ):
        raise ValueError("No valid feature version is registered")
    known_versions = {
        item.get("version_id")
        for item in registry["versions"]
        if isinstance(item, dict)
    }
    if resolved_version not in known_versions:
        raise ValueError(f"Feature version is not registered: {resolved_version}")
    return resolved_version


def iter_feature_rows(
    store_root: Path,
    view_name: str,
    version_id: str | None = None,
    entity_filters: dict[str, str] | None = None,
) -> Iterator[dict[str, str]]:
    resolved_version = resolve_version_id(store_root, version_id)
    registry = _read_registry(store_root)
    registered_version = next(
        entry
        for entry in registry["versions"]
        if isinstance(entry, dict) and entry.get("version_id") == resolved_version
    )
    version_dir = store_root / "versions" / resolved_version
    version_manifest_path = version_dir / "version_manifest.json"
    metadata_path = version_dir / "metadata.json"
    version_manifest = _read_json(version_manifest_path)
    metadata, _, metadata_sha256 = _read_metadata(metadata_path)
    if (
        version_manifest.get("schema_version") != "1"
        or version_manifest.get("version_id") != resolved_version
        or version_manifest.get("gold_manifest_sha256")
        != registered_version.get("gold_manifest_sha256")
        or version_manifest.get("feature_metadata_sha256") != metadata_sha256
        or metadata_sha256 != registered_version.get("feature_metadata_sha256")
        or sha256_file(version_manifest_path)
        != registered_version.get("version_manifest_sha256")
    ):
        raise ValueError(f"Feature version manifest is invalid: {resolved_version}")

    views = metadata["feature_views"]
    if view_name not in views:
        raise ValueError(f"Unknown feature view {view_name!r}")
    view = views[view_name]
    if entity_filters is None:
        filters = {}
    elif not isinstance(entity_filters, dict):
        raise ValueError(f"Entity filters for {view_name!r} must be a mapping")
    else:
        filters = entity_filters
    if any(
        key not in view["entity_keys"] or not isinstance(value, str)
        for key, value in filters.items()
    ):
        raise ValueError(
            f"Entity filters for {view_name!r} must use its keys: "
            f"{view['entity_keys']}"
        )

    version_files = version_manifest.get("files")
    if not isinstance(version_files, dict):
        raise ValueError(f"Invalid feature file manifest for {resolved_version}")
    file_entry = version_files.get(view_name)
    if not isinstance(file_entry, dict) or file_entry.get("file") != view["source_file"]:
        raise ValueError(f"Missing file metadata for feature view {view_name!r}")
    expected_columns = view["entity_keys"] + [
        feature["name"] for feature in view["features"]
    ]
    feature_path = version_dir / view["source_file"]
    _verify_feature_file(
        feature_path,
        file_entry.get("sha256", ""),
        file_entry.get("row_count", -1),
        expected_columns,
    )

    with gzip.open(feature_path, "rt", encoding="utf-8-sig", newline="") as source:
        reader = csv.DictReader(source)
        for row in reader:
            if not filters or all(row[key] == value for key, value in filters.items()):
                yield row

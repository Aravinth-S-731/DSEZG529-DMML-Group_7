# Local Feature Store

This custom, file-based feature store snapshots the Gold tables into immutable
versions. It provides a registry, feature metadata, checksum/schema validation,
and version-pinned retrieval without requiring a database or external service.

## Register a Gold version

After Gold has completed, run:

```powershell
python -m feature_store.register
```

The Prefect flow performs this registration automatically after Gold and before
the EDA dashboard stage.

The command verifies the Gold manifest, file checksums, CSV schemas, and row
counts, then stores snapshots under:

```text
data/
  feature_store/
    registry.json
    versions/
      v1-<content-and-metadata-hash>/
        metadata.json
        version_manifest.json
        customer_features.csv.gz
        product_features.csv.gz
        customer_product_interactions.csv.gz
```

Each immutable version is identified by its Gold manifest and feature-metadata
hashes. Registering the same Gold inputs and metadata is idempotent; a later
Gold output creates a new version while retaining previous snapshots. The
registry's `active_version` points to the most recently registered version.
The current registry is `data\feature_store\registry.json`.

## Feature views

`metadata.json` documents each view's entity keys and each feature's type,
meaning, source, transformation, unit where applicable, and nullability.
`registry.json` exposes the current view definitions and version catalog.
Every version also contains its metadata snapshot and file checksums/row counts.

- `customer_features`: one row per `customer_id`.
- `product_features`: one row per `stock_code`.
- `customer_product_interactions`: one row per `(customer_id, stock_code)`.

The CSV values returned by the Python API are strings; use the corresponding
metadata types when converting them for a model.

## Versioned retrieval

Copy a version ID from the registration output or `registry.json`, then pin it
for a reproducible sample lookup:

```powershell
$version = (Get-Content .\data\feature_store\registry.json -Raw | ConvertFrom-Json).active_version
python -m feature_store.retrieve `
  --view customer_features `
  --filter customer_id=12347 `
  --version $version
```

For the currently registered sample version `v1-cff72041ecd9156fee8b`, the
lookup returns:

```json
{
  "version_id": "v1-cff72041ecd9156fee8b",
  "feature_view": "customer_features",
  "entity_filters": {
    "customer_id": "12347"
  },
  "records": [
    {
      "customer_id": "12347",
      "unique_products": "103",
      "purchase_line_count": "182",
      "total_units_purchased": "2458",
      "total_spend": "4310",
      "first_purchase_at": "2010-12-07T14:57:00",
      "last_purchase_at": "2011-12-07T15:52:00"
    }
  ]
}
```

For a customer-product interaction lookup, filter both entity keys:

```powershell
python -m feature_store.retrieve `
  --view customer_product_interactions `
  --filter customer_id=12347 `
  --filter stock_code=22423 `
  --version $version
```

The default CLI output is limited to 10 rows. For model training, use the
iterator to read all features from a pinned version:

```python
from pathlib import Path

from feature_store.store import iter_feature_rows, resolve_version_id

store_root = Path("data/feature_store")
version = resolve_version_id(store_root)
training_rows = iter_feature_rows(
    store_root,
    "customer_product_interactions",
    version_id=version,
)
```

For inference, pass exact entity filters to the same API. The version's feature
file is checksum- and schema-verified before any rows are returned.

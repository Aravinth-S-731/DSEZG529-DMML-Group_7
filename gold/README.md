# Member 4: Gold Layer

Gold combines the monthly Silver transactions into recommendation and
analytical feature tables. It verifies Silver manifests, checksums, schemas,
and row counts before using the data.

## Run

Run after both Silver datasets are available:

```powershell
python -m gold.build
```

Optional alternate roots:

```powershell
python -m gold.build `
  --silver-root ".\data\silver" `
  --gold-root ".\data\gold"
```

## Recommendation policy

- Uses Silver rows marked `is_recommendation_eligible=true`.
- Excludes rows flagged `is_duplicate_content=true` from recommendation
  interactions to avoid counting repeated record content twice.
- Silver cancellations, returns, zero-quantity rows, negative-price rows, and
  rows without customer IDs remain auditable in Silver but do not enter these
  positive-purchase features.
- Aggregates at customer-product level. Purchase-line count, quantity, and
  spend are retained as separate implicit-feedback signals; no single model
  score or train/test split is imposed.
- Product catalog duplicates are represented once per stock code. The most
  frequent non-empty description is selected; ties are resolved
  alphabetically.

## Outputs

```text
data/
  gold/
    customer_product_interactions.csv.gz
    customer_features.csv.gz
    product_features.csv.gz
    quality_report.json
    gold_manifest.json
```

- `customer_product_interactions.csv.gz`: one row per customer-product pair,
  with purchase-line count, units, spend, and first/last purchase timestamps.
- `customer_features.csv.gz`: one row per customer with product diversity,
  purchase activity, total units/spend, and first/last purchase timestamps.
- `product_features.csv.gz`: one row per catalog or sold product, with
  description, customer reach, units, sales, dates, and popularity rank.
  Products without sales remain present with zero sales metrics and no rank.
- `quality_report.json` records eligible/ineligible and duplicate-excluded
  counts, catalog duplicates, and products missing from either side.
- `gold_manifest.json` records the Silver manifest checksums and output file
  checksums, sizes, and row counts.

All data outputs are gzip-compressed CSVs and can be opened with Python's
`gzip` module or Pandas compression inference.

The matching SQLite/PostgreSQL-compatible table definitions and keys are in
[`../sql/gold_schema.sql`](../sql/gold_schema.sql). This schema documents a
warehouse representation; the current pipeline continues to write CSV files.
The SQLite loader converts empty `first_sold_at`, `last_sold_at`, and
`popularity_rank` values in `product_features.csv.gz` to SQL `NULL`.

To materialize the three Gold tables in a local SQLite warehouse after Gold is
built, run:

```powershell
python -m gold.load_sqlite
```

The loader verifies the Gold manifest, file sizes, checksums, CSV schemas, and
row counts before replacing `data/warehouse/online_retail.sqlite3`. It also
checks the database's foreign keys and integrity. The SQLite database is
regenerable and ignored by Git; the source CSVs and manifests remain versioned.
